//! Command line arguments.

use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    net::{SocketAddrV4, SocketAddrV6},
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::{
    error::{ContextKind, ErrorKind},
    CommandFactory, Parser, Subcommand,
};
use console::style;
use futures_buffered::BufferedStreamExt;
use indicatif::{
    HumanBytes, HumanDuration, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle,
};
use iroh::{
    address_lookup::{dns::DnsAddressLookup, pkarr::PkarrPublisher},
    endpoint::presets,
    Endpoint, EndpointAddr, RelayMode, RelayUrl, SecretKey, TransportAddr,
};
use iroh_blobs::{
    api::{
        blobs::{
            AddPathOptions, AddProgressItem, ExportMode, ExportOptions, ExportProgressItem,
            ImportMode,
        },
        remote::GetProgressItem,
        Store, TempTag,
    },
    format::collection::Collection,
    get::{request::get_hash_seq_and_sizes, GetError, Stats},
    provider::{
        self,
        events::{ConnectMode, EventMask, EventSender, ProviderMessage, RequestUpdate},
    },
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol, Hash,
};
use n0_future::{task::AbortOnDropHandle, FuturesUnordered, StreamExt};
use tokio::{select, sync::mpsc};
use tracing::{error, trace};
use walkdir::WalkDir;

/// Send a file or directory between two machines, using blake3 verified streaming.
///
/// For all subcommands, you can specify a secret key using the IROH_SECRET
/// environment variable. If you don't, a random one will be generated.
///
/// You can also specify a port for the magicsocket. If you don't, a random one
/// will be chosen.
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Option<Commands>,

    /// A file or folder to send. Will ask for confirmation.
    pub path: Option<PathBuf>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    #[default]
    Hex,
    Cid,
}

impl FromStr for Format {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hex" => Ok(Format::Hex),
            "cid" => Ok(Format::Cid),
            _ => Err(anyhow::anyhow!("invalid format")),
        }
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Format::Hex => write!(f, "hex"),
            Format::Cid => write!(f, "cid"),
        }
    }
}

fn print_hash(hash: &Hash, format: Format) -> String {
    match format {
        Format::Hex => hash.to_hex().to_string(),
        Format::Cid => hash.to_string(),
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or directory.
    Send(SendArgs),

    /// Receive a file or directory.
    #[clap(visible_alias = "recv")]
    Receive(ReceiveArgs),
}

#[derive(Parser, Debug)]
pub struct CommonArgs {
    /// The IPv4 address that magicsocket will listen on.
    ///
    /// If None, defaults to a random free port, but it can be useful to specify a fixed
    /// port, e.g. to configure a firewall rule.
    #[clap(long, default_value = None)]
    pub magic_ipv4_addr: Option<SocketAddrV4>,

    /// The IPv6 address that magicsocket will listen on.
    ///
    /// If None, defaults to a random free port, but it can be useful to specify a fixed
    /// port, e.g. to configure a firewall rule.
    #[clap(long, default_value = None)]
    pub magic_ipv6_addr: Option<SocketAddrV6>,

    #[clap(long, default_value_t = Format::Hex)]
    pub format: Format,

    #[clap(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress progress bars.
    #[clap(long, default_value_t = false)]
    pub no_progress: bool,

    /// The relay URL to use as a home relay,
    ///
    /// Can be set to "disabled" to disable relay servers and "default"
    /// to configure default servers.
    #[clap(long, default_value_t = RelayModeOption::Default)]
    pub relay: RelayModeOption,

    #[clap(long)]
    pub show_secret: bool,

    /// Number of parallel jobs to use while importing files.
    ///
    /// Defaults to the number of logical CPU cores.
    #[clap(short = 'j', long)]
    pub jobs: Option<usize>,
}

/// Available command line options for configuring relays.
#[derive(Clone, Debug)]
pub enum RelayModeOption {
    /// Disables relays altogether.
    Disabled,
    /// Uses the default relay servers.
    Default,
    /// Uses a single, custom relay server by URL.
    Custom(RelayUrl),
}

impl FromStr for RelayModeOption {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "disabled" => Ok(Self::Disabled),
            "default" => Ok(Self::Default),
            _ => Ok(Self::Custom(RelayUrl::from_str(s)?)),
        }
    }
}

impl Display for RelayModeOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("disabled"),
            Self::Default => f.write_str("default"),
            Self::Custom(url) => url.fmt(f),
        }
    }
}

impl From<RelayModeOption> for RelayMode {
    fn from(value: RelayModeOption) -> Self {
        match value {
            RelayModeOption::Disabled => RelayMode::Disabled,
            RelayModeOption::Default => RelayMode::Default,
            RelayModeOption::Custom(url) => RelayMode::Custom(url.into()),
        }
    }
}

#[derive(Parser, Debug)]
pub struct SendArgs {
    /// Path to the file or directory to send.
    ///
    /// The last component of the path will be used as the name of the data
    /// being shared.
    pub path: PathBuf,

    /// What type of ticket to use.
    ///
    /// Use "id" for the shortest type only including the endpoint ID,
    /// "addresses" to only add IP addresses without a relay url,
    /// "relay" to only add a relay address, and leave the option out
    /// to use the biggest type of ticket that includes both relay and
    /// address information.
    ///
    /// Generally, the more information the higher the likelihood of
    /// a successful connection, but also the bigger a ticket to connect.
    ///
    /// This is most useful for debugging which methods of connection
    /// establishment work well.
    #[clap(long, default_value_t = AddrInfoOptions::RelayAndAddresses)]
    pub ticket_type: AddrInfoOptions,

    #[clap(flatten)]
    pub common: CommonArgs,

    /// Do not compress folders into a single archive before sending.
    #[clap(long)]
    pub noarchive: bool,

    /// Show the receive command as a QR code.
    #[clap(long)]
    pub qr: bool,

    /// Keep the sender running after the first complete transfer
    /// (normally it exits once a receiver has downloaded the data).
    #[clap(long)]
    pub nostop: bool,

    /// Run the sender in the background; exits after the first
    /// complete transfer. The ticket is printed and the shell is freed.
    #[clap(long, conflicts_with = "bg")]
    pub bg_stop: bool,

    /// Run the sender in the background forever (until killed manually).
    #[clap(long)]
    pub bg: bool,

    /// Show debug details: content hash, per-file listing, import speed.
    #[clap(long)]
    pub debug: bool,

    /// Store the receive command in the clipboard.
    #[cfg(feature = "clipboard")]
    #[clap(short = 'c', long)]
    pub clipboard: bool,
}

#[derive(Parser, Debug)]
pub struct ReceiveArgs {
    /// The ticket to use to connect to the sender.
    #[clap(conflicts_with = "scan")]
    pub ticket: Option<BlobTicket>,

    /// Discover senders on the local network and pick one interactively.
    #[clap(long)]
    pub scan: bool,

    /// Keep the partial download cache after a failed or interrupted
    /// transfer, so a retry can resume where it left off.
    #[clap(long)]
    pub resume: bool,

    /// Directory to export the received files into.
    ///
    /// Created if it does not exist. Defaults to the current directory.
    pub dest: Option<PathBuf>,

    #[clap(flatten)]
    pub common: CommonArgs,
}

/// Expand a leading `~` in a path to the user's home directory.
fn expand_home(path: &Path) -> PathBuf {
    let Some(str_path) = path.to_str() else {
        return path.to_path_buf();
    };
    if str_path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = str_path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

/// Options to configure what is included in a [`EndpointAddr`]
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
pub enum AddrInfoOptions {
    /// Only the Endpoint ID is added.
    ///
    /// This usually means that iroh-dns address lookup is used to find address information.
    #[default]
    Id,
    /// Includes the Endpoint ID and both the relay URL, and the direct addresses.
    RelayAndAddresses,
    /// Includes the Endpoint ID and the relay URL.
    Relay,
    /// Includes the Endpoint ID and the direct addresses.
    Addresses,
}

impl Display for AddrInfoOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id => f.write_str("id"),
            Self::RelayAndAddresses => f.write_str("relay-and-addresses"),
            Self::Relay => f.write_str("relay"),
            Self::Addresses => f.write_str("addresses"),
        }
    }
}

impl FromStr for AddrInfoOptions {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().replace('_', "-").as_str() {
            "id" => Ok(Self::Id),
            "relay-and-addresses" => Ok(Self::RelayAndAddresses),
            "relay" => Ok(Self::Relay),
            "addresses" => Ok(Self::Addresses),
            _ => Err(anyhow::anyhow!(
                "invalid ticket type, expected id, relay, addresses or relay-and-addresses"
            )),
        }
    }
}

fn apply_options(addr: &mut EndpointAddr, opts: AddrInfoOptions) {
    match opts {
        AddrInfoOptions::Id => {
            addr.addrs = Default::default();
        }
        AddrInfoOptions::RelayAndAddresses => {
            // nothing to do
        }
        AddrInfoOptions::Relay => {
            addr.addrs = addr
                .addrs
                .iter()
                .filter(|addr| matches!(addr, TransportAddr::Relay(_)))
                .cloned()
                .collect();
        }
        AddrInfoOptions::Addresses => {
            addr.addrs = addr
                .addrs
                .iter()
                .filter(|addr| matches!(addr, TransportAddr::Ip(_)))
                .cloned()
                .collect();
        }
    }
}

/// Get the secret key or generate a new one.
///
/// Print the secret key to stderr if it was generated, so the user can save it.
fn get_or_create_secret(print: bool) -> anyhow::Result<SecretKey> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => SecretKey::from_str(&secret).context("invalid secret"),
        Err(_) => {
            let key = SecretKey::generate();
            if print {
                let key = hex::encode(key.to_bytes());
                eprintln!("using secret key {key}");
            }
            Ok(key)
        }
    }
}

fn validate_path_component(component: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !component.contains('/'),
        "path components must not contain the only correct path separator, /"
    );
    Ok(())
}

/// This function converts an already canonicalized path to a string.
///
/// If `must_be_relative` is true, the function will fail if any component of the path is
/// `Component::RootDir`
///
/// This function will also fail if the path is non canonical, i.e. contains
/// `..` or `.`, or if the path components contain any windows or unix path
/// separators.
pub fn canonicalized_path_to_string(
    path: impl AsRef<Path>,
    must_be_relative: bool,
) -> anyhow::Result<String> {
    let mut path_str = String::new();
    let parts = path
        .as_ref()
        .components()
        .filter_map(|c| match c {
            Component::Normal(x) => {
                let c = match x.to_str() {
                    Some(c) => c,
                    None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                };

                if !c.contains('/') && !c.contains('\\') {
                    Some(Ok(c))
                } else {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                }
            }
            Component::RootDir => {
                if must_be_relative {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                } else {
                    path_str.push('/');
                    None
                }
            }
            _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let parts = parts.join("/");
    path_str.push_str(&parts);
    Ok(path_str)
}

/// Import from a file or directory into the database.
///
/// The returned tag always refers to a collection. If the input is a file, this
/// is a collection with a single blob, named like the file.
///
/// If the input is a directory, the collection contains all the files in the
/// directory.
async fn import(
    path: PathBuf,
    db: &Store,
    mp: &mut MultiProgress,
    jobs: Option<usize>,
) -> anyhow::Result<(TempTag, u64, Collection)> {
    let parallelism = jobs.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display());
    let root = path.parent().context("context get parent")?;
    // walkdir also works for files, so we don't need to special case them
    let files = WalkDir::new(path.clone()).into_iter();
    // flatten the directory structure into a list of (name, path) pairs.
    // ignore symlinks.
    let data_sources: Vec<(String, PathBuf)> = files
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                // Skip symlinks. Directories are handled by WalkDir.
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<anyhow::Result<Vec<_>>>()?;
    // import all the files, using num_cpus workers, return names and temp tags
    // Only show an overall bar when there are multiple files. For a single
    // file it would just flash in and out.
    let op = if data_sources.len() > 1 {
        let op = mp.add(make_import_overall_progress());
        op.set_message(format!("importing {} files", data_sources.len()));
        op.set_length(data_sources.len() as u64);
        Some(op)
    } else {
        None
    };
    let mut names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            let op = op.clone();
            let mp = mp.clone();
            async move {
                if let Some(op) = &op {
                    op.inc(1);
                }
                let import = db.add_path_with_opts(AddPathOptions {
                    path,
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                });
                let mut stream = import.stream().await;
                let mut item_size = 0;
                // Only show a per-file bar for larger files; small ones finish
                // so fast the bar would just flash.
                let mut pb: Option<ProgressBar> = None;
                let temp_tag = loop {
                    let item = stream
                        .next()
                        .await
                        .context("import stream ended without a tag")?;
                    trace!("importing {name} {item:?}");
                    match item {
                        AddProgressItem::Size(size) => {
                            item_size = size;
                            if size > MIN_IMPORT_BAR_BYTES {
                                let bar = mp.add(make_import_item_progress());
                                bar.set_message(format!("copying {name}"));
                                bar.set_length(size);
                                pb = Some(bar);
                            }
                        }
                        AddProgressItem::CopyProgress(offset) => {
                            if let Some(pb) = &pb {
                                pb.set_position(offset);
                            }
                        }
                        AddProgressItem::CopyDone => {
                            if let Some(pb) = &pb {
                                pb.set_message(format!("computing outboard {name}"));
                                pb.set_position(0);
                            }
                        }
                        AddProgressItem::OutboardProgress(offset) => {
                            if let Some(pb) = &pb {
                                pb.set_position(offset);
                            }
                        }
                        AddProgressItem::Error(cause) => {
                            if let Some(pb) = &pb {
                                pb.finish_and_clear();
                            }
                            anyhow::bail!("error importing {}: {}", name, cause);
                        }
                        AddProgressItem::Done(tt) => {
                            if let Some(pb) = &pb {
                                pb.finish_and_clear();
                            }
                            break tt;
                        }
                    }
                };
                anyhow::Ok((name, temp_tag, item_size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    if let Some(op) = &op {
        op.finish_and_clear();
    }
    names_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    // total size of all files
    let size = names_and_tags.iter().map(|(_, _, size)| *size).sum::<u64>();
    // collect the (name, hash) tuples into a collection
    // we must also keep the tags around so the data does not get gced.
    let (collection, tags) = names_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(db).await?;
    // now that the collection is stored, we can drop the tags
    // data is protected by the collection
    drop(tags);
    Ok((temp_tag, size, collection))
}

fn get_export_path(root: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let parts = name.split('/');
    let mut path = root.to_path_buf();
    for part in parts {
        validate_path_component(part)?;
        path.push(part);
    }
    Ok(path)
}

async fn export(
    db: &Store,
    collection: Collection,
    mp: &mut MultiProgress,
    root: &Path,
) -> anyhow::Result<()> {
    let op = mp.add(make_export_overall_progress());
    op.set_length(collection.len() as u64);
    for (i, (name, hash)) in collection.iter().enumerate() {
        op.set_position(i as u64);
        let target = get_export_path(root, name)?;
        if target.exists() {
            eprintln!(
                "target {} already exists. Export stopped.",
                target.display()
            );
            eprintln!(
                "You can remove the file or directory and try again. The download will not be repeated."
            );
            anyhow::bail!("target {} already exists", target.display());
        }
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::Copy,
            })
            .stream()
            .await;
        let pb = mp.add(make_export_item_progress());
        pb.set_message(format!("exporting {name}"));
        while let Some(item) = stream.next().await {
            match item {
                ExportProgressItem::Size(size) => {
                    pb.set_length(size);
                }
                ExportProgressItem::CopyProgress(offset) => {
                    pb.set_position(offset);
                }
                ExportProgressItem::Done => {
                    pb.finish_and_clear();
                }
                ExportProgressItem::Error(cause) => {
                    pb.finish_and_clear();
                    anyhow::bail!("error exporting {}: {}", name, cause);
                }
            }
        }
    }
    op.finish_and_clear();
    Ok(())
}

#[derive(Debug)]
struct PerConnectionProgress {
    endpoint_id: String,
    requests: BTreeMap<u64, ProgressBar>,
    /// At least one get request completed on this connection.
    served: bool,
}

async fn per_request_progress(
    mp: MultiProgress,
    connection_id: u64,
    request_id: u64,
    connections: Arc<Mutex<BTreeMap<u64, PerConnectionProgress>>>,
    mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
) {
    let pb = mp.add(ProgressBar::hidden());
    let endpoint_id = if let Some(connection) = connections.lock().unwrap().get_mut(&connection_id)
    {
        connection.requests.insert(request_id, pb.clone());
        connection.endpoint_id.clone()
    } else {
        error!("got request for unknown connection {connection_id}");
        return;
    };
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
        ).unwrap()
        .progress_chars("#>-"),
    );
    while let Ok(Some(msg)) = rx.recv().await {
        match msg {
            RequestUpdate::Started(msg) => {
                pb.set_message(format!(
                    "n {} r {}/{} i {} # {}",
                    endpoint_id,
                    connection_id,
                    request_id,
                    msg.index,
                    msg.hash.fmt_short()
                ));
                pb.set_length(msg.size);
            }
            RequestUpdate::Progress(msg) => {
                pb.set_position(msg.end_offset);
            }
            RequestUpdate::Completed(_) => {
                if let Some(msg) = connections.lock().unwrap().get_mut(&connection_id) {
                    msg.served = true;
                    msg.requests.remove(&request_id);
                };
            }
            RequestUpdate::Aborted(_) => {
                if let Some(msg) = connections.lock().unwrap().get_mut(&connection_id) {
                    msg.requests.remove(&request_id);
                };
            }
        }
    }
    pb.finish_and_clear();
    mp.remove(&pb);
}

async fn show_provide_progress(
    mp: MultiProgress,
    mut recv: mpsc::Receiver<ProviderMessage>,
    done: mpsc::Sender<()>,
) -> anyhow::Result<()> {
    let connections = Arc::new(Mutex::new(BTreeMap::new()));
    let mut tasks = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            item = recv.recv() => {
                let Some(item) = item else {
                    break;
                };

                trace!("got event {item:?}");
                match item {
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        let endpoint_id = msg.endpoint_id.map(|id| id.fmt_short().to_string()).unwrap_or_else(|| "?".to_string());
                        let connection_id = msg.connection_id;
                        connections.lock().unwrap().insert(
                            connection_id,
                            PerConnectionProgress {
                                requests: BTreeMap::new(),
                                endpoint_id,
                                served: false,
                            },
                        );
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        // Do the map mutation without holding the lock across
                        // the await below (keeps the future Send).
                        let served = {
                            let mut map = connections.lock().unwrap();
                            match map.remove(&msg.connection_id) {
                                Some(connection) => {
                                    for pb in connection.requests.values() {
                                        pb.finish_and_clear();
                                        mp.remove(pb);
                                    }
                                    connection.served
                                }
                                None => false,
                            }
                        };
                        // The receiver downloaded at least one request and
                        // disconnected: that counts as a complete send.
                        if served {
                            let _ = done.send(()).await;
                        }
                    }
                    ProviderMessage::GetRequestReceivedNotify(msg) => {
                        let request_id = msg.request_id;
                        let connection_id = msg.connection_id;
                        let connections = connections.clone();
                        let mp = mp.clone();
                        tasks.push(per_request_progress(mp, connection_id, request_id, connections, msg.rx));
                    }
                    _ => {}
                }
            }
            Some(_) = tasks.next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.next().await.is_some() {}
    Ok(())
}

/// Spawn a detached background sender, wait for it to produce its ticket,
/// print it, then return (freeing the shell).
async fn spawn_background(args: SendArgs) -> anyhow::Result<()> {
    use std::process::{Command, Stdio};

    let suffix = SecretKey::generate().to_bytes();
    let ticket_file =
        std::env::temp_dir().join(format!("dashe-ticket-{}.txt", hex::encode(&suffix[..8])));
    let _ = std::fs::remove_file(&ticket_file);

    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.args(std::env::args_os().skip(1))
        .env("DASHE_DAEMON", "1")
        .env("DASHE_TICKET_FILE", &ticket_file)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = cmd.spawn().context("failed to start background sender")?;
    let pid = child.id();

    // Wait for the worker to finish importing and write its ticket.
    let deadline = Instant::now() + Duration::from_secs(60 * 30);
    let ticket = loop {
        if let Ok(content) = std::fs::read_to_string(&ticket_file) {
            let t = content.trim().to_string();
            if !t.is_empty() {
                break t;
            }
        }
        if child.try_wait()?.is_some() {
            anyhow::bail!("background sender exited before sharing (bad path or setup failure)");
        }
        if Instant::now() > deadline {
            anyhow::bail!("timed out waiting for the background sender");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    println!("{}", style(format!("dshe receive {ticket}")).bold(),);
    if args.qr {
        print_ticket_qr(&ticket)?;
    }
    let mode = if args.bg_stop {
        "stops automatically after the first transfer"
    } else {
        "stops when killed"
    };
    println!(
        "{}",
        style(format!(
            "running in background (pid {pid}, {mode}; stop with: kill {pid}))"
        ))
        .dim(),
    );
    Ok(())
}

/// Print the receive command as a scannable QR code (inverted colors so it
/// scans on dark terminal backgrounds).
fn print_ticket_qr(ticket: &str) -> anyhow::Result<()> {
    use qrcode::render::unicode::Dense1x2;
    let code = qrcode::QrCode::new(format!("dshe receive {ticket}"))?;
    let qr = code
        .render::<Dense1x2>()
        .dark_color(Dense1x2::Light)
        .light_color(Dense1x2::Dark)
        .quiet_zone(true)
        .build();
    println!();
    println!("{qr}");
    println!();
    Ok(())
}

async fn send(args: SendArgs) -> anyhow::Result<()> {
    // Background mode: the first invocation spawns a detached worker and
    // waits for it to write its ticket, prints it, then frees the shell.
    // The worker re-runs this same function with DASHE_DAEMON set.
    if (args.bg || args.bg_stop) && std::env::var("DASHE_DAEMON").is_err() {
        return spawn_background(args).await;
    }
    #[cfg(all(unix, feature = "clipboard"))]
    if (args.bg || args.bg_stop) && std::env::var("DASHE_DAEMON").is_ok() {
        // Detach from the controlling terminal so closing it doesn't kill us.
        unsafe {
            let _ = libc::setsid();
        }
    }

    let secret_key = get_or_create_secret(args.common.verbose > 0)?;
    if args.common.show_secret {
        let secret_key = hex::encode(secret_key.to_bytes());
        eprintln!("using secret key {secret_key}");
    }
    // create a magicsocket endpoint
    let relay_mode: RelayMode = args.common.relay.into();
    let mut builder = Endpoint::builder(presets::N0)
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .secret_key(secret_key)
        .relay_mode(relay_mode.clone());
    if args.ticket_type == AddrInfoOptions::Id {
        builder = builder.address_lookup(PkarrPublisher::n0_dns());
    }
    if let Some(addr) = args.common.magic_ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    if let Some(addr) = args.common.magic_ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }

    // use a flat store - todo: use a partial in mem store instead
    // random suffix from the crypto RNG already in the tree (no rand dep needed)
    let suffix = SecretKey::generate().to_bytes();
    let cwd = std::env::current_dir()?;
    let blobs_data_dir = cwd.join(format!(".dashe-send-{}", hex::encode(&suffix[..16])));
    if blobs_data_dir.exists() {
        println!(
            "can not share twice from the same directory: {}",
            cwd.display(),
        );
        std::process::exit(1);
    }
    // todo: remove this as soon as we have a mem store that does not require a temp dir,
    // or create a temp dir outside the current directory.
    if cwd.join(&args.path) == cwd {
        println!("can not share from the current directory");
        std::process::exit(1);
    }

    let mut mp = MultiProgress::new();
    let mp2 = mp.clone();
    let path = args.path;

    // Compress folders into a single tar.gz before sending, unless
    // --noarchive was given. The receiver unpacks it automatically.
    let archived = path.is_dir() && !args.noarchive;
    let share_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .context("invalid path name")?
        .to_string();
    let archive_file: Option<PathBuf> = if archived {
        let folder_name = share_name.clone();
        let archive_dir = std::env::temp_dir().join(format!("dashe-{}", hex::encode(&suffix[..8])));
        std::fs::create_dir_all(&archive_dir)?;
        let archive_path = archive_dir.join(format!("{folder_name}.tar.gz"));
        let file = std::fs::File::create(&archive_path)?;
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar = tar::Builder::new(enc);
        tar.append_dir_all(&folder_name, &path)?;
        tar.into_inner()?.finish()?;
        Some(archive_path)
    } else {
        None
    };
    let import_path = match &archive_file {
        Some(p) => p.clone(),
        None => path.clone(),
    };

    let path2 = import_path;
    let blobs_data_dir2 = blobs_data_dir.clone();
    let (progress_tx, progress_rx) = mpsc::channel(32);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
    let progress = AbortOnDropHandle::new(n0_future::task::spawn(show_provide_progress(
        mp2,
        progress_rx,
        done_tx,
    )));
    let setup = async move {
        let t0 = Instant::now();
        tokio::fs::create_dir_all(&blobs_data_dir2).await?;

        let endpoint = builder.bind().await?;
        let draw_target = if args.common.no_progress {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stderr()
        };
        mp.set_draw_target(draw_target);
        let store = FsStore::load(&blobs_data_dir2).await?;
        let blobs = BlobsProtocol::new(
            &store,
            Some(EventSender::new(
                progress_tx,
                EventMask {
                    connected: ConnectMode::Notify,
                    get: provider::events::RequestMode::NotifyLog,
                    ..EventMask::DEFAULT
                },
            )),
        );

        let import_result = import(path2, blobs.store(), &mut mp, args.common.jobs).await?;
        let dt = t0.elapsed();

        let router = iroh::protocol::Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn();

        // wait for the endpoint to figure out its address before making a ticket
        let ep = router.endpoint();
        tokio::time::timeout(Duration::from_secs(30), async move {
            if !matches!(relay_mode, RelayMode::Disabled) {
                let _ = ep.online().await;
            }
        })
        .await?;

        anyhow::Ok((router, import_result, dt))
    };
    let (router, (temp_tag, size, collection), dt) = select! {
        x = setup => x?,
        _ = tokio::signal::ctrl_c() => {
            std::process::exit(130);
        }
    };
    let hash = temp_tag.hash();

    // make a ticket
    let mut addr = router.endpoint().addr();
    apply_options(&mut addr, args.ticket_type);
    let ticket = BlobTicket::new(addr, hash, BlobFormat::HashSeq);

    // Announce the share on the local network (SHAREit-style beacons) so
    // `dshe receive --scan` can find it without any ticket exchange.
    {
        let endpoint = router.endpoint();
        let endpoint_id = endpoint.addr().id.to_string();
        let addrs: Vec<std::net::SocketAddr> = endpoint.addr().ip_addrs().copied().collect();
        n0_future::task::spawn(announce_share_beacon(
            endpoint_id,
            addrs,
            hash.to_hex().to_string(),
            true,
            size,
            share_name.clone(),
        ));
    }

    let entry_type = if path.is_file() {
        "file"
    } else if archived {
        "folder (compressed)"
    } else {
        "directory"
    };
    println!(
        "imported {} {} ({})",
        entry_type,
        style(path.display()).bold(),
        HumanBytes(size),
    );
    if args.debug || args.common.verbose > 1 {
        println!(
            "  {} {}",
            style("hash").dim(),
            print_hash(&hash, args.common.format),
        );
        for (name, hash) in collection.iter() {
            println!("    {} {name}", print_hash(hash, args.common.format));
        }
        println!(
            "  imported in {}s, {}/s",
            dt.as_secs_f64(),
            HumanBytes(((size as f64) / dt.as_secs_f64()).floor() as u64)
        );
    }

    println!();
    println!(
        "  {} {}",
        style("dshe").bold().cyan(),
        style(format!("receive {ticket}")).bold(),
    );

    // Hand the ticket to the foreground process that spawned us.
    if let Ok(ticket_file) = std::env::var("DASHE_TICKET_FILE") {
        let _ = std::fs::write(&ticket_file, ticket.to_string());
    }

    if args.qr {
        print_ticket_qr(&ticket.to_string())?;
    }

    #[cfg(feature = "clipboard")]
    {
        if args.qr {
            // No key listener with --qr (nothing to hint about); still honor
            // an explicit --clipboard request, just without the prompt.
            if args.clipboard {
                add_to_clipboard(&ticket);
            }
        } else {
            handle_key_press(args.clipboard, ticket);
        }
    }

    // Exit after the first complete transfer, or on Ctrl-C.
    // With --nostop or --bg the sender keeps running until killed manually.
    let complete = tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = done_rx.recv(), if !args.nostop && !args.bg => true,
    };

    drop(temp_tag);

    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    tokio::fs::remove_dir_all(blobs_data_dir).await?;
    if let Some(archive_path) = &archive_file {
        let _ = std::fs::remove_file(archive_path);
        if let Some(parent) = archive_path.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }
    // drop everything that owns blobs to close the progress sender
    drop(router);
    // await progress completion so the progress bars are cleared first
    progress.await.ok();
    if complete {
        println!("{}", style("transfer complete").green());
    }

    Ok(())
}

#[cfg(feature = "clipboard")]
fn handle_key_press(set_clipboard: bool, ticket: BlobTicket) {
    #[cfg(any(unix, windows))]
    use std::io;

    use crossterm::{
        event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        terminal::{disable_raw_mode, enable_raw_mode},
    };
    #[cfg(unix)]
    use libc::{raise, SIGINT};
    #[cfg(windows)]
    use windows_sys::Win32::System::Console::{GenerateConsoleCtrlEvent, CTRL_C_EVENT};

    if set_clipboard {
        add_to_clipboard(&ticket);
    }

    // Without a terminal there are no key events to listen for, and polling
    // the crossterm EventStream panics with "reader source not set".
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return;
    }

    let _keyboard = tokio::task::spawn(async move {
        println!("{}", style("press c to copy the receive command").dim());

        // `enable_raw_mode` will remember the current terminal mode
        // and restore it when `disable_raw_mode` is called.
        enable_raw_mode().unwrap_or_else(|err| eprintln!("Failed to enable raw mode: {err}"));
        EventStream::new()
            .for_each(move |e| match e {
                Err(err) => eprintln!("Failed to process event: {err}"),
                // c is pressed
                Ok(Event::Key(KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::NONE,
                    kind: KeyEventKind::Press,
                    ..
                })) => add_to_clipboard(&ticket),
                // Ctrl+c is pressed
                Ok(Event::Key(KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::CONTROL,
                    kind: KeyEventKind::Press,
                    ..
                })) => {
                    disable_raw_mode()
                        .unwrap_or_else(|e| eprintln!("Failed to disable raw mode: {e}"));

                    #[cfg(unix)]
                    // Safety: Raw syscall to re-send the SIGINT signal to the console.
                    // `raise` returns nonzero for failure.
                    if unsafe { raise(SIGINT) } != 0 {
                        eprintln!("Failed to raise signal: {}", io::Error::last_os_error());
                    }

                    #[cfg(windows)]
                    // Safety: Raw syscall to re-send the `CTRL_C_EVENT` to the console.
                    // `GenerateConsoleCtrlEvent` returns 0 for failure.
                    if unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) } == 0 {
                        eprintln!(
                            "Failed to generate console event: {}",
                            io::Error::last_os_error()
                        );
                    }
                }
                _ => {}
            })
            .await
    });
}

#[cfg(feature = "clipboard")]
fn add_to_clipboard(ticket: &BlobTicket) {
    use std::io::stdout;

    use crossterm::{clipboard::CopyToClipboard, execute};

    execute!(
        stdout(),
        CopyToClipboard::to_clipboard_from(format!("dshe receive {ticket}"))
    )
    .unwrap_or_else(|e| eprintln!("Failed to copy to clipboard: {e}"));
}

const TICK_MS: u64 = 250;

/// UDP port used for local-network sender discovery (SHAREit-style beacons).
const BEACON_PORT: u16 = 51510;
/// Multicast group for beacons (works alongside 255.255.255.255 broadcast).
const BEACON_MULTICAST: std::net::Ipv4Addr = std::net::Ipv4Addr::new(239, 255, 43, 211);

/// Periodically announce a running share on the local network.
/// The packet carries everything a receiver needs to build a ticket:
/// endpoint id, direct addresses, content hash, format, size and name.
async fn announce_share_beacon(
    endpoint_id: String,
    addrs: Vec<std::net::SocketAddr>,
    hash: String,
    hash_seq: bool,
    size: u64,
    name: String,
) -> anyhow::Result<()> {
    let socket = tokio::net::UdpSocket::bind(("0.0.0.0", 0)).await?;
    socket.set_broadcast(true)?;
    let addr_list = addrs
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let packet = format!(
        "DSHE1|{endpoint_id}|{hash}|{}|{size}|{addr_list}|{name}",
        u8::from(hash_seq),
    );
    let targets = [
        (std::net::Ipv4Addr::BROADCAST, BEACON_PORT),
        (BEACON_MULTICAST, BEACON_PORT),
    ];
    loop {
        for target in targets {
            let _ = socket.send_to(packet.as_bytes(), target).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Clone)]
struct DiscoveredSender {
    endpoint_id: String,
    hash: String,
    hash_seq: bool,
    name: String,
    size: u64,
    addrs: Vec<std::net::SocketAddr>,
}

/// Listen for beacons for `duration` and return the discovered senders.
async fn discover_senders(duration: Duration) -> anyhow::Result<Vec<DiscoveredSender>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    socket.bind(&std::net::SocketAddr::from(([0, 0, 0, 0], BEACON_PORT)).into())?;
    socket
        .join_multicast_v4(&BEACON_MULTICAST, &std::net::Ipv4Addr::UNSPECIFIED)
        .ok();
    socket.set_nonblocking(true)?;
    let socket = tokio::net::UdpSocket::from_std(std::net::UdpSocket::from(socket))?;

    let mut buf = [0u8; 2048];
    let mut found: BTreeMap<String, DiscoveredSender> = BTreeMap::new();
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let timeout = deadline.saturating_duration_since(Instant::now());
        let Ok(Ok((len, _from))) = tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await
        else {
            break; // scan window over
        };
        let Ok(text) = std::str::from_utf8(&buf[..len]) else {
            continue;
        };
        let Some(rest) = text.strip_prefix("DSHE1|") else {
            continue;
        };
        let mut parts = rest.splitn(6, '|');
        let (Some(id), Some(hash), Some(format), Some(size), Some(addrs), Some(name)) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            continue;
        };
        // Dedupe by endpoint id; a later beacon may know more addresses.
        let entry = found
            .entry(id.to_string())
            .or_insert_with(|| DiscoveredSender {
                endpoint_id: id.to_string(),
                hash: hash.to_string(),
                hash_seq: format == "1",
                name: name.to_string(),
                size: size.parse().unwrap_or(0),
                addrs: Vec::new(),
            });
        for a in addrs.split(',') {
            if let Ok(sa) = a.parse::<std::net::SocketAddr>() {
                if !entry.addrs.contains(&sa) {
                    entry.addrs.push(sa);
                }
            }
        }
    }
    Ok(found.into_values().collect())
}

/// Scan the local network and let the user pick a sender.
async fn scan_and_pick() -> anyhow::Result<BlobTicket> {
    println!("scanning the local network for senders...");
    let senders = discover_senders(Duration::from_secs(5)).await?;
    if senders.is_empty() {
        anyhow::bail!(
            "no senders found on the local network (is someone running `dshe send` on this network?)"
        );
    }
    // A single sender means there's nothing to choose — connect directly.
    if senders.len() == 1 {
        let s = &senders[0];
        println!(
            "{}",
            style(format!(
                "found {} ({}) from {}",
                s.name,
                HumanBytes(s.size),
                s.addrs
                    .first()
                    .map(|a| a.ip().to_string())
                    .unwrap_or_else(|| "?".to_string()),
            ))
            .cyan(),
        );
        return build_ticket(s);
    }
    for (i, s) in senders.iter().enumerate() {
        let from = s
            .addrs
            .first()
            .map(|a| a.ip().to_string())
            .unwrap_or_else(|| "?".to_string());
        println!(
            "  [{}] {} ({}) from {}",
            i + 1,
            style(&s.name).bold(),
            HumanBytes(s.size),
            from,
        );
    }
    print!("pick a sender (1-{}): ", senders.len());
    use std::io::Write as _;
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let pick: usize = line.trim().parse().context("invalid pick")?;
    let sender = senders
        .get(pick.checked_sub(1).context("invalid pick")?)
        .context("invalid pick")?;
    build_ticket(sender)
}

/// Build a ticket from a discovered sender's beacon data.
fn build_ticket(sender: &DiscoveredSender) -> anyhow::Result<BlobTicket> {
    let mut addr = EndpointAddr::new(
        sender
            .endpoint_id
            .parse()
            .context("invalid endpoint id in beacon")?,
    );
    for sa in &sender.addrs {
        addr.addrs.insert(TransportAddr::Ip(*sa));
    }
    let format = if sender.hash_seq {
        BlobFormat::HashSeq
    } else {
        BlobFormat::Raw
    };
    let hash: Hash = sender.hash.parse().context("invalid hash in beacon")?;
    Ok(BlobTicket::new(addr, hash, format))
}

/// Files smaller than this don't get their own import progress bar —
/// they finish too fast and the bar just flashes.
const MIN_IMPORT_BAR_BYTES: u64 = 8 * 1024 * 1024;

fn make_import_overall_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb
}

fn make_import_item_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template("{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}

fn make_download_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template(" downloading [{wide_bar:.cyan/blue}] {binary_remaining_bytes}/{binary_total_bytes} {msg}")
            .unwrap()
            .progress_chars("██░"),
    );
    pb
}

fn make_export_overall_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template("{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {human_pos}/{human_len} {per_sec}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}

fn make_export_item_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb
}

pub async fn show_download_progress(
    mp: MultiProgress,
    mut recv: mpsc::Receiver<u64>,
    local_size: u64,
    total_size: u64,
) -> anyhow::Result<()> {
    let op = mp.add(make_download_progress());
    op.set_length(total_size);
    let started = Instant::now();
    let mut last = (local_size, started);
    let mut rate: f64 = 0.0;
    while let Some(offset) = recv.recv().await {
        let pos = (local_size + offset).min(total_size);
        op.set_position(pos);
        // Estimate the transfer rate and show a humanized ETA in the message.
        let now = Instant::now();
        let dt = now.duration_since(last.1).as_secs_f64();
        if dt >= 0.5 {
            let inst = (pos.saturating_sub(last.0)) as f64 / dt;
            rate = if rate == 0.0 {
                inst
            } else {
                rate * 0.7 + inst * 0.3
            };
            last = (pos, now);
        }
        if rate > 0.0 {
            let remaining = total_size.saturating_sub(pos);
            let eta = Duration::from_secs_f64(remaining as f64 / rate);
            op.set_message(format!("({} left)", HumanDuration(eta)));
        }
    }
    op.finish_and_clear();
    Ok(())
}

fn show_get_error(e: GetError) -> GetError {
    match &e {
        GetError::InitialNext { source, .. } => eprintln!(
            "{}",
            style(format!("initial connection error: {source}")).yellow()
        ),

        GetError::ConnectedNext { source, .. } => {
            eprintln!("{}", style(format!("connected error: {source}")).yellow())
        }
        GetError::AtBlobHeaderNext { source, .. } => eprintln!(
            "{}",
            style(format!("reading blob header error: {source}")).yellow()
        ),
        GetError::Decode { source, .. } => {
            eprintln!("{}", style(format!("decoding error: {source}")).yellow())
        }
        GetError::IrpcSend { source, .. } => eprintln!(
            "{}",
            style(format!("error sending over irpc: {source}")).yellow()
        ),
        GetError::AtClosingNext { source, .. } => {
            eprintln!("{}", style(format!("error at closing: {source}")).yellow())
        }
        GetError::BadRequest { .. } => eprintln!("{}", style("bad request").yellow()),
        GetError::LocalFailure { source, .. } => {
            eprintln!("{} {source:?}", style("local failure").yellow())
        }
    }
    e
}

async fn receive(args: ReceiveArgs) -> anyhow::Result<()> {
    let ticket = match args.ticket {
        Some(ticket) => ticket,
        None if args.scan => scan_and_pick().await?,
        None => anyhow::bail!("provide a ticket, or use --scan to find senders on this network"),
    };
    let addr = ticket.addr().clone();
    let secret_key = get_or_create_secret(args.common.verbose > 0)?;
    // Destination directory for the received files.
    let dest = match args.dest {
        Some(dest) => {
            let expanded = expand_home(&dest);
            std::fs::create_dir_all(&expanded)?;
            expanded.canonicalize()?
        }
        None => std::env::current_dir()?,
    };
    let mut builder = Endpoint::builder(presets::N0)
        .alpns(vec![])
        .secret_key(secret_key)
        .relay_mode(args.common.relay.into());

    if ticket.addr().relay_urls().next().is_none() && ticket.addr().ip_addrs().next().is_none() {
        builder = builder.address_lookup(DnsAddressLookup::n0_dns());
    }
    if let Some(addr) = args.common.magic_ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    if let Some(addr) = args.common.magic_ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }
    let endpoint = builder.bind().await?;
    let dir_name = format!(".dashe-recv-{}", ticket.hash().to_hex());
    let iroh_data_dir = std::env::current_dir()?.join(dir_name);
    let db = iroh_blobs::store::fs::FsStore::load(&iroh_data_dir).await?;
    let db2 = db.clone();
    trace!("load done!");
    let fut = async {
        trace!("running");
        let mut mp: MultiProgress = MultiProgress::new();
        let draw_target = if args.common.no_progress {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stderr()
        };
        mp.set_draw_target(draw_target);
        let hash_and_format = ticket.hash_and_format();
        trace!("computing local");
        let local = db.remote().local(hash_and_format).await?;
        trace!("local done");
        let (stats, total_files, payload_size) = if !local.is_complete() {
            trace!("{} not complete", hash_and_format.hash);
            let connection = endpoint.connect(addr, iroh_blobs::protocol::ALPN).await?;
            let (_hash_seq, sizes) =
                get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                    .await
                    .map_err(show_get_error)?;
            let total_size = sizes.iter().copied().sum::<u64>();
            let payload_size = sizes.iter().skip(1).copied().sum::<u64>();
            let total_files = (sizes.len().saturating_sub(1)) as u64;
            let noun = if total_files == 1 { "file" } else { "files" };
            eprintln!(
                "{} {} {} ({})",
                style("fetching").cyan(),
                total_files,
                noun,
                HumanBytes(payload_size),
            );
            // print the details of the collection only in verbose mode
            if args.common.verbose > 0 {
                eprintln!(
                    "getting {} blobs in total, {}",
                    total_files + 1,
                    HumanBytes(total_size)
                );
            }
            let (tx, rx) = mpsc::channel(32);
            let local_size = local.local_bytes();
            let get = db.remote().execute_get(connection, local.missing());
            let task = tokio::spawn(show_download_progress(
                mp.clone(),
                rx,
                local_size,
                total_size,
            ));
            // let mut stream = get.stream();
            let mut stats = Stats::default();
            let mut stream = get.stream();
            while let Some(item) = stream.next().await {
                trace!("got item {item:?}");
                match item {
                    GetProgressItem::Progress(offset) => {
                        tx.send(offset).await.ok();
                    }
                    GetProgressItem::Done(value) => {
                        stats = value;
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!(show_get_error(cause));
                    }
                }
            }
            drop(tx);
            task.await.ok();
            (stats, total_files, payload_size)
        } else {
            println!("{} already complete", hash_and_format.hash);
            let total_files = local.children().unwrap() - 1;
            let payload_bytes = 0; // todo local.sizes().skip(2).map(Option::unwrap).sum::<u64>();
            (Stats::default(), total_files, payload_bytes)
        };
        let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;
        if args.common.verbose > 1 {
            for (name, hash) in collection.iter() {
                println!("    {} {name}", print_hash(hash, args.common.format));
            }
        }
        let first_name = collection
            .iter()
            .next()
            .map(|(name, _)| name.clone())
            .unwrap_or_default();
        // A single .tar.gz entry means the sender archived a folder.
        let is_archive = collection.len() == 1 && first_name.ends_with(".tar.gz");
        let root_name = if is_archive {
            first_name
                .strip_suffix(".tar.gz")
                .unwrap_or(&first_name)
                .to_string()
        } else {
            first_name
                .split('/')
                .next()
                .unwrap_or(&first_name)
                .to_string()
        };
        export(&db, collection, &mut mp, &dest).await?;
        if is_archive {
            // Unpack the archive into the destination directory and drop it.
            let archive_target = dest.join(&first_name);
            let file = std::fs::File::open(&archive_target)
                .with_context(|| format!("open {}", archive_target.display()))?;
            let gz = flate2::read::GzDecoder::new(file);
            tar::Archive::new(gz).unpack(&dest)?;
            std::fs::remove_file(&archive_target)?;
        }
        anyhow::Ok((root_name, total_files, payload_size, stats))
    };
    let (root_name, total_files, payload_size, stats) = select! {
        x = fut => match x {
            Ok(x) => {
                endpoint.close().await;
                x
            }
            Err(e) => {
                endpoint.close().await;
                // make sure we shutdown the db before exiting
                db2.shutdown().await?;
                // without --resume the partial cache is deleted; with it the
                // retry will pick up where this attempt left off
                if !args.resume {
                    let _ = tokio::fs::remove_dir_all(&iroh_data_dir).await;
                }
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
        _ = tokio::signal::ctrl_c() => {
            endpoint.close().await;
            db2.shutdown().await?;
            if !args.resume {
                let _ = tokio::fs::remove_dir_all(&iroh_data_dir).await;
            }
            std::process::exit(130);
        }
    };
    tokio::fs::remove_dir_all(iroh_data_dir).await?;
    println!(
        "{} {} ({}) in {}",
        style("received").green(),
        style(root_name).bold(),
        HumanBytes(payload_size),
        HumanDuration(stats.elapsed),
    );
    if args.common.verbose > 0 {
        println!(
            "downloaded {} files, {}. took {} ({}/s)",
            total_files,
            HumanBytes(payload_size),
            HumanDuration(stats.elapsed),
            HumanBytes((stats.total_bytes_read() as f64 / stats.elapsed.as_secs_f64()) as u64),
        );
    }
    Ok(())
}

/// Ask the user whether to send the given path. Returns true for y/yes.
/// In an interactive terminal, a single keypress is enough (no Enter).
fn confirm_send(path: &Path) -> anyhow::Result<bool> {
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display(),);
    use std::io::{IsTerminal, Read, Write as _};
    print!("send {}? [y/N] ", path.display());
    std::io::stdout().flush()?;
    let answer = if std::io::stdin().is_terminal() {
        // Single keypress, no Enter needed.
        #[cfg(feature = "clipboard")]
        {
            use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
            enable_raw_mode()?;
            let mut buf = [0u8; 1];
            let n = std::io::stdin().read(&mut buf)?;
            disable_raw_mode()?;
            // Echo the pressed key since raw mode does not.
            if n == 1 && buf[0].is_ascii_graphic() {
                println!("{}", buf[0] as char);
            } else {
                println!();
            }
            std::io::stdout().flush()?;
            if n == 1 && buf[0] == 3 {
                // Ctrl-C in raw mode is just a byte; honor it as an interrupt.
                std::process::exit(130);
            }
            n == 1 && (buf[0] == b'y' || buf[0] == b'Y')
        }
        #[cfg(not(feature = "clipboard"))]
        {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            let answer = line.trim().to_ascii_lowercase();
            answer == "y" || answer == "yes"
        }
    } else {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        let answer = line.trim().to_ascii_lowercase();
        answer == "y" || answer == "yes"
    };
    Ok(answer)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(cause) => {
            if let Some(text) = cause.get(ContextKind::InvalidSubcommand) {
                eprintln!("{} \"{}\"\n", ErrorKind::InvalidSubcommand, text);
                eprintln!("Available subcommands are");
                for cmd in Args::command().get_subcommands() {
                    eprintln!("    {}", style(cmd.get_name()).bold());
                }
                std::process::exit(1);
            } else {
                cause.exit();
            }
        }
    };
    let command = match args.command {
        Some(command) => command,
        None => {
            let Some(path) = args.path else {
                Args::command().print_help()?;
                std::process::exit(2);
            };
            if !confirm_send(&path)? {
                println!("aborted");
                std::process::exit(0);
            }
            // Re-parse through clap so the defaults are applied.
            let path_str = path.as_os_str().to_str().context("invalid path")?;
            Commands::Send(SendArgs::try_parse_from(["dshe", path_str])?)
        }
    };
    let res = match command {
        Commands::Send(args) => send(args).await,
        Commands::Receive(args) => receive(args).await,
    };
    if let Err(e) = &res {
        eprintln!("{e}");
    }
    match res {
        Ok(()) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
