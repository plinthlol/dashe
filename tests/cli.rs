use std::{
    io::{self, Read},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
};

use iroh_blobs::ticket::BlobTicket;

/// Serializes the network tests so their LAN beacons don't cross-talk
/// (a `--scan` receiver could otherwise pick another test's sender).
static NET_TEST_LOCK: Mutex<()> = Mutex::new(());

// binary path
fn dshe_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dshe")
}

/// Read from `reader` until a line starting with `dshe receive` is found,
/// returning the ticket that follows the command name.
fn read_ticket(reader: &mut impl Read) -> io::Result<String> {
    let mut line = Vec::new();
    let mut buf = [0u8; 1];
    loop {
        if reader.read(&mut buf)? != 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ticket line not found in output",
            ));
        }
        if buf[0] == b'\n' {
            let text = String::from_utf8_lossy(&line).trim().to_string();
            line.clear();
            if let Some(ticket) = text.strip_prefix("dshe receive ") {
                return Ok(ticket.trim().to_string());
            }
        } else {
            line.push(buf[0]);
        }
    }
}

// fn wait2() -> Arc<Barrier> {
//     Arc::new(Barrier::new(2))
// }

// /// generate a random, non privileged port
// fn random_port() -> u16 {
//     rand::thread_rng().gen_range(10000u16..60000)
// }

#[test]
fn send_recv_file() {
    let name = "somefile.bin";
    let data = vec![0u8; 100];
    // create src and tgt dir, and src file
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let src_file = src_dir.path().join(name);
    std::fs::write(&src_file, &data).unwrap();
    let mut send_cmd = duct::cmd(dshe_bin(), ["send", src_file.as_os_str().to_str().unwrap()])
        .dir(src_dir.path())
        .env_remove("RUST_LOG") // disable tracing
        .stderr_to_stdout()
        .reader()
        .unwrap();
    let ticket = read_ticket(&mut send_cmd).unwrap();
    let ticket = BlobTicket::from_str(&ticket).unwrap();
    let receive_output = duct::cmd(dshe_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir.path())
        .env_remove("RUST_LOG") // disable tracing
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output.status.success());
    let tgt_file = tgt_dir.path().join(name);
    let tgt_data = std::fs::read(tgt_file).unwrap();
    assert_eq!(tgt_data, data);
}

#[test]
fn receive_closes_endpoint_no_iroh_socket_error() {
    let name = "graceful-close.bin";
    let data = vec![0xabu8; 64];
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let src_file = src_dir.path().join(name);
    std::fs::write(&src_file, &data).unwrap();
    let mut send_cmd = duct::cmd(dshe_bin(), ["send", src_file.as_os_str().to_str().unwrap()])
        .dir(src_dir.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .reader()
        .unwrap();
    let ticket = read_ticket(&mut send_cmd).unwrap();
    let ticket = BlobTicket::from_str(&ticket).unwrap();
    let receive_output = duct::cmd(dshe_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir.path())
        .env("RUST_LOG", "iroh::socket=error")
        .stdout_capture()
        .stderr_capture()
        .run()
        .unwrap();
    assert!(receive_output.status.success(), "{receive_output:?}");
    let stderr = String::from_utf8_lossy(&receive_output.stderr);
    assert!(
        !stderr.contains("Endpoint dropped"),
        "unexpected iroh shutdown log on stderr: {stderr}"
    );
    assert!(
        !stderr.contains("Aborting ungracefully"),
        "unexpected iroh shutdown log on stderr: {stderr}"
    );
    let tgt_file = tgt_dir.path().join(name);
    assert_eq!(std::fs::read(&tgt_file).unwrap(), data);
}

#[test]
fn send_recv_dir() {
    fn create_file(base: &Path, i: usize, j: usize, k: usize) -> (PathBuf, Vec<u8>) {
        let name = base
            .join(format!("dir-{i}"))
            .join(format!("subdir-{j}"))
            .join(format!("file-{k}"));
        let len = i * 100 + j * 10 + k;
        let data = vec![0u8; len];
        (name, data)
    }

    // create src and tgt dir, and src file
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let src_data_dir = src_dir.path().join("data");
    let tgt_data_dir = tgt_dir.path().join("data");
    // create a complex directory structure
    for i in 0..5 {
        for j in 0..5 {
            for k in 0..5 {
                let (name, data) = create_file(&src_data_dir, i, j, k);
                std::fs::create_dir_all(name.parent().unwrap()).unwrap();
                std::fs::write(&name, &data).unwrap();
            }
        }
    }
    let mut send_cmd = duct::cmd(
        dshe_bin(),
        [
            "send",
            "--noarchive",
            src_data_dir.as_os_str().to_str().unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env_remove("RUST_LOG") // disable tracing
    .stderr_to_stdout()
    .reader()
    .unwrap();
    let ticket = read_ticket(&mut send_cmd).unwrap();
    let ticket = BlobTicket::from_str(&ticket).unwrap();
    let receive_output = duct::cmd(dshe_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir.path())
        .env_remove("RUST_LOG") // disable tracing
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output.status.success());
    // validate directory structure
    for i in 0..5 {
        for j in 0..5 {
            for k in 0..5 {
                let (name, data) = create_file(&tgt_data_dir, i, j, k);
                let tgt_data = std::fs::read(&name).unwrap();
                assert_eq!(tgt_data, data);
            }
        }
    }
}

/// Send a folder (default: compressed into a single tar.gz), receive it into
/// an explicit destination directory, and verify the structure is restored.
#[test]
fn send_recv_folder_archive_with_dest() {
    let _guard = NET_TEST_LOCK.lock().unwrap();
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let dest = tgt_dir.path().join("out");
    let folder = src_dir.path().join("myfolder");
    std::fs::create_dir_all(folder.join("sub")).unwrap();
    std::fs::write(folder.join("a.txt"), "hello").unwrap();
    std::fs::write(folder.join("sub").join("b.txt"), "world").unwrap();

    let mut send_cmd = duct::cmd(dshe_bin(), ["send", folder.as_os_str().to_str().unwrap()])
        .dir(src_dir.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .reader()
        .unwrap();
    let ticket = read_ticket(&mut send_cmd).unwrap();
    let ticket = BlobTicket::from_str(&ticket).unwrap();
    let receive_output = duct::cmd(
        dshe_bin(),
        [
            "receive",
            &ticket.to_string(),
            dest.as_os_str().to_str().unwrap(),
        ],
    )
    .dir(tgt_dir.path())
    .env_remove("RUST_LOG")
    .stderr_to_stdout()
    .run()
    .unwrap();
    assert!(receive_output.status.success(), "{receive_output:?}");
    assert_eq!(
        std::fs::read_to_string(dest.join("myfolder").join("a.txt")).unwrap(),
        "hello"
    );
    assert_eq!(
        std::fs::read_to_string(dest.join("myfolder").join("sub").join("b.txt")).unwrap(),
        "world"
    );
}

/// SHAREit-style discovery: a sender beaconing on the LAN is found by
/// `receive --scan` and picked by number.
#[test]
fn scan_finds_and_receives() {
    let _guard = NET_TEST_LOCK.lock().unwrap();
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src_dir.path().join("scanned")).unwrap();
    std::fs::write(
        src_dir.path().join("scanned").join("f.bin"),
        vec![7u8; 1000],
    )
    .unwrap();

    let mut send_cmd = duct::cmd(
        dshe_bin(),
        [
            "send",
            "--noarchive",
            "--relay",
            "disabled",
            src_dir.path().join("scanned").as_os_str().to_str().unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env_remove("RUST_LOG")
    .stderr_to_stdout()
    .reader()
    .unwrap();
    let ticket = read_ticket(&mut send_cmd).unwrap();

    let receive_output = duct::cmd(dshe_bin(), ["receive", "--scan", "--relay", "disabled"])
        .dir(tgt_dir.path())
        .stdin_bytes("1\n".as_bytes())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output.status.success(), "{receive_output:?}");
    assert_eq!(
        std::fs::read(tgt_dir.path().join("scanned").join("f.bin")).unwrap(),
        vec![7u8; 1000]
    );
    drop(ticket);
}

/// `--bg-stop`: the sender detaches into the background, the shell is freed,
/// and the background worker exits by itself after the receiver finishes.
#[test]
fn bg_stop_exits_after_transfer() {
    let _guard = NET_TEST_LOCK.lock().unwrap();
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    std::fs::write(src_dir.path().join("bg.bin"), vec![0xABu8; 64]).unwrap();

    let send_cmd = duct::cmd(
        dshe_bin(),
        [
            "send",
            "--bg-stop",
            "--relay",
            "disabled",
            src_dir.path().join("bg.bin").as_os_str().to_str().unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env_remove("RUST_LOG")
    .stderr_to_stdout()
    .stdout_capture()
    .stderr_capture()
    .start()
    .unwrap();
    // The background parent exits quickly; its output ends with the ticket.
    let output = send_cmd.wait().unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let ticket = stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("dshe receive "))
        .map(|t| t.trim().to_string())
        .expect("ticket in bg output: {stdout}");

    let receive_output = duct::cmd(dshe_bin(), ["receive", "--relay", "disabled", &ticket])
        .dir(tgt_dir.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output.status.success(), "{receive_output:?}");
    assert_eq!(
        std::fs::read(tgt_dir.path().join("bg.bin")).unwrap(),
        vec![0xABu8; 64]
    );

    // Give the detached worker a moment to notice the transfer and stop.
    std::thread::sleep(std::time::Duration::from_secs(5));
    // The worker cleans up its store dir on exit; none should remain.
    let leftovers: Vec<_> = std::fs::read_dir(src_dir.path())
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().to_string_lossy().to_string();
            name.starts_with(".dashe-send-").then_some(name)
        })
        .collect();
    assert!(leftovers.is_empty(), "worker left behind: {leftovers:?}");
}
