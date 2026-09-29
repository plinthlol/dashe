# dshe~

send files between machines. no cloud, no accounts, no cables.

dshe is a small CLI tool for moving files and folders from one machine to
another — over your local network or across the internet. it's a fork-flavored
rewrite of [sendme](https://github.com/n0-computer/sendme) built on
[iroh](https://iroh.computer): the sender just exists on the network, the
receiver sees it, picks it, done.

everything is encrypted end-to-end and verified against a blake3 hash, so a
corrupted or tampered transfer fails loudly instead of quietly handing you
garbage.

## install

linux / macos:

```sh
curl -fsSL https://plinthlol.github.io/dashe/install.sh | sh
```

windows (powershell):

```powershell
irm https://plinthlol.github.io/dashe/install.ps1 | iex
```

this drops the binary into `~/.local/bin` (linux/macos) or
`%LOCALAPPDATA%\Programs\dshe` (windows), and the installer tells you if you
need to touch your PATH. grab the tarball yourself from the
[releases page](https://github.com/plinthlol/dashe/releases) if piping scripts
into a shell isn't your thing.

or build it the old way:

```sh
cargo build --release
```

## usage

on the sending machine:

```sh
$ dshe send myfolder
imported folder (compressed) myfolder (977.14 KiB)

  dshe receive blobav3vanxqbcbyn7a46lqimpk47utzp7wnuu...
```

on the receiving machine — you can paste the ticket, or just scan the local
network and pick the sender:

```sh
$ dshe receive --scan
scanning the local network for senders...
  [1] myfolder (977.14 KiB) from 10.0.1.172
pick a sender (1-1): 1
received myfolder (977.14 KiB) in 3s
```

with exactly one sender nearby it doesn't even ask — it just connects. senders
broadcast a tiny beacon on the LAN every second, so this works with the router
unplugged. turn on a phone hotspot, both machines join it, done.

you can send several things in one go:

```sh
$ dshe send a.txt b.md docs/
imported share a.txt, b.md, docs (1.2 MiB)
```

each file lands under its own name and each folder keeps its name as a prefix,
so the receiver gets `a.txt`, `b.md` and `docs/...` side by side. sending a
single folder still packs it into a tar.gz; several paths are sent as-is, and
send flags go after a `--`:

```sh
$ dshe send a.txt b.md -- --noarchive
```

you can also point the receiver somewhere specific:

```sh
$ dshe receive <ticket> ~/somewhere
```

## the flags

| flag | what it does |
|---|---|
| `--scan` | find senders on the local network, no ticket needed |
| `--qr` | print the receive command as a QR code |
| `--bg` / `--bg-stop` | run the sender in the background (forever / until the first receiver finishes) |
| `--nostop` | keep serving after the first receiver, instead of exiting |
| `--noarchive` | send folders as-is instead of packing them into a tar.gz |
| `--resume` | keep the partial download after a failed transfer, so a retry continues |
| `--debug` | show the content hash, per-file listing and import speed |
| `--relay <url\|disabled>` | pick a relay server, or go fully offline (LAN-only) |

## how it works, roughly

senders announce themselves with a tiny UDP beacon on the local network every
second — endpoint id, addresses, content hash, name. a receiver collects those
beacons, builds a ticket from the one you pick, and then the actual transfer
happens peer-to-peer over QUIC with NAT hole punching. if a direct connection
can't be made, traffic falls back to a relay; it's still encrypted either way,
the relay just shuttles ciphertext.

the content hash travels with the ticket and every chunk of the transfer is
checked against it. fake beacons, lying relays, broken cables — anything that
corrupts the data makes the transfer fail instead of writing bad bytes to your
disk.

the sender exits by itself after the first receiver completes. `--nostop` if
you want it to stick around, `--bg` if you want it in the background.

## notes

- folder shares are packed into a single tar.gz before sending and unpacked on
  the other side; `--noarchive` keeps them as plain files. this applies to a
  single folder — sending several paths sends them as a plain collection
- received archives are unpacked without following links or paths that point
  outside the destination, and are refused if they expand past 64 GiB or
  100,000 entries
- interrupted transfers keep their partial cache only with `--resume`,
  otherwise it's cleaned up
## license

mit.
