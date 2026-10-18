<div align="center">

# Paxos Meeting Manager

**A replicated meeting manager built on Paxos, with Multi-Paxos, Vertical Paxos reconfiguration and a consensus-free fast path for TOPIC queries.**

![Java](https://img.shields.io/badge/Java-22-orange?logo=openjdk&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-multi--module-C71A36?logo=apachemaven&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protocol%20Buffers-3.12-4285F4)
![Consensus](https://img.shields.io/badge/Consensus-Paxos-6f42c1)

[Features](#features) · [Architecture](#architecture) · [Getting Started](#getting-started) · [Running](#running) · [Report](#report)

</div>

---

## Overview

Meeting Manager is a distributed system for managing meetings. Clients can open a meeting, add participants and close it, and all servers apply these operations in the same order, even when a server fails or the cluster is reconfigured.

The servers agree on that order with Paxos. The project starts from a simplified Paxos baseline and extends it step by step: full Phase 1 recovery, Multi-Paxos, reconfiguration with Vertical Paxos (VP2), and a fast path for TOPIC queries that skips consensus.

It was built for the *Design and Implementation of Distributed Applications* course (2025-2026) at Instituto Superior Técnico.

## Features

| Feature | What it does |
|---|---|
| **Multi-Paxos** | The leader proposes new commands continuously. Each Phase 2 instance runs in its own thread, so commands do not wait for earlier ones to be decided. |
| **Phase 1 recovery** | A new leader collects the logs of a quorum of acceptors and keeps the entry with the highest ballot for each instance. Instances with no value are filled with a NO-OP. |
| **Reconfiguration (Vertical Paxos VP2)** | The console starts a new ballot and activates the new leader only after the previous ballot is completed. This lets the system change its configuration safely. |
| **Ordered apply thread** | A separate thread applies decided commands strictly in log order, so every replica ends up with the same state while consensus keeps running. |
| **Fast TOPIC** | TOPIC queries run directly on one server, without consensus. The client sends the last Paxos instance it has seen, and the server answers only after it has applied that instance, so the client's view never goes backwards. |

## Architecture

The project is a Maven multi-module build:

| Module | Role |
|---|---|
| `server/` | A Paxos replica. Contains the main loop, the proposer (Phase 1 and Phase 2), the acceptor, the learner and the apply thread. |
| `app/` | Client with a terminal where you issue meeting commands. |
| `consoleclient/` | The master of the VP2 protocol. Starts ballots and activates new leaders. Also turns debug mode on and off for a replica. |
| `contract/` | The `.proto` files that define the messages between all components. |
| `core/` | Classes that manage meetings. |
| `configs/` | Classes that manage configurations. |
| `util/` | Helpers for collecting RPC responses. |

**How a command is handled.** The leader checks whether Phase 1 must run, recovers any missing entries from the previous ballot, and waits for the console's activation. Then it proposes client commands in Phase 2. Once a quorum of acceptors accepts an instance, the apply thread executes it in order.

**How Fast TOPIC stays consistent.** Only `openMeeting` and `addParticipant` can change the result of a TOPIC query, so only they return the last applied instance number to the client. A server answers a TOPIC request after the apply thread has reached the instance the client sent.

## Tech Stack

| Area | Technologies |
|---|---|
| Language | Java 22 |
| Messaging | Protocol Buffers (`protoc` 3.12), RPC between all components |
| Build | Apache Maven |
| Algorithms | Paxos, Multi-Paxos, Vertical Paxos (VP2) |

## Getting Started

### Prerequisites

- Java 22
- Apache Maven 3.8.4
- Protoc 3.12

On Linux/Ubuntu (Intel x86), `setup_env.sh` downloads all of them into an `INSTALL_DIR` folder. Activate that folder with:

```bash
source INSTALL_DIR/env.sh
```

### Choose the contract `pom.xml`

The `contract` module needs a different `pom.xml` depending on your CPU and operating system. Before the first build, copy the right one to `pom.xml` inside `contract/`:

| System | File to copy |
|---|---|
| ARM / Apple Silicon Mac | `arm-pom.xml` |
| Intel / Linux | `intel-pom.xml` |

```bash
cp contract/intel-pom.xml contract/pom.xml
```

### Build

From the repository root:

```bash
mvn clean install
```

## Running

The current setup runs everything on one machine and needs **five servers**. Open a separate terminal for each component.

### 1. Servers

In the `server/` directory, start servers with ids `0` to `4`:

```bash
mvn exec:java -Dexec.args="{port} {id} {scheduler} {max}"
```

| Argument | Meaning |
|---|---|
| `port` | Base port. Use the same value for all servers. Each server listens on `port + id`. |
| `id` | Sequential server id, from `0` to `N-1` |
| `scheduler` | Scheduler used for reconfiguration. Use `A` to start. |
| `max` | Maximum number of participants in a meeting |

Example for server 0: `mvn exec:java -Dexec.args="8000 0 A 5"`

### 2. Console

In the `consoleclient/` directory:

```bash
mvn exec:java -Dexec.args="{host} {port} {scheduler}"
```

Use `localhost` as the host. The port and scheduler are the same as for the servers. Commands:

| Command | Effect |
|---|---|
| `help` | Lists all commands |
| `ballot <ballot_number> <server>` | Tells a server to start a ballot with that number |
| `debug <mode> <replica_id>` | Turns on debug mode for a replica |
| `exit` | Closes the console |

A new leader only starts Phase 2 after the console activates it, so start a ballot here before sending client commands.

### 3. Client

In the `app/` directory:

```bash
mvn exec:java -Dexec.args="{id} {host} {port} {scheduler}"
```

| Argument | Meaning |
|---|---|
| `id` | Sequential client id, starting at `1` |
| `host` | Use `localhost` |
| `port` | Base port of the servers |
| `scheduler` | Use `A` to start |

The client opens a terminal. Type `help` to list the available commands, and `exit` to close it.

## Report

The [project report](docs/report.pdf) explains the design in detail: the main loop, Phase 1 and Phase 2, the apply thread, Vertical Paxos and Fast TOPIC.

<details>
<summary><b>Repository layout</b></summary>

```
.
├── server/           # Paxos replica
├── app/              # Client
├── consoleclient/    # VP2 master / configuration console
├── contract/         # .proto files (arm-pom.xml, intel-pom.xml)
├── core/             # Meeting logic
├── configs/          # Configuration classes
├── util/             # RPC response helpers
├── setup_env.sh      # Optional: downloads Java, Maven and Protoc
└── pom.xml           # Parent Maven build
```

</details>

## Authors

- [Diogo Almada](https://github.com/almada39)
- [Miguel Barbosa](https://github.com/MiguelCBar)
- [Rita Melo](https://github.com/ritamelo06)

## References

1. L. Lamport. *Paxos Made Simple*. ACM SIGACT News, 2001.
2. L. Lamport, D. Malkhi, L. Zhou. *Vertical Paxos and Primary-Backup Replication*. Microsoft Research, 2009.