# Accord Protocol Demo

This demo showcases the Accord protocol's live message tracing and transaction processing in a distributed Cassandra cluster.

## Prerequisites

- Docker
- A web browser

## Launching the Demo

To launch the demo with default settings (including slow-motion), run:

```bash
./demo.sh
```

### Options

- `--fast`: Disable slow-motion mode.
- `--test`: Run a shorter benchmark (60s) and optimize container resources for the local machine.
- `--protocols=LIST`: Override the protocols to run (default is `accord`).

## Visualization

Once the demo starts, it will automatically attempt to open `http://localhost:3000` in your default web browser.

The visualization includes:
- **World Map**: Shows the geographical location of the Cassandra nodes.
- **Message Flow**: Animates messages (REQ, RSP, ACCEPT, COMMIT/APPLY) between nodes in real-time.
- **Database State**: Shows the current balances of user accounts (represented by robots).
- **Transaction Toasts**: Notifies when a transfer transaction completes.

## Slow-Motion Mode (Default)

The slow-motion mode is enabled by default to make it easier to follow the sequence of messages for each transaction. It queues incoming log events and releases them at a controlled pace. Use `--fast` if you prefer to see the real-time processing speed.
