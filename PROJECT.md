# Redis Server Implementation in Go

A compliant, high-performance in-memory key-value database and cache built from first principles in Go. Implements the core Redis protocol (RESP), concurrent storage engines, persistence layers, master-replica streaming, pub/sub messaging, geospatial queries, and dynamic access control.

## 🚀 Key Features & Modules

- **Core Engine & Data Structures**:
  - In-memory thread-safe key-value store with granular locking and passive/active key expiry.
  - Full support for Strings, Integers, Lists (`LPUSH`, `RPUSH`, `LPOP`, `LRANGE`), and Sorted Sets (`ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`).
  - Bitmaps manipulation using `SETBIT`, `GETBIT`, `BITCOUNT`, and bitwise `BITOP` operations.
  - Append-only Streams (`XADD`, `XRANGE`, `XREAD` with blocking support).

- **Transactions & Concurrency**:
  - ACID-style atomic transactions using `MULTI`, `EXEC`, and `DISCARD`.
  - Optimistic concurrency control via `WATCH` / `UNWATCH` key tracking.
  - Event-driven connection handling with dedicated reader/writer pipelines.

- **Persistence (AOF & RDB)**:
  - **Append-Only File (AOF)** persistence engine supporting configurable fsync strategies (`always`, `everysec`, `no`).
  - **RDB Snapshot Parsing**: Native decoding of RDB binary snapshots to restore database state on startup.

- **Master-Replica Replication**:
  - Full two-way handshake (`PING`, `REPLCONF listening-port`, `REPLCONF capa`, `PSYNC`).
  - Asynchronous command streaming and offset propagation from master to replicas.
  - Synchronous write coordination via `WAIT` and `REPLCONF ACK` telemetry tracking.

- **Pub/Sub Engine**:
  - Channel subscription and broadcasting (`SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`).
  - Subscribed-state command validation and real-time socket demultiplexing.

- **Geospatial Indexing & Mathematics**:
  - 52-bit geohash Morton coding (EPSG:3857) via bitwise coordinate interleaving and bit compaction.
  - Haversine distance formula with exact Earth radius constants (`GEODIST`).
  - Radial searches (`GEOSEARCH` with `FROMLONLAT` and `BYRADIUS`).

- **Access Control Lists (ACL) & Security**:
  - Dynamic user state engine tracking user flags (`nopass`) and SHA-256 hashed passwords.
  - Connection-level authentication enforcement (`AUTH`, `ACL GETUSER`, `ACL SETUSER`, `ACL WHOAMI`).
