# Pony Raft

![Pony Raft][pony-raft.png]

This library implements the [Raft][raft] algorithm, for [Pony][pony-lang],
following the Raft [paper][raft-paper].

Please note, this is still a young project and this library has not yet been
"battle tested."

### Features:

- Leader Election + Log Replication: Yes
- Persistence: No
- Membership Changes: No
- Log Compaction: No

## Build

```bash
ponyc
```

[raft]: https://raft.github.io/ "The Raft Consensus Algorithm"
[raft-paper]: https://raft.github.io/raft.pdf "In Search of an Understandable Consensus Algorithm"
[pony-raft.png]:pony-raft.png "Pony Raft"
[pony-lang]:https://ponylang.io "Pony Actor Model Language"
