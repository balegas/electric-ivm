# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=20000/8 procs, ELECTRIC_IVM_FEED_TRACE=0

| phase | users | subscriptions | live shapes | engine RSS (MiB) | engine footprint (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 24.0 | 9.5 | 1283.1 | 0 | 0 |
| created | 2500 | 25000 | 12505 | 393.7 | 224.0 | 251.5 | 2500 | 15000 |
| created | 10000 | 100000 | 50005 | 698.4 | 609.0 | 737.0 | 10000 | 60000 |
| live subs 20000 | 10000 | 100000 | 50005 | 455.8 | 642.0 | 336.3 | 10000 | 60000 |
| live subs 20000 +15s | 10000 | 100000 | 50005 | 455.9 | 642.0 | 353.3 | 10000 | 60000 |
