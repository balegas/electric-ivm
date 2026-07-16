# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=0/8 procs, ELECTRIC_CIRCUITS_FEED_TRACE=0

| phase | users | subscriptions | live shapes | engine RSS (MiB) | engine footprint (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 24.3 | 10.0 | 813.8 | 0 | 0 |
| created | 10000 | 100000 | 50005 | 343.3 | 690.0 | 81.9 | 10000 | 60000 |
