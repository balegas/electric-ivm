# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=20000/8 procs, ELECTRIC_CIRCUITS_FEED_TRACE=1

| phase | users | subscriptions | live shapes | engine RSS (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 24.2 | 1282.5 | 0 | 0 |
| created | 1000 | 10000 | 5005 | 198.0 | 312.1 | 1000 | 6000 |
| created | 2500 | 25000 | 12505 | 353.4 | 248.9 | 2500 | 15000 |
| created | 5000 | 50000 | 25005 | 349.0 | 257.3 | 5000 | 30000 |
| created | 10000 | 100000 | 50005 | 731.8 | 631.2 | 10000 | 60000 |
| live subs held | 10000 | 100000 | 50005 | 271.4 | 320.2 | 10000 | 60000 |
| live subs held +15s | 10000 | 100000 | 50005 | 221.7 | 360.5 | 10000 | 60000 |
