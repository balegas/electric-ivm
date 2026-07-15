# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=20000/8 procs, ELECTRIC_IVM_FEED_TRACE=0

| phase | users | subscriptions | live shapes | engine RSS (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 23.9 | 1283.1 | 0 | 0 |
| created | 1000 | 10000 | 5005 | 170.4 | 134.8 | 1000 | 6000 |
| created | 2500 | 25000 | 12505 | 330.4 | 253.2 | 2500 | 15000 |
| created | 5000 | 50000 | 25005 | 492.4 | 429.2 | 5000 | 30000 |
| created | 10000 | 100000 | 50005 | 408.1 | 117.8 | 10000 | 60000 |
| live subs 20000 | 10000 | 100000 | 50005 | 162.8 | 190.6 | 10000 | 60000 |
| live subs 20000 +15s | 10000 | 100000 | 50005 | 43.7 | 94.5 | 10000 | 60000 |
