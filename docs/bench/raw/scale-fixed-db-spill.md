# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=20000/8 procs, ELECTRIC_CIRCUITS_FEED_TRACE=0

| phase | users | subscriptions | live shapes | engine RSS (MiB) | engine footprint (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 24.2 | 9.8 | 1283.1 | 0 | 0 |
| created | 100 | 1000 | 505 | 195.5 | 41.0 | 1354.6 | 100 | 600 |
| created | 250 | 2500 | 1255 | 214.8 | 60.0 | 1381.1 | 250 | 1500 |
| created | 500 | 5000 | 2505 | 253.5 | 98.0 | 1416.8 | 500 | 3000 |
| created | 1000 | 10000 | 5005 | 271.4 | 116.0 | 128.8 | 1000 | 6000 |
| created | 2500 | 25000 | 12505 | 401.7 | 237.0 | 247.0 | 2500 | 15000 |
| created | 5000 | 50000 | 25005 | 445.7 | 441.0 | 375.2 | 5000 | 30000 |
| created | 10000 | 100000 | 50005 | 576.8 | 699.0 | 489.3 | 10000 | 60000 |
| live subs 5000 | 10000 | 100000 | 50005 | 377.5 | 699.0 | 443.7 | 10000 | 60000 |
| live subs 10000 | 10000 | 100000 | 50005 | 51.8 | 657.0 | 140.0 | 10000 | 60000 |
| live subs 20000 | 10000 | 100000 | 50005 | 149.7 | 657.0 | 29.6 | 10000 | 60000 |
| live subs 20000 +15s | 10000 | 100000 | 50005 | 49.6 | 657.0 | 52.1 | 10000 | 60000 |
