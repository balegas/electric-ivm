# Shape-memory at scale — 100000 issues, 10000 users, 100000 subscriptions

Config: projects=2000, memberships/user=6, shapes/user=10, materialized=true, clientProcs=4, liveSubs=20000/8 procs, ELECTRIC_IVM_FEED_TRACE=0

| phase | users | subscriptions | live shapes | engine RSS (MiB) | engine footprint (MiB) | ds RSS (MiB) | sq nodes | contributors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 0 | 0 | 0 | 24.1 | 10.0 | 1283.1 | 0 | 0 |
| created | 100 | 1000 | 505 | 168.1 | 93.0 | 1364.5 | 100 | 600 |
| created | 250 | 2500 | 1255 | 173.9 | 98.0 | 439.0 | 250 | 1500 |
| created | 500 | 5000 | 2505 | 182.5 | 90.0 | 385.7 | 500 | 3000 |
| created | 1000 | 10000 | 5005 | 130.5 | 142.0 | 130.7 | 1000 | 6000 |
| created | 2500 | 25000 | 12505 | 291.6 | 253.0 | 233.6 | 2500 | 15000 |
| created | 5000 | 50000 | 25005 | 456.5 | 350.0 | 404.1 | 5000 | 30000 |
| created | 10000 | 100000 | 50005 | 423.6 | 789.0 | 165.6 | 10000 | 60000 |
| live subs 5000 | 10000 | 100000 | 50005 | 38.6 | 692.0 | 145.3 | 10000 | 60000 |
| live subs 10000 | 10000 | 100000 | 50005 | 37.7 | 698.0 | 19.4 | 10000 | 60000 |
| live subs 20000 | 10000 | 100000 | 50005 | 33.5 | 698.0 | 25.8 | 10000 | 60000 |
| live subs 20000 +15s | 10000 | 100000 | 50005 | 35.5 | 698.0 | 373.2 | 10000 | 60000 |
