
#### History (limit-first), schema v3: no match_players(player_id) index
```text
Nested Loop (actual time=15.241..16.352 rows=20.00 loops=1)
  Buffers: shared hit=4798
  ->  Nested Loop (actual time=15.154..15.648 rows=20.00 loops=1)
        Buffers: shared hit=4538
        ->  Limit (actual time=15.110..15.553 rows=20.00 loops=1)
              Buffers: shared hit=4478
              ->  Gather Merge (actual time=15.105..15.546 rows=20.00 loops=1)
                    Workers Planned: 2
                    Workers Launched: 2
                    Buffers: shared hit=4478
                    ->  Sort (actual time=5.867..5.869 rows=11.67 loops=3)
                          Sort Key: match_players.started_at DESC
                          Sort Method: top-N heapsort  Memory: 27kB
                          Buffers: shared hit=4478
                          Worker 0:  Sort Method: quicksort  Memory: 26kB
                          Worker 1:  Sort Method: quicksort  Memory: 25kB
                          ->  Parallel Seq Scan on match_players (actual time=0.050..5.778 rows=40.00 loops=3)
                                Filter: (player_id = '1234'::bigint)
                                Rows Removed by Filter: 199960
                                Buffers: shared hit=4412
        ->  Append (actual time=0.004..0.004 rows=1.00 loops=20)
              Buffers: shared hit=60
              ->  Index Scan using matches_2026_09_pkey on matches_2026_09 m_1 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_10_pkey on matches_2026_10 m_2 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_11_pkey on matches_2026_11 m_3 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_12_pkey on matches_2026_12 m_4 (actual time=0.003..0.003 rows=1.00 loops=20)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 20
                    Buffers: shared hit=60
              ->  Seq Scan on matches_default m_5 (never executed)
                    Filter: ((id = match_players.match_id) AND (started_at = match_players.started_at))
  ->  Aggregate (actual time=0.035..0.035 rows=1.00 loops=20)
        Buffers: shared hit=260
        ->  Sort (actual time=0.024..0.025 rows=10.00 loops=20)
              Sort Key: mp.score DESC, mp.player_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=260
              ->  Bitmap Heap Scan on match_players mp (actual time=0.011..0.016 rows=10.00 loops=20)
                    Recheck Cond: (match_id = m.id)
                    Heap Blocks: exact=200
                    Buffers: shared hit=260
                    ->  Bitmap Index Scan on match_players_pkey (actual time=0.008..0.008 rows=10.00 loops=20)
                          Index Cond: (match_id = m.id)
                          Index Searches: 20
                          Buffers: shared hit=60
Planning:
  Buffers: shared hit=254
Planning Time: 1.146 ms
Execution Time: 16.510 ms
```

| query | schema | runs | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| history limit-last | v3 no index | 50 | 13.67 | 46.80 | 57.92 |
| history limit-first | v3 no index | 50 | 12.02 | 42.31 | 47.43 |
| history limit-last | v4 indexed | 500 | 5.70 | 7.07 | 9.09 |
| history limit-first (1 query) | v4 indexed | 500 | 0.80 | 1.23 | 1.52 |
| history N+1 (21 queries) | v4 indexed | 500 | 7.75 | 10.83 | 13.51 |

#### History limit-last, schema v4
```text
Limit (actual time=11.907..11.912 rows=20.00 loops=1)
  Buffers: shared hit=2128
  ->  Sort (actual time=11.905..11.909 rows=20.00 loops=1)
        Sort Key: m.started_at DESC
        Sort Method: top-N heapsort  Memory: 45kB
        Buffers: shared hit=2128
        ->  Nested Loop (actual time=0.282..11.860 rows=120.00 loops=1)
              Buffers: shared hit=2128
              ->  Hash Join (actual time=0.240..9.492 rows=120.00 loops=1)
                    Hash Cond: ((m.id = me.match_id) AND (m.started_at = me.started_at))
                    Buffers: shared hit=568
                    ->  Append (actual time=0.010..6.581 rows=60000.00 loops=1)
                          Buffers: shared hit=444
                          ->  Seq Scan on matches_2026_09 m_1 (actual time=0.009..0.822 rows=14982.00 loops=1)
                                Buffers: shared hit=111
                          ->  Seq Scan on matches_2026_10 m_2 (actual time=0.006..0.846 rows=15482.00 loops=1)
                                Buffers: shared hit=114
                          ->  Seq Scan on matches_2026_11 m_3 (actual time=0.005..0.851 rows=14983.00 loops=1)
                                Buffers: shared hit=111
                          ->  Seq Scan on matches_2026_12 m_4 (actual time=0.007..0.810 rows=14553.00 loops=1)
                                Buffers: shared hit=108
                          ->  Seq Scan on matches_default m_5 (actual time=0.004..0.004 rows=0.00 loops=1)
                    ->  Hash (actual time=0.133..0.134 rows=120.00 loops=1)
                          Buckets: 1024  Batches: 1  Memory Usage: 15kB
                          Buffers: shared hit=124
                          ->  Bitmap Heap Scan on match_players me (actual time=0.021..0.116 rows=120.00 loops=1)
                                Recheck Cond: (player_id = '1234'::bigint)
                                Heap Blocks: exact=120
                                Buffers: shared hit=124
                                ->  Bitmap Index Scan on match_players_player_started_idx (actual time=0.010..0.010 rows=120.00 loops=1)
                                      Index Cond: (player_id = '1234'::bigint)
                                      Index Searches: 1
                                      Buffers: shared hit=4
              ->  Aggregate (actual time=0.019..0.019 rows=1.00 loops=120)
                    Buffers: shared hit=1560
                    ->  Sort (actual time=0.013..0.013 rows=10.00 loops=120)
                          Sort Key: mp.score DESC, mp.player_id
                          Sort Method: quicksort  Memory: 25kB
                          Buffers: shared hit=1560
                          ->  Bitmap Heap Scan on match_players mp (actual time=0.004..0.011 rows=10.00 loops=120)
                                Recheck Cond: (match_id = m.id)
                                Heap Blocks: exact=1200
                                Buffers: shared hit=1560
                                ->  Bitmap Index Scan on match_players_pkey (actual time=0.003..0.003 rows=10.00 loops=120)
                                      Index Cond: (match_id = m.id)
                                      Index Searches: 120
                                      Buffers: shared hit=360
Planning:
  Buffers: shared hit=9
Planning Time: 0.305 ms
Execution Time: 12.020 ms
```

#### History limit-first, schema v4
```text
Nested Loop (actual time=0.076..0.611 rows=20.00 loops=1)
  Buffers: shared hit=343
  ->  Nested Loop (actual time=0.031..0.105 rows=20.00 loops=1)
        Buffers: shared hit=83
        ->  Limit (actual time=0.017..0.034 rows=20.00 loops=1)
              Buffers: shared hit=23
              ->  Index Scan using match_players_player_started_idx on match_players (actual time=0.016..0.031 rows=20.00 loops=1)
                    Index Cond: (player_id = '1234'::bigint)
                    Index Searches: 1
                    Buffers: shared hit=23
        ->  Append (actual time=0.003..0.003 rows=1.00 loops=20)
              Buffers: shared hit=60
              ->  Index Scan using matches_2026_09_pkey on matches_2026_09 m_1 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_10_pkey on matches_2026_10 m_2 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_11_pkey on matches_2026_11 m_3 (never executed)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 0
              ->  Index Scan using matches_2026_12_pkey on matches_2026_12 m_4 (actual time=0.003..0.003 rows=1.00 loops=20)
                    Index Cond: ((id = match_players.match_id) AND (started_at = match_players.started_at))
                    Index Searches: 20
                    Buffers: shared hit=60
              ->  Seq Scan on matches_default m_5 (never executed)
                    Filter: ((id = match_players.match_id) AND (started_at = match_players.started_at))
  ->  Aggregate (actual time=0.025..0.025 rows=1.00 loops=20)
        Buffers: shared hit=260
        ->  Sort (actual time=0.015..0.016 rows=10.00 loops=20)
              Sort Key: mp.score DESC, mp.player_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=260
              ->  Bitmap Heap Scan on match_players mp (actual time=0.005..0.013 rows=10.00 loops=20)
                    Recheck Cond: (match_id = m.id)
                    Heap Blocks: exact=200
                    Buffers: shared hit=260
                    ->  Bitmap Index Scan on match_players_pkey (actual time=0.003..0.003 rows=10.00 loops=20)
                          Index Cond: (match_id = m.id)
                          Index Searches: 20
                          Buffers: shared hit=60
Planning:
  Buffers: shared hit=1
Planning Time: 0.319 ms
Execution Time: 0.711 ms
```

#### Partition pruning: one month of matches
```text
Aggregate (actual time=1.469..1.469 rows=1.00 loops=1)
  Buffers: shared hit=111
  ->  Seq Scan on matches_2026_11 matches (actual time=0.007..0.914 rows=14983.00 loops=1)
        Filter: ((started_at >= '2026-11-01 00:00:00+00'::timestamp with time zone) AND (started_at < '2026-12-01 00:00:00+00'::timestamp with time zone))
        Buffers: shared hit=111
Planning:
  Buffers: shared hit=20
Planning Time: 0.178 ms
Execution Time: 1.479 ms
```
