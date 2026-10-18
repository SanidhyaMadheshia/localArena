| strategy | buyers | stock | sold | out_of_stock | other errors | final stock | wall ms | deadlock retries | optimistic retries |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| naive | 200 | 20 | 200 | 0 | 0 | 18 | 1244 | 0 | 0 |
| pessimistic-forupdate | 200 | 20 | 0 | 0 | 200 | 20 | 59787 | 344 | 0 |
| pessimistic | 200 | 20 | 20 | 180 | 0 | 0 | 984 | 0 | 0 |
| optimistic | 200 | 20 | 20 | 180 | 0 | 0 | 832 | 0 | 206 |
| conditional | 200 | 20 | 20 | 180 | 0 | 0 | 129 | 0 | 0 |
