# Cleaning log

Input: `data/valid.csv` (203 rows, already validated by Pandera).
Output: `data/clean.csv` (203 rows, 18 columns).
Script: `src/cleaner.py`.

| # | Operation | Why it is needed | Result |
|---|---|---|---|
| 0 | Convert numeric columns to `float` | The data passes through Kafka and CSV as text. Median and NumPy need real numbers. | 5 columns converted |
| 1 | Missing values: fill `miss_distance_km` with the median, keep flag `miss_distance_filled` | Median is robust to outliers (distances are highly skewed). The flag keeps the fact of imputation visible. | 0 missing values, nothing filled |
| 2 | Remove duplicates by `record_id` | `record_id` is the primary key: one row per asteroid per approach date. | 0 duplicates removed |
| 3 | Convert `is_hazardous`, `is_sentry` to `bool`; `close_approach_date` to `datetime`; `neo_id` to string | SQLite and analysis need real types. `map()` is used instead of `astype(bool)`, because the text "False" would become `True`. | Types fixed |
| 4 | Text normalization of `name` and `orbiting_body` | "(2004 XK14)" becomes "2004 XK14": brackets removed, spaces collapsed, upper case. One format for grouping and joins. | 203 names normalized |
| 5 | NumPy features: `diameter_avg_km`, `log_velocity`, `miss_distance_lunar`, `size_class` | Average diameter summarizes the estimate range; log reduces skew; lunar distance is easier to read than km; size classes allow grouping. | 4 new columns |
| 6 | Multi-condition filter: diameter > 0.3 km, velocity > 50 000 km/h, distance < 20 lunar | Finds large, fast and close asteroids. Used for analysis only, no rows are removed from `clean.csv`. | 1 row matches |
| 7 | GroupBy `close_approach_date` with aggregation (count, mean velocity, max diameter, hazardous count) | Shows how many approaches and how fast per day. Saved to `data/agg_by_date.csv`. | One row per date |
| 8 | `pivot_table`: `size_class` x `is_hazardous` (count, mean velocity) | Compares size and hazard status in one table. Saved to `data/pivot_size_hazard.csv`. | Small asteroids: 0 hazardous |

## Row count through cleaning

| Step | Rows |
|---|---|
| Input (valid) | 203 |
| After removing missing / duplicates | 203 |
| Output (`clean.csv`) | 203 |

Cleaning did not remove any rows because validation (done before cleaning) had already separated the 3 injected invalid records. The main effect of cleaning is correct types and new derived columns.

## Note

In the pivot table the cell `small` + hazardous shows mean velocity `0` because of `fill_value=0`. It means "no data" (there are no hazardous small asteroids), not "zero velocity".