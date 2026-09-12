# Source Profiling Report

## 1. Source Inventory

| Source | Type | Rows/Records | Key | Update Pattern | Quality Findings |
|---|---|---:|---|---|---|
| customers.csv | CSV | 250 | customer_id (**not unique** — see below) | Batch drop (cadence TBD) | 2 exact duplicate rows; 1 true key collision (C0090); 3 missing email; 2 missing city |
| orders.json | JSON | 250 | order_id (unique, verified) | Cadence TBD (no update-time field observed) | Clean — no missing fields, no duplicate order_id, no negative amounts, subtotal+shipping_fee reconciles exactly to total_amount |
| products.parquet | Parquet | 200 | product_id (unique, verified) | Daily snapshot (assumed — TBD) | No missing values in any column; product_id verified unique; no negative unit_price or stock_quantity |
| Local REST API (events) | Paginated REST API | 122 raw / 120 unique | event_id (2 deliberate duplicates, verified) | Near-real-time, watermark-based | 2 event_ids repeat with a newer updated_at (E0020, E0055) — by design; 0 missing fields; 1 orphaned customer_id (C0222, no match in customers.csv) |
| support_tickets (PostgreSQL) | Relational DB table | 250 | ticket_id (unique, verified) | Continuous (OLTP) | 4 missing assigned_agent; 125 missing resolved_at (matches Open+In Progress count exactly — consistent); 2 orphaned customer_id values (C0222, C0223) |

## 2. Schema Findings

### customers.csv
| Field | Logical Type | Nullable | Key Role |
|---|---|---|---|
| customer_id | string | No | Candidate key (violated — see Data Quality) |
| first_name | string | No | — |
| last_name | string | No | — |
| email | string | Yes (3/250 missing) | — |
| city | string | Yes (2/250 missing) | — |
| signup_date | date (arrives as text, YYYY-MM-DD) | No | — |
| customer_segment | string, categorical (SME/Retail/Professional/Student) | No | — |

### products.parquet
| Field | Logical Type (Parquet dtype) | Nullable |
|---|---|---|
| product_id | string | No (0 missing observed) |
| product_name | string | No |
| category | string | No |
| brand | string | No |
| unit_price | float64 | No |
| stock_quantity | int32 | No |
| weight_kg | float64 | No |

### orders.json
| Field | Logical Type | Nullable | Key Role |
|---|---|---|---|
| order_id | string | No | Candidate key — verified unique (250/250) |
| customer_id | string | No | Foreign key to customers.csv |
| order_timestamp | timestamp (arrives as ISO text) | No | — |
| status | string, categorical (Pending/Packed/Shipped/Delivered/Cancelled/Paid) | No | — |
| item_count | numeric (int) | No | — |
| subtotal | numeric (float) | No | — |
| shipping_fee | numeric (float) | No | — |
| total_amount | numeric (float) | No | — |
| shipping | nested object (`method`, `region`), always present with both sub-keys populated | No | — |

Two representation options for `shipping` downstream: (a) keep it as a nested JSON/struct column if the target store supports it (e.g. a JSON column in Postgres, or a struct type in Parquet), preserving the relationship without a join; or (b) flatten it into `shipping_method` and `shipping_region` columns at the next lifecycle stage, trading nesting for simpler flat-table queries. Either is reasonable — this raw stage keeps it nested, since flattening is a transformation and the raw area preserves source structure as-is.

### Local REST API (events)
| Field | Logical Type | Nullable | Key Role |
|---|---|---|---|
| event_id | string | No | Candidate key — 120 of 122 raw records unique; 2 IDs (E0020, E0055) deliberately repeat with a newer updated_at, by lab design |
| customer_id | string | No | Foreign key to customers.csv (1 orphaned value: C0222) |
| event_type | string, categorical (page_view/add_to_cart/checkout/payment/support) | No | — |
| amount | numeric (float), range 1.86-4953.77, no negatives | No | — |
| updated_at | timestamp (arrives as ISO text), range 2026-08-01 to 2026-08-21 | No | Watermark field |
| metadata | nested object (`channel`, `campaign`), always present with both sub-keys populated | No | — |

Pagination envelope (from the server, not the record itself): `page`, `per_page`, `total`, `has_more`, `next_page`, `items`.

### support_tickets (PostgreSQL)
| Field | Logical Type | Nullable | Key Role |
|---|---|---|---|
| ticket_id | integer | No | Primary key — verified unique (250/250) |
| customer_id | string (VARCHAR(10)) | No | Foreign key to customers.csv (2 orphaned values: C0222, C0223) |
| category | string, categorical (Account/Product/Delivery/Billing/Technical) | No | — |
| priority | string, categorical (Low/High/Medium) | No | — |
| assigned_agent | string | Yes (4/250 missing) | — |
| opened_at | timestamp | No | — |
| resolved_at | timestamp | Yes (125/250 missing) | — |
| status | string, categorical (In Progress/Resolved/Closed/Open) | No | — |

## 3. Data Quality Findings

- **customers.csv:** `customer_id` uniqueness is violated — 247 of 250 values unique. Two IDs (C0036, C0145) are harmless exact-duplicate rows. One ID (C0090) is a genuine collision: two different customers share it with conflicting `first_name`/`last_name`/`city`/`customer_segment` values, and there is no timestamp column to break the tie. 3 records (1.2%) are missing `email`; 2 (0.8%) are missing `city`. `signup_date` is consistently `YYYY-MM-DD`; `customer_segment` has exactly 4 clean categorical values with no typos.
- **products.parquet:** No missing values in any of the 200 rows. Parquet preserves numeric types natively (`float64`, `int32`) — unlike the CSV source, which loses this and reads everything as string. `product_id` is verified unique (200/200); no negative `unit_price` or `stock_quantity`; `category` has 6 clean values with no typos.
- **orders.json:** Clean. 250/250 unique `order_id` values, zero exact-duplicate rows, zero missing top-level fields, zero missing values within the nested `shipping` object, no negative `item_count`/`subtotal`/`shipping_fee`/`total_amount`, and `subtotal + shipping_fee` reconciles exactly to `total_amount` on every record. `status` has exactly 6 clean categorical values with no typos. `order_timestamp` ranges 2026-01-02 to 2026-06-30 with no parsing failures. This is the cleanest of the three file sources profiled so far.
- **Local REST API (events):** 122 raw records, 120 unique `event_id` values. Two IDs (`E0020`, `E0055`) deliberately repeat with a newer `updated_at`, matching the lab's stated design — verified end-to-end against the real server, including correctly keeping the newer-`updated_at` version of each. No missing fields anywhere, no negative `amount` values (range 1.86-4953.77). One referential-integrity issue: `customer_id` C0222 appears in the events but has no matching row in `customers.csv`.
- **support_tickets:** 250 rows, `ticket_id` verified unique. `assigned_agent` is missing on 4 tickets; `resolved_at` is missing on 125 — but this isn't a data-quality problem, it's expected business logic: every `Open`/`In Progress` ticket (56 + 69 = 125) has no `resolved_at`, and every `Closed`/`Resolved` ticket has one, with zero cases of `resolved_at` preceding `opened_at`. Two `customer_id` values (`C0222`, `C0223`) don't match any row in `customers.csv`.
- **Cross-source finding:** `C0222` is orphaned in *both* `support_tickets` and the API events — the same non-existent customer is referenced from two independent sources, not a one-off glitch in either. This is worth raising with whoever owns `customers.csv`, since it suggests that file may be missing records rather than the other two sources containing bad references.

## 4. Recommended Acquisition Method

- **customers.csv, orders.json, products.parquet:** Full file ingestion (copy + SHA-256 content-hash manifest), not incremental. None of the three has a reliable last-modified column to support safe incremental extraction, and `customers.csv`'s key collision reinforces why an incremental merge would be risky here — there's no trustworthy tiebreaker to decide which version of a record is current.
- **Local REST API (events):** Incremental paginated GET using an `updated_after` watermark. Deduplicate by `event_id`, keeping the record with the greatest `updated_at`. Advance the watermark only after a successful raw write.
- **support_tickets (PostgreSQL):** Inspection only for this lab. A future extraction would need a timestamp- or CDC-based strategy (see Task 2.5) — pulling the full table repeatedly against a live OLTP system risks degrading it.

## 5. Risks and Assumptions

- `customer_id` in `customers.csv` is not reliably unique in the current extract; any downstream join or dedup logic must not assume a 1:1 mapping without additional reconciliation at the source.
- All five sources in this report have now been profiled against real data — none remain placeholders.
- Two customer references (`C0222` in both `support_tickets` and the API events; `C0223` in `support_tickets` only) do not exist in `customers.csv`. Any downstream join to enrich tickets or events with customer attributes will silently drop these records unless this is resolved at the source.
- The API watermark strategy assumes `updated_at` values are trustworthy; if the source ever produces multiple records sharing the exact same `updated_at`, ties could be split unpredictably across runs (see the Task 2.5 watermark discussion).
- No credentials or environment-specific values are hard-coded anywhere in this report or the pipeline; PostgreSQL access assumes local Docker Compose credentials only, scoped to this lab environment.