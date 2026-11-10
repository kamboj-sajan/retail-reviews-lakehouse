# Observed schema

This is what the source files actually contain, read from the files themselves
rather than from the dataset's documentation.

How it was produced:

```
python3 scripts/sample_raw.py     # first 20,000 reviews and 5,000 products per category
make schema                       # prints the tables below
```

The sample is the head of each file, not a random sample. A type that only
appears deeper in a file would not show up here, so the Spark jobs declare
their schema explicitly and the data quality checks (phase 5) test the full
data.

## What this means for the pipeline

- **Reviews have no category field.** The category exists only in the file
  name, so the raw S3 key carries it as `category=<name>/` and Spark reads it
  from the path.
- **`timestamp` is Unix time in milliseconds** (13 digits), stored as a JSON
  integer. It must be divided by 1000 before converting to a timestamp.
- **`rating` is a JSON float** (`5.0`), although every value in the sample is a
  whole number from 1 to 5.
- **`asin` and `parent_asin` differ in 33% to 47% of sampled review rows**,
  depending on the category. `parent_asin` is the product; `asin` is one variant of it (a colour or size).
  Product metadata is keyed by `parent_asin` only, so that is the join key.
- **Exact repeats of (`user_id`, `asin`, `timestamp`) exist** in the review
  files, so de-duplication in phase 2 has real work to do.
- **`details` in the metadata is an object with hundreds of different keys.**
  Letting Spark infer a schema would turn each key into its own column. It is
  read as a map or a JSON string instead.
- **`price` is null for 42% to 57% of the sampled products**, and
  `main_category` is sometimes null and does not always match the file's
  category (the Electronics file contains "All Electronics", for example).
- `author` and `subtitle` are missing from almost every metadata row;
  `bought_together` is null in every sampled row.

## Field tables

### Electronics.jsonl.gz

Rows read: 20000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `asin` | string (20000) | 0 |
| `helpful_vote` | int (20000) | 0 |
| `images` | array[empty] (18370), array[object] (1630) | 0 |
| `parent_asin` | string (20000) | 0 |
| `rating` | float (20000) | 0 |
| `text` | string (20000) | 0 |
| `timestamp` | int (20000) | 0 |
| `title` | string (20000) | 0 |
| `user_id` | string (20000) | 0 |
| `verified_purchase` | bool (20000) | 0 |

- Rating values: 1.0: 1458, 2.0: 740, 3.0: 1307, 4.0: 2848, 5.0: 13647.
- `timestamp` range: 997489611000 to 1679106908237 (as dates: 2001-08-11 to 2023-03-18).
- Rows that repeat an earlier (user_id, asin, timestamp): 0.
- Rows where `asin` differs from `parent_asin`: 9016.

### Toys_and_Games.jsonl.gz

Rows read: 20000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `asin` | string (20000) | 0 |
| `helpful_vote` | int (20000) | 0 |
| `images` | array[empty] (17500), array[object] (2500) | 0 |
| `parent_asin` | string (20000) | 0 |
| `rating` | float (20000) | 0 |
| `text` | string (20000) | 0 |
| `timestamp` | int (20000) | 0 |
| `title` | string (20000) | 0 |
| `user_id` | string (20000) | 0 |
| `verified_purchase` | bool (20000) | 0 |

- Rating values: 1.0: 687, 2.0: 496, 3.0: 1097, 4.0: 2768, 5.0: 14952.
- `timestamp` range: 1012519228000 to 1679006119995 (as dates: 2002-01-31 to 2023-03-16).
- Rows that repeat an earlier (user_id, asin, timestamp): 1.
- Rows where `asin` differs from `parent_asin`: 6524.

### Video_Games.jsonl.gz

Rows read: 20000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `asin` | string (20000) | 0 |
| `helpful_vote` | int (20000) | 0 |
| `images` | array[empty] (18946), array[object] (1054) | 0 |
| `parent_asin` | string (20000) | 0 |
| `rating` | float (20000) | 0 |
| `text` | string (20000) | 0 |
| `timestamp` | int (20000) | 0 |
| `title` | string (20000) | 0 |
| `user_id` | string (20000) | 0 |
| `verified_purchase` | bool (20000) | 0 |

- Rating values: 1.0: 1607, 2.0: 925, 3.0: 1518, 4.0: 3041, 5.0: 12909.
- `timestamp` range: 944610001000 to 1679131878878 (as dates: 1999-12-07 to 2023-03-18).
- Rows that repeat an earlier (user_id, asin, timestamp): 13.
- Rows where `asin` differs from `parent_asin`: 9336.

### meta_Electronics.jsonl.gz

Rows read: 5000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `author` | null (2), object (2) | 4996 |
| `average_rating` | float (5000) | 0 |
| `bought_together` | null (5000) | 0 |
| `categories` | array[string] (4638), array[empty] (362) | 0 |
| `description` | array[string] (2840), array[empty] (2160) | 0 |
| `details` | object (5000) | 0 |
| `features` | array[string] (3925), array[empty] (1075) | 0 |
| `images` | array[object] (5000) | 0 |
| `main_category` | string (4947), null (53) | 0 |
| `parent_asin` | string (5000) | 0 |
| `price` | null (2862), float (2138) | 0 |
| `rating_number` | int (5000) | 0 |
| `store` | string (4965), null (35) | 0 |
| `subtitle` | string (3), null (1) | 4996 |
| `title` | string (5000) | 0 |
| `videos` | array[empty] (2881), array[object] (2119) | 0 |

- `author` is a nested object with 3 distinct keys across the sample (first few: about, avatar, name).
- `details` is a nested object with 584 distinct keys across the sample (first few: Accessory Connection Type, Actuator Type, Age Range (Description), Age Range Description, Air Flow Capacity, Alarm, Alarm Clock, Alert Type).

### meta_Toys_and_Games.jsonl.gz

Rows read: 5000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `author` | object (1) | 4999 |
| `average_rating` | float (5000) | 0 |
| `bought_together` | null (5000) | 0 |
| `categories` | array[string] (4570), array[empty] (430) | 0 |
| `description` | array[string] (3346), array[empty] (1654) | 0 |
| `details` | object (5000) | 0 |
| `features` | array[string] (4192), array[empty] (808) | 0 |
| `images` | array[object] (5000) | 0 |
| `main_category` | string (4957), null (43) | 0 |
| `parent_asin` | string (5000) | 0 |
| `price` | float (2915), null (2085) | 0 |
| `rating_number` | int (5000) | 0 |
| `store` | string (4978), null (22) | 0 |
| `subtitle` | string (1) | 4999 |
| `title` | string (5000) | 0 |
| `videos` | array[object] (2720), array[empty] (2280) | 0 |

- `author` is a nested object with 3 distinct keys across the sample (first few: about, avatar, name).
- `details` is a nested object with 395 distinct keys across the sample (first few: Additional product features, Age Range (Description), Age Range Description, Alarm Clock, Amperage, Answering System Type, Are Batteries Included, Assembled Height).

### meta_Video_Games.jsonl.gz

Rows read: 5000. Lines that were not valid JSON: 0.

| Field | Types seen (count) | Missing |
|---|---|---|
| `author` | object (3), null (1) | 4996 |
| `average_rating` | float (5000) | 0 |
| `bought_together` | null (5000) | 0 |
| `categories` | array[string] (4588), array[empty] (412) | 0 |
| `description` | array[string] (3260), array[empty] (1740) | 0 |
| `details` | object (5000) | 0 |
| `features` | array[string] (3810), array[empty] (1190) | 0 |
| `images` | array[object] (5000) | 0 |
| `main_category` | string (4982), null (18) | 0 |
| `parent_asin` | string (5000) | 0 |
| `price` | float (2711), null (2289) | 0 |
| `rating_number` | int (5000) | 0 |
| `store` | string (4860), null (140) | 0 |
| `subtitle` | string (3), null (1) | 4996 |
| `title` | string (5000) | 0 |
| `videos` | array[empty] (3177), array[object] (1823) | 0 |

- `author` is a nested object with 3 distinct keys across the sample (first few: about, avatar, name).
- `details` is a nested object with 285 distinct keys across the sample (first few: Actors, Age Range (Description), Age Range Description, Amperage, Antenna, Are Batteries Included, Aspect Ratio, Assembly Required).

