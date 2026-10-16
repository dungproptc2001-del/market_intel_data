# market_intel_data

Vietnamese banking and market news with full text, and daily market rates (SBV central rate, Vietcombank FX, bank
deposit rates), as JSON Lines for ingestion. One file of each kind per day.

This repo is a one-way mirror written by an automated job. Nothing here is edited by hand, and anything that is not
listed below is removed by the next push.

## Layout

| Path | Content |
|---|---|
| `feed/news/mra-news-YYYY-MM-DD.jsonl` | news of day D, one article per line |
| `feed/rates/mra-rates-YYYY-MM-DD.jsonl` | rates of day D, one observation per line |
| `meta/` | JSON Schema of each file, the taxonomy behind `category` / `subcategory`, the data dictionary |
| `README.md` | this file |

Day D is the day of the data, not a calendar day: it runs from one morning cut to the next (about 04:40
Asia/Ho_Chi_Minh). The files of D are pushed on the morning of D+1.

- A day without news has no news file. A day on which no rate could be read has no rates file.
- Only the most recent days are kept (about 30). Older files are deleted from the repo; keep your own copy of what
  you ingested.

## History is kept

A day file that leaves the window is deleted with an ordinary commit, so a plain `git pull` keeps working. The
deleted files stay in the git history of this repo.

- Never commit to a clone of this repo.
- If the repo grows too large one day, the branch `main` will be squashed to a single commit and force-pushed, and
  this section will say so first. A plain `git pull` fails after that: sync by discarding the local state,
  `git fetch origin && git reset --hard origin/main`. In a Databricks Git folder, update the folder in the mode that
  discards local changes, or delete the folder and create it again.

## File rules

- UTF-8 without BOM, lines end with `\n`, Unicode NFC.
- One JSON object per line, never pretty-printed. Every field of the schema is present on every line (`null` or `[]`
  when there is no value).
- Line order: news by (`relevance` descending, `published_at` descending, `id`); rates by (`kind`, `bank_code`,
  `series_key`, `status`).
- A file is byte for byte the one the pipeline wrote; the mirror never edits content.

## Ingest: MERGE, never append

A day can be built again, and the new file replaces the old one under the same name. The same article can also come
back on a later day when its publisher updates it. Ingest with MERGE on these keys:

| File | Key | Which line wins |
|---|---|---|
| news | `id` | the greatest (`digest_date`, `updated_at`) |
| rates | (`snapshot_date`, `series_key`, `status`) | the line read last (overwrite) |

## Schema

- News: [meta/news-daily.schema.json](meta/news-daily.schema.json), `schema_version` 2.
- Rates: [meta/rates-daily.schema.json](meta/rates-daily.schema.json), `schema_version` 1.
- What each field means and how the records relate (for a knowledge graph or retrieval):
  [meta/dictionary.md](meta/dictionary.md). The same sentences are the `COMMENT` of each column below.
- The values of `category` and `subcategory`: [meta/taxonomy.json](meta/taxonomy.json).

A new field is added at the end of the DDL and is always present; `schema_version` does not change for that. A field
that changes type or meaning, or is dropped, raises the `schema_version` of that file.

### Spark DDL — news

```sql
schema_version INT COMMENT 'Version of the file layout; 2 is the layout described here. It rises only when a field changes type or meaning or is dropped. A new field does not change it.',
digest_date DATE COMMENT 'Day D this line belongs to, the same date as in the file name. Day D is a window between two morning cuts (about 04:40 Asia/Ho_Chi_Minh), not a calendar day.',
id STRING COMMENT 'Stable article id: mi- followed by 8 hex characters of a hash of the normalized article URL. An article keeps its id on every day it appears. Key for MERGE.',
title STRING COMMENT 'Headline as published, usually in Vietnamese.',
description STRING COMMENT 'Lead paragraph (sapo) from the feed, as published. null when the feed gave none.',
source_url STRING COMMENT 'URL of the article on the publisher site. For an item found through Google News whose publisher link could not be resolved, the Google News link.',
source_name STRING COMMENT 'Name of the publisher, e.g. CafeF, VnExpress.',
source_domain STRING COMMENT 'Domain of the publisher. For a Google News item, the domain named in the feed entry. null when unknown.',
language STRING COMMENT 'Language of the article. vi = Vietnamese; en = English.',
published_at TIMESTAMP COMMENT 'When the publisher says the article was published, with UTC offset. null when the feed gave no time.',
published_date DATE COMMENT 'Date of published_at in Vietnam time (UTC+7). null when published_at is null.',
collected_at TIMESTAMP COMMENT 'When the collector first saw the article.',
updated_at TIMESTAMP COMMENT 'Time of the stored version of the article used for this line. When one id appears in several files, the line with the greatest (digest_date, updated_at) is the current one.',
feeds ARRAY<STRING> COMMENT 'Ids of the feeds (RSS feeds or Google News queries) in which the article was seen. One article can come from several.',
tier INT COMMENT 'Reliability tier of the source, fixed in configuration and not judged per article. 1 = the body that issues the figures (regulator, official publisher); 2 = established news outlet; 3 = any other source.',
content_status STRING COMMENT 'Outcome of fetching the full text. ok = the full text is in content_md; failed = the page could not be fetched; no_text = the page was fetched but holds no article body; paywalled = the publisher is behind a paywall and was not fetched; unresolved = a Google News link that did not lead to the publisher page; skipped_budget = not fetched because the monthly fetch budget was reached; listed = headline only, never fetched by design.',
fetch_via STRING COMMENT 'How the full text was fetched. http = a plain HTTP request; firecrawl = a rendering service, used where plain HTTP cannot get the page. null when nothing was fetched.',
content_md STRING COMMENT 'Full text of the article as Markdown, exactly as the collector stored it. null unless content_status is ok.',
prefilter STRING COMMENT 'Result of the keyword filter that runs before any model. candidate = passed the filter; unrelated = out of scope by the keyword rules, kept in the file with category news_unrelated and not processed further.',
llm_use BOOLEAN COMMENT 'Whether the full text of this source may be given to the language model. When false the model saw only the title and the lead: summary is then the lead (summary_source sapo), and an analysis, when there is one, is written from that lead alone.',
cluster_id STRING COMMENT 'Copy cluster: the id of the earliest article among near-identical copies of one text (syndication). Equals id when the article has no copy.',
dedup_suspect ARRAY<STRING> COMMENT 'Ids of other articles the collector suspected of reporting the same event. A hint only; the decision is in event_key.',
enrichment STRING COMMENT 'How far the model layer processed this article. full = labelled and summarized; triage_only = labelled (category, topic, relevance) but not summarized; raw = not processed, every model field is empty.',
llm_run_id STRING COMMENT 'Id of the model run that produced the model fields, in the form llm-YYYY-MM-DD_HHMM. null when enrichment is raw.',
event_key STRING COMMENT 'Event id: ev- followed by 10 hex characters. Articles that report the same event (same actors, same action, same time) share one event_key. They mostly share a digest_date too, but an article can take the event_key of an article of an earlier day that reports the same event. null when enrichment is raw.',
representative BOOLEAN COMMENT 'true for exactly one article of each event_key, the one that stands for the event; false for the other articles of that event, which may sit in a later digest_date than the representative. Only a representative is scored: join on event_key across days to give the scores of an event to all its articles. When the first article of the event was never enriched, no article has true for that event_key. null when enrichment is raw.',
category ARRAY<STRING> COMMENT 'Categories from meta/taxonomy.json (macro, micro, retail, other), the primary one first; or news_unrelated alone for an article out of scope. Empty when enrichment is raw.',
subcategory ARRAY<STRING> COMMENT 'Subcategories from meta/taxonomy.json, each under one of the categories of the article, the primary one first. Empty for news_unrelated and when enrichment is raw.',
topic STRING COMMENT 'The event in a few plain Vietnamese words, at most 120 characters. null when the title says nothing usable or enrichment is raw.',
segment ARRAY<STRING> COMMENT 'Business segments of the bank the article concerns. RBG = retail banking: individuals and the retail business; CIBG = corporate and institutional banking: corporate clients and the credit portfolio. Can hold both, or be empty.',
relevance INT COMMENT 'How much the bank needs this article, judged by the model from the title and the lead. 3 = changes operating conditions or a key market now; 2 = clearly bears on the business or a client sector, worth reading in full; 1 = background; 0 = unrelated. null when enrichment is raw.',
summary ARRAY<STRING> COMMENT 'Facts the article states, one sentence per element, in Vietnamese. Nothing here is inferred. Empty when the article was not summarized.',
summary_source STRING COMMENT 'Where summary comes from. llm = written by the model from the full text; sapo = a lead written by the publisher, used when llm_use is false: the lead paragraph of the feed or, when the feed gave none, the meta description of the page, taken only when it is short enough to be a lead (600 characters) and never cut. null when summary is empty.',
entities ARRAY<STRUCT<name: STRING, type: STRING>> COMMENT 'Organizations, people, places and instruments the article names. Empty when the article was not summarized.',
metrics ARRAY<STRING> COMMENT 'Names of the indicators the article reports, in snake_case, e.g. deposit_rate, lending_rate, policy_rate, credit_growth. A free vocabulary, not a fixed list.',
figures ARRAY<STRUCT<metric: STRING, entity: STRING, value: DOUBLE, unit: STRING, period: STRING, as_of: DATE>> COMMENT 'Numbers quoted from the text of the article. They are what the article says, NOT official data; official rates are in the rates file.',
direction STRING COMMENT 'The main move the article reports. up = rising; down = falling; flat = unchanged; new = something new (a product, a rule, a deal); n/a = no direction applies. null when the article was not summarized.',
event_date DATE COMMENT 'Date the event happened or takes effect, as the article states it; not the publication date. null when the article does not say.',
analysis STRING COMMENT 'Short assessment written by the model for every representative event of relevance 2 or more that has a summary, longer for the day top events. An opinion, not a fact. Markdown without headings. null for every other article, and for an event whose model batch failed. Days before 2026-10-15 have one only for their top 15 events.',
sentiment_corporate DOUBLE COMMENT 'Model judgement, not data: direction of the effect of the event on the corporate and institutional banking business of the bank. From -1 (clear loss) through 0 (no direction) to 1 (clear gain). null when the article was not scored or the event does not touch this lens.',
impact_corporate INT COMMENT 'Model judgement, not data: size of the effect on the corporate and institutional banking business, in either direction. 1 = hardly touches it; 2 = indirect, slow or unclear; 3 = indirect through a clear channel; 4 = direct on a core sector or a major client group; 5 = direct and large. null exactly when sentiment_corporate is null.',
sentiment_general DOUBLE COMMENT 'Model judgement, not data: direction of the effect of the event on the SME, household and overall business of the bank. From -1 (clear loss) through 0 (no direction) to 1 (clear gain). null when the article was not scored or the event does not touch this lens.',
impact_general INT COMMENT 'Model judgement, not data: size of the effect on the SME, household and overall business, in either direction. 1 = hardly touches it; 2 = small or slow effect; 3 = effect through consumption, jobs or one sector; 4 = strong effect on households or on pricing; 5 = system-wide change for every customer. null exactly when sentiment_general is null.',
scoring_version INT COMMENT 'Version of the scoring rules in force when the article was scored; compare scores across versions with care. A value with both lenses null means the article was scored and found to touch neither lens. null when the article was not scored.'
```

- `enrichment` is `raw` for an article no model has read: the labels, `summary` and `analysis` are then empty.
- `summary` holds only what the article states. `analysis` is an assessment, not a fact.
- The five score fields are `null` for an article that was not scored. For each lens (`corporate`, `general`),
  `sentiment` and `impact` are both set or both `null`. A `scoring_version` with both lenses `null` means the article
  was scored and found to touch neither lens.

### Spark DDL — rates

```sql
schema_version INT COMMENT 'Version of the file layout; 1 is the layout described here. It rises only when a field changes type or meaning or is dropped.',
snapshot_date DATE COMMENT 'Day D of the snapshot in Vietnam time, the same date as in the file name.',
confirmed_at TIMESTAMP COMMENT 'Last time on snapshot_date the source page was read and showed this value.',
changed BOOLEAN COMMENT 'true when (value, as_of) differs from the latest valid line of the series before snapshot_date, or when the series is new. false when nothing changed, and always false on a rejected line.',
prev_value DOUBLE COMMENT 'Latest valid value of the series before snapshot_date; equals value when changed is false. null for a series seen for the first time.',
prev_as_of DATE COMMENT 'as_of of prev_value. null when prev_value is null.',
obs_id STRING COMMENT 'Id of the observation: 12 hex characters of a hash of (series_key, as_of, value). One observation is one value of one series as published.',
source_id STRING COMMENT 'Id of the page the value was read from: sbv_central_rate, vcb_fx, or deposit- followed by the bank code.',
series_key STRING COMMENT 'Stable id of the time series, e.g. usd_vnd_central, vcb_fx/USD/transfer, deposit/VCB/12/online. The key to follow one series across days.',
kind STRING COMMENT 'What the series measures. central_rate = the central USD/VND rate announced by the State Bank of Vietnam; fx_quote = an exchange rate quoted by a bank; deposit_rate = a deposit interest rate posted by a bank.',
bank_code STRING COMMENT 'Short code of the bank, e.g. VCB, ACB. null for the State Bank of Vietnam.',
currency STRING COMMENT 'ISO 4217 code. For central_rate and fx_quote, the foreign currency priced in VND; for deposit_rate, the currency of the deposit.',
side STRING COMMENT 'For fx_quote, which price of the bank. buy = the bank buys cash; transfer = the bank buys by transfer; sell = the bank sells. null for the other kinds.',
term_months DOUBLE COMMENT 'For deposit_rate, the term in months; 0 is a demand deposit, 0.25 is one week. null for the other kinds.',
channel STRING COMMENT 'For deposit_rate, where the deposit is made. counter = at a branch; online = in the app or on the website. null for the other kinds.',
value DOUBLE COMMENT 'The number, expressed in unit. null only on a rejected line whose number could not be read.',
unit STRING COMMENT 'Unit of value. VND = Vietnamese dong for one unit of currency; pct_per_year = percent per year.',
as_of DATE COMMENT 'Date the value applies from, as the source states it.',
as_of_inferred BOOLEAN COMMENT 'true when the source page states no date and as_of is the day the collector first read the value.',
doc_no STRING COMMENT 'Number of the official document behind the value, e.g. 414/TB-NHNN. null when there is none.',
issued DATE COMMENT 'Date the official document was issued. null when there is none.',
status STRING COMMENT 'Result of the checks on the reading. valid = passed the format and range checks; rejected = read from the page but failed a check, kept as a trace and not to be used as data.',
reject_reason STRING COMMENT 'Why a rejected line failed its check. null on a valid line.',
source_url STRING COMMENT 'URL of the page the value was read from.',
snapshot_ref STRING COMMENT 'Path of the stored copy of the source page in the private archive. A reference for audit only; the copy is not in this repository.',
first_seen TIMESTAMP COMMENT 'When the collector first read this value for the series.'
```

- Every series has a line every day, also when its value did not change (`changed` is then `false`).
- `prev_value` and `prev_as_of` are the latest `valid` value before day D, so with `changed: false` they can equal
  `value` and `as_of`.
- `snapshot_ref` is the path of the page snapshot in the private store. It is a reference only; the snapshot is not
  in this repo.

### Reading in Spark

```python
news = spark.read.schema(NEWS_DDL).option("mode", "FAILFAST").json("<git folder>/feed/news/")
rates = spark.read.schema(RATES_DDL).option("mode", "FAILFAST").json("<git folder>/feed/rates/")
# then MERGE with the keys above
```

## Content

Every number comes from the publisher's own page, and every article keeps the link to its original in `source_url`.
The text of an article belongs to its publisher. This repo exists for its owner's own ingestion and is not a news
service.
