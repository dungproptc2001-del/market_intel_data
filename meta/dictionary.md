# Data dictionary

What every field of the two daily files means, and how the records relate to each other. Written for whoever, person
or model, builds tables, a knowledge graph or retrieval on top of this feed.

The field tables and the taxonomy table below are generated from the JSON Schemas
([news-daily.schema.json](news-daily.schema.json), [rates-daily.schema.json](rates-daily.schema.json)) and from
[taxonomy.json](taxonomy.json). The same sentences are the `COMMENT` of each column in the Spark DDL of the
[README](../README.md). The other sections are written by hand.

## Three kinds of content

Keep them apart when you answer a question from this data.

| Kind | Where | How far to trust it |
|---|---|---|
| Collected | news fields from `schema_version` to `dedup_suspect`; the whole rates file | Written by deterministic code. News text is what the publisher published. A rate is read from the page of the body or bank that sets it. |
| Model output | news fields from `enrichment` to `analysis` | Written by a language model. `summary`, `entities`, `metrics`, `figures`, `direction` and `event_date` restate what the article says. `analysis` is an opinion. |
| Model judgement | the four `sentiment_*` / `impact_*` fields and `scoring_version` | A score of how the event bears on one bank. Not data, and not comparable across `scoring_version` without care. |

- The only official numbers are in the rates file. A number in `figures` is a quote from a news article.
- Text is in Vietnamese. Field names, enum values, category names and `metrics` are in English.
- Every field is present on every line. A field with no value is `null`, an empty list is `[]`.

## News fields

File `feed/news/mra-news-YYYY-MM-DD.jsonl`, one article per line. Key: `id`.

<!-- generated:news -->
| Field | Spark type | Null | Meaning |
|---|---|---|---|
| `schema_version` | `INT` | no | Version of the file layout; 2 is the layout described here. It rises only when a field changes type or meaning or is dropped. A new field does not change it. |
| `digest_date` | `DATE` | no | Day D this line belongs to, the same date as in the file name. Day D is a window between two morning cuts (about 04:40 Asia/Ho_Chi_Minh), not a calendar day. |
| `id` | `STRING` | no | Stable article id: mi- followed by 8 hex characters of a hash of the normalized article URL. An article keeps its id on every day it appears. Key for MERGE. |
| `title` | `STRING` | no | Headline as published, usually in Vietnamese. |
| `description` | `STRING` | yes | Lead paragraph (sapo) from the feed, as published. null when the feed gave none. |
| `source_url` | `STRING` | no | URL of the article on the publisher site. For an item found through Google News whose publisher link could not be resolved, the Google News link. |
| `source_name` | `STRING` | no | Name of the publisher, e.g. CafeF, VnExpress. |
| `source_domain` | `STRING` | yes | Domain of the publisher. For a Google News item, the domain named in the feed entry. null when unknown. |
| `language` | `STRING` | no | Language of the article. vi = Vietnamese; en = English. |
| `published_at` | `TIMESTAMP` | yes | When the publisher says the article was published, with UTC offset. null when the feed gave no time. |
| `published_date` | `DATE` | yes | Date of published_at in Vietnam time (UTC+7). null when published_at is null. |
| `collected_at` | `TIMESTAMP` | no | When the collector first saw the article. |
| `updated_at` | `TIMESTAMP` | no | Time of the stored version of the article used for this line. When one id appears in several files, the line with the greatest (digest_date, updated_at) is the current one. |
| `feeds` | `ARRAY<STRING>` | no | Ids of the feeds (RSS feeds or Google News queries) in which the article was seen. One article can come from several. |
| `tier` | `INT` | no | Reliability tier of the source, fixed in configuration and not judged per article. 1 = the body that issues the figures (regulator, official publisher); 2 = established news outlet; 3 = any other source. |
| `content_status` | `STRING` | no | Outcome of fetching the full text. ok = the full text is in content_md; failed = the page could not be fetched; no_text = the page was fetched but holds no article body; paywalled = the publisher is behind a paywall and was not fetched; unresolved = a Google News link that did not lead to the publisher page; skipped_budget = not fetched because the monthly fetch budget was reached; listed = headline only, never fetched by design. |
| `fetch_via` | `STRING` | yes | How the full text was fetched. http = a plain HTTP request; firecrawl = a rendering service, used where plain HTTP cannot get the page. null when nothing was fetched. |
| `content_md` | `STRING` | yes | Full text of the article as Markdown, exactly as the collector stored it. null unless content_status is ok. |
| `prefilter` | `STRING` | no | Result of the keyword filter that runs before any model. candidate = passed the filter; unrelated = out of scope by the keyword rules, kept in the file with category news_unrelated and not processed further. |
| `llm_use` | `BOOLEAN` | no | Whether the full text of this source may be given to the language model. When false the model saw only the title and the lead: summary is then the lead (summary_source sapo) and analysis is null. |
| `cluster_id` | `STRING` | no | Copy cluster: the id of the earliest article among near-identical copies of one text (syndication). Equals id when the article has no copy. |
| `dedup_suspect` | `ARRAY<STRING>` | no | Ids of other articles the collector suspected of reporting the same event. A hint only; the decision is in event_key. |
| `enrichment` | `STRING` | no | How far the model layer processed this article. full = labelled and summarized; triage_only = labelled (category, topic, relevance) but not summarized; raw = not processed, every model field is empty. |
| `llm_run_id` | `STRING` | yes | Id of the model run that produced the model fields, in the form llm-YYYY-MM-DD_HHMM. null when enrichment is raw. |
| `event_key` | `STRING` | yes | Event id: ev- followed by 10 hex characters. Articles of one digest_date that report the same event (same actors, same action, same time) share one event_key. null when enrichment is raw. |
| `representative` | `BOOLEAN` | yes | true for exactly one article of each event_key, the one that stands for the event; false for the other articles of that event. Only a representative is scored. null when enrichment is raw. |
| `category` | `ARRAY<STRING>` | no | Categories from meta/taxonomy.json (macro, micro, retail, other), the primary one first; or news_unrelated alone for an article out of scope. Empty when enrichment is raw. |
| `subcategory` | `ARRAY<STRING>` | no | Subcategories from meta/taxonomy.json, each under one of the categories of the article, the primary one first. Empty for news_unrelated and when enrichment is raw. |
| `topic` | `STRING` | yes | The event in a few plain Vietnamese words, at most 120 characters. null when the title says nothing usable or enrichment is raw. |
| `segment` | `ARRAY<STRING>` | no | Business segments of the bank the article concerns. RBG = retail banking: individuals and the retail business; CIBG = corporate and institutional banking: corporate clients and the credit portfolio. Can hold both, or be empty. |
| `relevance` | `INT` | yes | How much the bank needs this article, judged by the model from the title and the lead. 3 = changes operating conditions or a key market now; 2 = clearly bears on the business or a client sector, worth reading in full; 1 = background; 0 = unrelated. null when enrichment is raw. |
| `summary` | `ARRAY<STRING>` | no | Facts the article states, one sentence per element, in Vietnamese. Nothing here is inferred. Empty when the article was not summarized. |
| `summary_source` | `STRING` | yes | Where summary comes from. llm = written by the model from the full text; sapo = the lead paragraph of the publisher, used when llm_use is false. null when summary is empty. |
| `entities` | `ARRAY<STRUCT<name: STRING, type: STRING>>` | no | Organizations, people, places and instruments the article names. Empty when the article was not summarized. |
| `entities[].name` | `STRING` | no | The name as written in the article. |
| `entities[].type` | `STRING` | no | Kind of entity. bank = commercial bank; company = any other company; regulator = state body or regulator; person = an individual; place = country, province or city; index = market index; currency = currency or currency pair; commodity = commodity such as gold or oil; other = anything else. |
| `metrics` | `ARRAY<STRING>` | no | Names of the indicators the article reports, in snake_case, e.g. deposit_rate, lending_rate, policy_rate, credit_growth. A free vocabulary, not a fixed list. |
| `figures` | `ARRAY<STRUCT<metric: STRING, entity: STRING, value: DOUBLE, unit: STRING, period: STRING, as_of: DATE>>` | no | Numbers quoted from the text of the article. They are what the article says, NOT official data; official rates are in the rates file. |
| `figures[].metric` | `STRING` | no | Name of the indicator, in snake_case, as in metrics. |
| `figures[].entity` | `STRING` | yes | Who or what the number is about. null when it is about no one in particular. |
| `figures[].value` | `DOUBLE` | no | The number, expressed in unit. |
| `figures[].unit` | `STRING` | no | Unit of value, e.g. pct, VND, bn_VND, USD. |
| `figures[].period` | `STRING` | yes | Period the number covers, e.g. 2026-Q3. null when the article does not say. |
| `figures[].as_of` | `DATE` | yes | Date the number refers to. null when the article does not say. |
| `direction` | `STRING` | yes | The main move the article reports. up = rising; down = falling; flat = unchanged; new = something new (a product, a rule, a deal); n/a = no direction applies. null when the article was not summarized. |
| `event_date` | `DATE` | yes | Date the event happened or takes effect, as the article states it; not the publication date. null when the article does not say. |
| `analysis` | `STRING` | yes | Short assessment written by the model for the most important articles of the day. An opinion, not a fact. Markdown without headings. null for every other article. |
| `sentiment_corporate` | `DOUBLE` | yes | Model judgement, not data: direction of the effect of the event on the corporate and institutional banking business of the bank. From -1 (clear loss) through 0 (no direction) to 1 (clear gain). null when the article was not scored or the event does not touch this lens. |
| `impact_corporate` | `INT` | yes | Model judgement, not data: size of the effect on the corporate and institutional banking business, in either direction. 1 = hardly touches it; 2 = indirect, slow or unclear; 3 = indirect through a clear channel; 4 = direct on a core sector or a major client group; 5 = direct and large. null exactly when sentiment_corporate is null. |
| `sentiment_general` | `DOUBLE` | yes | Model judgement, not data: direction of the effect of the event on the SME, household and overall business of the bank. From -1 (clear loss) through 0 (no direction) to 1 (clear gain). null when the article was not scored or the event does not touch this lens. |
| `impact_general` | `INT` | yes | Model judgement, not data: size of the effect on the SME, household and overall business, in either direction. 1 = hardly touches it; 2 = small or slow effect; 3 = effect through consumption, jobs or one sector; 4 = strong effect on households or on pricing; 5 = system-wide change for every customer. null exactly when sentiment_general is null. |
| `scoring_version` | `INT` | yes | Version of the scoring rules in force when the article was scored; compare scores across versions with care. A value with both lenses null means the article was scored and found to touch neither lens. null when the article was not scored. |
<!-- /generated:news -->

## Rates fields

File `feed/rates/mra-rates-YYYY-MM-DD.jsonl`, one value of one series per line. Key: (`snapshot_date`,
`series_key`, `status`).

<!-- generated:rates -->
| Field | Spark type | Null | Meaning |
|---|---|---|---|
| `schema_version` | `INT` | no | Version of the file layout; 1 is the layout described here. It rises only when a field changes type or meaning or is dropped. |
| `snapshot_date` | `DATE` | no | Day D of the snapshot in Vietnam time, the same date as in the file name. |
| `confirmed_at` | `TIMESTAMP` | no | Last time on snapshot_date the source page was read and showed this value. |
| `changed` | `BOOLEAN` | no | true when (value, as_of) differs from the latest valid line of the series before snapshot_date, or when the series is new. false when nothing changed, and always false on a rejected line. |
| `prev_value` | `DOUBLE` | yes | Latest valid value of the series before snapshot_date; equals value when changed is false. null for a series seen for the first time. |
| `prev_as_of` | `DATE` | yes | as_of of prev_value. null when prev_value is null. |
| `obs_id` | `STRING` | no | Id of the observation: 12 hex characters of a hash of (series_key, as_of, value). One observation is one value of one series as published. |
| `source_id` | `STRING` | no | Id of the page the value was read from: sbv_central_rate, vcb_fx, or deposit- followed by the bank code. |
| `series_key` | `STRING` | no | Stable id of the time series, e.g. usd_vnd_central, vcb_fx/USD/transfer, deposit/VCB/12/online. The key to follow one series across days. |
| `kind` | `STRING` | no | What the series measures. central_rate = the central USD/VND rate announced by the State Bank of Vietnam; fx_quote = an exchange rate quoted by a bank; deposit_rate = a deposit interest rate posted by a bank. |
| `bank_code` | `STRING` | yes | Short code of the bank, e.g. VCB, ACB. null for the State Bank of Vietnam. |
| `currency` | `STRING` | yes | ISO 4217 code. For central_rate and fx_quote, the foreign currency priced in VND; for deposit_rate, the currency of the deposit. |
| `side` | `STRING` | yes | For fx_quote, which price of the bank. buy = the bank buys cash; transfer = the bank buys by transfer; sell = the bank sells. null for the other kinds. |
| `term_months` | `DOUBLE` | yes | For deposit_rate, the term in months; 0 is a demand deposit, 0.25 is one week. null for the other kinds. |
| `channel` | `STRING` | yes | For deposit_rate, where the deposit is made. counter = at a branch; online = in the app or on the website. null for the other kinds. |
| `value` | `DOUBLE` | yes | The number, expressed in unit. null only on a rejected line whose number could not be read. |
| `unit` | `STRING` | no | Unit of value. VND = Vietnamese dong for one unit of currency; pct_per_year = percent per year. |
| `as_of` | `DATE` | no | Date the value applies from, as the source states it. |
| `as_of_inferred` | `BOOLEAN` | no | true when the source page states no date and as_of is the day the collector first read the value. |
| `doc_no` | `STRING` | yes | Number of the official document behind the value, e.g. 414/TB-NHNN. null when there is none. |
| `issued` | `DATE` | yes | Date the official document was issued. null when there is none. |
| `status` | `STRING` | no | Result of the checks on the reading. valid = passed the format and range checks; rejected = read from the page but failed a check, kept as a trace and not to be used as data. |
| `reject_reason` | `STRING` | yes | Why a rejected line failed its check. null on a valid line. |
| `source_url` | `STRING` | no | URL of the page the value was read from. |
| `snapshot_ref` | `STRING` | no | Path of the stored copy of the source page in the private archive. A reference for audit only; the copy is not in this repository. |
| `first_seen` | `TIMESTAMP` | no | When the collector first read this value for the series. |
<!-- /generated:rates -->

## Taxonomy

The values of `category` and `subcategory`. Each subcategory belongs to exactly one category. The same content as
[taxonomy.json](taxonomy.json).

<!-- generated:taxonomy -->
- `macro`: Affects the whole economy and the bank's operating environment.
- `micro`: One sector only — CIBG clients and credit portfolio.
- `retail`: Individual customers and the retail banking business (RBG).
- `other`: Truly unrelated to finance or business; rarely worth collecting.
- `news_unrelated`: Out of scope for the bank; kept in the file, not summarized or scored.

| Category | Subcategory | Meaning |
|---|---|---|
| `macro` | `interest_rate` | SBV policy rates (refinancing, rediscount, OMO), deposit/lending rates, Fed/ECB/SBV monetary policy |
| `macro` | `oil_energy_price` | Brent/WTI, domestic fuel prices, OPEC+, fuel taxes and reserves |
| `macro` | `exchange_rate` | USD/VND (central, interbank, free market), SBV FX intervention, DXY |
| `macro` | `gold_commodities` | Global/domestic gold and the premium, steel, cement, farm commodities |
| `macro` | `gdp_growth` | Vietnam/regional GDP, stimulus, growth targets, ADB/WB/IMF/OECD forecasts |
| `macro` | `fdi_trade` | FDI into Vietnam, import/export, trade policy (US-China, EU rules) |
| `macro` | `geopolitical` | Conflicts, sanctions, embargoes, supply-chain risk |
| `macro` | `fiscal_policy` | Public investment, state budget, VAT/PIT/CIT/environmental tax, government bonds |
| `macro` | `household_consumption` | Retail sales, household income, labour market, consumer confidence, household debt |
| `micro` | `real_estate` | Developers, projects, property market and legal, RE bonds, developer debt restructuring |
| `micro` | `infrastructure` | Expressways, rail, airports, ports, contractors, construction materials, urban planning |
| `micro` | `banking_finance` | Other commercial banks (competitors), bank strategy and personnel, bank M&A, fintech, consumer finance |
| `micro` | `financial_services` | CIBG clients — securities firms, insurers, funds, asset managers, leasing, non-bank finance |
| `micro` | `entertainment_travel_hospitality` | Airlines, tourism, hotels, resorts, F&B, luxury retail |
| `micro` | `healthcare` | Private hospitals, pharma, medtech, health insurance and policy |
| `micro` | `capital_markets` | VN-Index and stocks, equity and corporate bond issuance, funds, M&A |
| `micro` | `diversified` | FMCG/retail, energy/utilities, manufacturing, tech/telecom, agriculture — when no sector above fits |
| `retail` | `deposits_casa` | CASA, deposit mobilisation, savings products and promotions (rate moves stay in interest_rate) |
| `retail` | `retail_lending` | Mortgage, auto, unsecured and consumer loans, household credit demand |
| `retail` | `cards_payments` | Card spending, QR/Napas, e-wallets, cashless and cross-border payments |
| `retail` | `wealth_bancassurance` | Bancassurance, retail funds/bonds, AUM, affluent and private banking |
| `retail` | `digital_banking` | Banking apps, eKYC/biometrics, digital channels, branch network, ecosystem partnerships |
| `retail` | `consumer_protection` | Scams and fraud targeting bank customers, SBV retail rules (cards, biometrics, deposit insurance) |
| `other` | `general_news` | Social, weather, crime, foreign news with no effect on Vietnam's economy |
| `other` | `fraud_scam` | Financial fraud not aimed at bank customers (crypto platforms, illegal funds), financial-law violations |
<!-- /generated:taxonomy -->

## Relations

For a graph. Nothing below adds data: every node and edge is read from the fields above.

### Nodes

| Node | Identity | Comes from |
|---|---|---|
| Article | `id` | one line of the news file; the current line is the one with the greatest (`digest_date`, `updated_at`) |
| Event | (`digest_date`, `event_key`) | the articles that share an `event_key` |
| Entity | (`name`, `type`) | elements of `entities` |
| Category, Subcategory | the name | `category`, `subcategory`, described in the taxonomy |
| Segment | `RBG`, `CIBG` | `segment` |
| Publisher | `source_domain`, or `source_name` when the domain is null | the news file |
| Figure | (article `id`, position in `figures`) | elements of `figures` |
| Bank | `bank_code` | the rates file |
| Series | `series_key` | the rates file |
| Observation | `obs_id` | one valid line of the rates file |

### Edges

| Edge | Read from | Notes |
|---|---|---|
| Article REPORTS Event | `event_key` | Several articles can report one event. Exactly one of them has `representative` true. |
| Event REPRESENTED_BY Article | `representative` | Take the summary, the labels and the scores of an event from this article. To count events, count the articles with `representative` true; counting every article counts an event several times. |
| Article COPY_OF Article | `cluster_id` | Near-identical text republished by another outlet. `cluster_id` is the `id` of the earliest copy. This is about text, while `event_key` is about what happened. |
| Article SUSPECTED_SAME_AS Article | `dedup_suspect` | A hint from the collector, before any model. Not a decision; do not merge on it. |
| Article MENTIONS Entity | `entities` | Names are as written in the article and are not normalized: one bank can appear under its full name and under its ticker. Normalize before merging nodes. |
| Article IN Subcategory, Subcategory UNDER Category | `subcategory`, `category`, the taxonomy | Multi-label, the primary one first. `news_unrelated` stands alone and marks an article out of scope. |
| Article CONCERNS Segment | `segment` | |
| Article PUBLISHED_BY Publisher | `source_domain`, `source_name`, `tier` | `tier` is a property of the publisher, set in configuration. |
| Article QUOTES Figure | `figures` | A figure is (`metric`, `entity`, `value`, `unit`, `period`, `as_of`) as the article states it. `entity` is free text and may match the `name` of an Entity of the same article. |
| Article REPORTS_ON metric | `metrics` | A free vocabulary in snake_case. |
| Bank POSTS Series | `bank_code`, `series_key` | `bank_code` is null for the State Bank of Vietnam (`kind` central_rate). |
| Series HAS Observation on a day | `series_key`, `snapshot_date`, `value`, `as_of` | One valid line per series per day while the source can be read. `changed` marks the days the value moved; `prev_value` and `prev_as_of` give the value before. A missing day means the page could not be read, not that the value was zero. |

### Between news and rates

No field links an article to a rate series. The two files meet only through meaning, so treat every join below as
an inference of your own, not as a key of this feed:

- an Entity of `type` bank and a `bank_code` name the same bank only after you map names to codes;
- a `metrics` name such as `deposit_rate` and a `kind` of the rates file can name the same indicator;
- `event_date`, `published_date` or `digest_date` of an article and `snapshot_date` or `as_of` of a rate place both
  on one time line.

When an article and the rates file disagree about a number, the rates file is the one read from the source.

### Time

- `digest_date` and `snapshot_date` are day D of the file. For news, D is a window between two morning cuts (about
  04:40 Asia/Ho_Chi_Minh), so an article published before the cut on the morning of D+1 is still in the file of D.
- `published_at` is when the publisher released the article, `event_date` when the event happened or takes effect,
  `collected_at` when the collector first saw the article.
- An article can be in the files of more than one day. Its `id` stays the same, and the line with the greatest
  (`digest_date`, `updated_at`) is the current one.
- For a rate, `as_of` is the date the value applies from, `confirmed_at` the last time on day D the source showed
  it, `first_seen` the first time the collector read it.
