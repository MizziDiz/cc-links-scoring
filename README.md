# cc-links — сбор и анализ ссылок из Common Crawl без Athena

## База URL-площадок (prospects)

`prospect_pipeline.py` собирает из Common Crawl не произвольный граф исходящих
ссылок, а классифицированную базу URL-кандидатов. Поиск выполняется в два этапа:

1. DuckDB отбирает из Parquet-индекса страницы по селективным URL-footprints,
   скомпилированным в одну векторизованную регулярку.
2. HTML извлекается из WARC и подтверждается по URL, `meta generator` и HTML-сигналам.

Таксономия и веса находятся в `cc_links/prospect_footprints.json`. Запись в
`candidates` содержит семейство площадки, платформу, score и JSON со всеми
совпавшими доказательствами. Исходящие ссылки для этого режима не извлекаются.

Небольшой тестовый discovery-запуск:

```
python prospect_pipeline.py --categories-file categories.json \
    --per-category-limit 100 --max-parts 2 --discovery-only
```

Полный запуск с CloudFront:

```
python prospect_pipeline.py --categories-file categories.json \
    --per-category-limit 10000 --db prospects.db --workers 20 \
    --rate-limit 15 --source cloudfront
```

На EC2 с доступом к `s3://commoncrawl` используйте `--source s3`. В этом режиме
`--index-source auto` направляет через S3 и DuckDB/Parquet discovery, и точечную
загрузку WARC. Для A/B-проверки backend можно принудительно выбрать через
`--index-source https` или `--index-source s3`. Для продолжения после discovery
передайте `--skip-discovery`; checkpoint по умолчанию хранится в
`<db>.prospects.jsonl` и `<db>.prospects.jsonl.state.json`.

Экспорт и отчёты:

```
python export_candidates.py --db prospects.db --family forum \
    --min-score 70 --format csv --out forum.csv
python analyze_candidates.py --db prospects.db --report families
```

### Amazon Linux + S3, фоновый запуск

EC2 instance role должен разрешать `s3:GetObject` для bucket `commoncrawl`.
Готовый unit `deploy/cc-prospects.service` запускает сборщик через systemd,
возобновляет его после ошибки и пишет обычную строку прогресса раз в минуту.

```
curl -fsSLO https://github.com/MizziDiz/cc-links-scoring/releases/download/prospects-v0.3.2/install-amazon-linux.sh
chmod +x install-amazon-linux.sh
./install-amazon-linux.sh
sudo journalctl -fu cc-prospects.service
```

`multi_crawl.py` последовательно обрабатывает свежие snapshots Common Crawl в
одну SQLite-базу до достижения `--target-total`. Каждый crawl получает отдельный
JSONL/state в `--state-dir`; `processed_urls` предотвращает повторную загрузку
одинаковых URL между snapshots и после рестартов.

```
python multi_crawl.py --target-total 100000 --max-crawls 12 \
    --discovery-shards 4 \
    --state-dir crawl_states --db prospects.db --source s3
```

`--discovery-shards 4` делит Parquet-части snapshot между четырьмя
непересекающимися процессами. После discovery их JSONL объединяются с
нормализацией URL, затем запускается единый быстрый WARC/scoring этап.
Индивидуальные гео-квоты задаются через `--category-limits`; при sharded
discovery они автоматически делятся между процессами, поэтому не умножаются
на число шардов.

Контролируемое расширение discovery:

```
python multi_crawl.py --target-total 100000 --max-crawls 12 \
    --discovery-profile broad \
    --broad-index-sample 0.02 --broad-quota-fraction 0.25 \
    --category-limits category_limits.small.json \
    --discovery-shards 4 --state-dir crawl_states-broad \
    --db prospects.db --source s3
```

Точные footprints всегда имеют приоритет. `--broad-index-sample 0.02`
детерминированно пропускает на ранжирование 2% слабых структурных совпадений,
а `--broad-quota-fraction 0.25` не позволяет им занять более 25% квоты
отдельного гео. Все такие URL затем проходят тот же WARC-классификатор и
`--min-score`; лимит `--max-per-domain 10` применяется ещё внутри SQL.

Стратифицированная выборка для ручной оценки качества:

```
python sample_candidates.py --db prospects.db \
    --per-family 50 --out quality_sample.csv

python validate_sample.py --input quality_sample.csv \
    --out quality_sample_validated.csv --workers 20
```

## Outreach URL pilot (опционально)

Новый режим `pipeline.py outreach` ищет в Common Crawl страницы-приглашения
для гостевых авторов. Он работает отдельно от `prospect_pipeline.py`: не
меняет таблицу `candidates`, её score, лимит 10 и текущий путь сбора.

Сначала постройте карту диапазонов `url_surtkey` для выбранного crawl:

```bash
python pipeline.py outreach partmap \
  --crawl CC-MAIN-2026-30 \
  --out data/ops/outreach/CC-MAIN-2026-30.partmap.json
```

На EC2 с instance profile используйте `--index-source s3`: статические AWS-ключи
не нужны. Для полного map по 300 частям команда по умолчанию пересоздаёт
DuckDB-соединение каждые 15 частей; интервал можно изменить через
`--reconnect-every`.

Карта строится для всех index parts один раз и позволяет целевому ccTLD-пилоту
не читать части, диапазоны которых точно не пересекаются с нужными TLD.
Неизвестные или некорректные диапазоны никогда не отбрасываются.

URL-only пилот на `co`, `mx`, `cl`:

```bash
python pipeline.py outreach discover \
  --crawl CC-MAIN-2026-30 \
  --tlds co mx cl \
  --part-map data/ops/outreach/CC-MAIN-2026-30.partmap.json \
  --max-parts 10 \
  --max-per-domain 2 \
  --out data/ops/outreach/pilot.jsonl \
  --db data/ops/outreach/pilot.db
```

`outreach discover` по умолчанию выводит источник из выбранных part URLs:
S3-карта включает S3-доступ через runtime identity, HTTPS-карта остаётся на
публичном endpoint. Источник можно зафиксировать через `--index-source`, а
ротация длинного прохода управляется `--reconnect-every`.

Discovery фильтрует колонку `url_path`, сохраняет отдельную outreach SQLite и
per-part JSONL-фрагменты. WARC и HTML на этом этапе не скачиваются. Готовый
фрагмент является recovery checkpoint: после сбоя он импортируется без
повторного чтения соответствующего Parquet.

Отчёт и стратифицированная ручная выборка:

```bash
python pipeline.py outreach report \
  --db data/ops/outreach/pilot.db \
  --out data/ops/outreach/pilot-report.json

python pipeline.py outreach sample \
  --db data/ops/outreach/pilot.db \
  --size 50 \
  --out data/ops/outreach/pilot-review.csv
```

После заполнения колонок `label` (`relevant`, `noise`, `uncertain`) и `reason`:

```bash
python pipeline.py outreach audit \
  --input data/ops/outreach/pilot-review.csv \
  --out data/ops/outreach/pilot-audit.json
```

Полный discovery не запускается, пока audit gate не подтвердит не менее 40
решительных оценок, общий шум ниже 20% и отсутствие паттерна с шумом выше 30%.
Для подробного журнала установите `LOG_LEVEL=DEBUG`.

Архитектура, инварианты и последующие этапы Web Graph/live qualification
описаны в `docs/outreach-pipeline-plan.md`.

### Full GET-only outreach qualification

Live qualification is an optional, resumable stage. It reads the discovery
database without modifying it, respects `robots.txt`, checks each domain with
per-domain concurrency of one, never submits forms or authenticates, and does
not retain HTML bodies. Results and checkpoints are written to a separate
SQLite database:

```bash
python pipeline.py outreach qualify \
  --db data/ops/outreach/outreach.db \
  --out-db data/ops/outreach/live-validation.db \
  --report data/ops/outreach/live-validation-report.json \
  --export-dir data/ops/outreach/live-exports \
  --workers 20
```

The exports separate `approved`, `review`, `rejected` and `unreachable`
domains. HTTP 403/429, uncertain `robots.txt` responses and transient server
errors are routed to review instead of being treated as proof that a site is
dead. Re-running the same command resumes from the page-level checkpoint.

HTML enrichment is a separate resumable stage. It reads every exact archived
WARC record, fetches current HTML only where the preceding robots check
allowed it, and inspects a bounded set of sitemaps. Full HTML is processed in
memory and discarded; the output contains only scoring features and bounded
text fields:

```bash
python pipeline.py outreach enrich \
  --db data/ops/outreach/outreach.db \
  --validation-db data/ops/outreach/live-validation.db \
  --out-db data/ops/outreach/html-enrichment.db \
  --report data/ops/outreach/html-enrichment-report.json \
  --export data/ops/outreach/scoring-features.csv \
  --fetch-source s3
```

The `scoring_features` view selects current HTML when available and otherwise
falls back to the Common Crawl snapshot. `best_lastmod` keeps its provenance:
an exact sitemap match has priority, followed by current HTML/HTTP metadata and
then archived HTML/HTTP metadata. Sitemap freshness remains a weighted signal,
not a standalone approval or rejection rule. Sitemap lookup is best-effort:
each bounded candidate document gets one request with an eight-second timeout,
so slow or broken sitemap hosts cannot stall the full enrichment run.

Combine discovery, live qualification, HTML content and freshness into
auditable component scores:

```bash
python pipeline.py outreach score \
  --db data/ops/outreach/outreach.db \
  --validation-db data/ops/outreach/live-validation.db \
  --enrichment-db data/ops/outreach/html-enrichment.db \
  --out-db data/ops/outreach/outreach-scores.db \
  --report data/ops/outreach/outreach-score-report.json \
  --export data/ops/outreach/outreach-scores.csv \
  --text-dir data/ops/outreach/score-bands \
  --profile v1
```

The combined score weights discovery 15%, current live qualification 40%,
content evidence 30% and freshness 15%. Missing dates are neutral. Archived
dates receive less confidence than current HTML/HTTP dates, while an exact URL
match in a sitemap receives full freshness confidence. Hard noise evidence and
failed live qualification cap the combined score; all component scores and
reason codes remain available for review.

`--profile v1` is the compatibility default. Optional `--profile v2` reduces
the saturated discovery component to 5%, gives current qualification/content
45%/35%, treats challenge markers as weak when a substantive editorial page is
present, and recognizes strong editorial title/H1 phrases in Spanish,
Portuguese, German and French. Broad collaboration wording alone is not
promoted because it often describes partnerships, volunteering or careers.

## Outreach economics and expected effectiveness

The optional value pipeline keeps three different questions separate:

1. **What is promised?** Exact archived HTML is read again by WARC range.
   Bounded evidence is retained for publication policy, guest/sponsored/link
   insertion type, advertised price, dofollow/nofollow policy, contextual or
   author-bio placement, permanence, turnaround and who writes the content.
   Closed submissions are explicitly downgraded. A number found only in broad
   placement context is marked `advertised_review` and is excluded from cost
   calculations until reviewed; it is never silently treated as a fee.
2. **How strong is the destination?** Provider exports are normalized into
   optional DR/DA/Authority Score, Trust Flow/Citation Flow, organic traffic,
   referring domains, spam and relevance fields. The project does not invent
   or scrape branded SEO metrics.
3. **What does a result cost?** Expected publication probability, placement
   quality, domain strength, contact labor, content production and advertised
   price remain visible as separate columns.

Extract publication terms without keeping full HTML bodies:

```bash
python pipeline.py outreach terms \
  --db data/ops/outreach/outreach.db \
  --out-db data/ops/outreach/placement-terms.db \
  --fetch-source s3 \
  --workers 24
```

Import a provider export. Accepted headers include `domain`, `provider`, `DR`,
`DA`, `Authority Score`, `Trust Flow`, `Citation Flow`, `Organic Traffic`,
`Referring Domains`, `Spam Score`, `Topical Relevance`, and `Geo Relevance`.
Missing metrics remain null and are never silently imputed.

```bash
python pipeline.py outreach metrics-template \
  --scores-db data/ops/outreach/outreach-scores-v2.db \
  --out data/ops/outreach/domain-metrics-template.csv

python pipeline.py outreach metrics \
  --input data/ops/outreach/domain-metrics.csv \
  --out-db data/ops/outreach/outreach-value.db
```

Calculate expected effectiveness and economics:

```bash
python pipeline.py outreach value \
  --scores-db data/ops/outreach/outreach-scores-v2.db \
  --terms-db data/ops/outreach/placement-terms.db \
  --out-db data/ops/outreach/outreach-value.db \
  --contact-cost 5 \
  --content-cost 40 \
  --base-currency USD \
  --fx data/ops/outreach/fx.csv \
  --report data/ops/outreach/value-report.json \
  --export data/ops/outreach/outreach-value.csv
```

`fx.csv` has `currency,rate_to_base`; currencies without an explicit rate are
not compared. Expected contact cost is `contact_cost / publication_probability`.
For pages advertising several one-time packages, comparable economics uses the
lowest explicit entry price; the full minimum/maximum range remains in the
terms export. Recurring monthly or annual plans are not treated as a one-off
placement fee.
Content cost is omitted only when the publisher explicitly promises to write
the article. Expected effectiveness multiplies domain strength, promised
publication probability, placement quality and current page qualification.
The value export also carries `best_lastmod`, its provenance and the freshness
components already calculated by the HTML/sitemap scoring stage.
The output exposes every component and reason code instead of presenting the
prediction as an observed SEO result.

Observed campaign results can be fed back with `outreach outcomes`. The CSV
supports `url`, `registered_domain`, `status`, `quoted_cost`, `actual_cost`,
`currency`, `published_url`, `contacted_at`, `published_at`, `live_30d`,
`live_90d`, and `notes`. Allowed statuses are `planned`, `contacted`, `replied`,
`quoted`, `accepted`, `published`, `rejected`, and `unreachable`. This funnel is
the calibration source for replacing initial promise-based probabilities with
empirical rates after enough outreach has actually been performed.

### Placement model and outbound-link graph (offline)

An optional second pass separates two materially different businesses:

- `external_service`: the discovered domain brokers placements elsewhere. Its
  own domain metrics may later support reliability/history, but are never used
  as the quality of a promised placement. Explicit examples and publisher
  inventory are linked to the service with evidence and confidence.
- `self_hosted`: the discovered publisher places content or links on its own
  registered domain. Existing page quality, freshness, promise and price can
  therefore produce a partial offline evaluation immediately.
- `hybrid` and `unknown` remain explicit review states.

The pass reads only the exact WARC fragments already selected by discovery. It
does not call an SEO API, submit forms or retain full HTML. It stores normalized
outbound URLs, registered destination domains, anchor, `rel`, bounded context,
DOM section, source lastmod, role and reason codes in a separate SQLite DB.

```bash
python pipeline.py outreach placements \
  --db data/ops/outreach/outreach.db \
  --terms-db data/ops/outreach/placement-terms.db \
  --scores-db data/ops/outreach/outreach-scores-v2.db \
  --out-db data/ops/outreach/placement-graph.db \
  --fetch-source s3 \
  --workers 24 \
  --report data/ops/outreach/placement-graph-report.json \
  --export-dir data/ops/outreach/placement-graph-exports
```

`placement_evaluations` deliberately leaves `service_reliability_score` null
without domain metrics. External services are `pending_placement_metrics`;
their own page score is not borrowed for third-party publishers. Self-hosted
rows are labelled `offline_partial_*`, because Common Crawl page evidence is
not a substitute for DR, traffic or flow.

The graph also creates empty outcome tables. A deterministic manual observation
sheet for every identified placement and 7/30/90/180-day window is generated
without any API:

```bash
python pipeline.py outreach impact-template \
  --db data/ops/outreach/placement-graph.db \
  --out data/ops/outreach/impact-observations.csv
```

Future before/after measurements belong to the target project and placement,
not to the service domain. `link_live`, indexation, rating, organic traffic,
keywords and referring domains are kept as timestamped observations with an
optional control label; the pipeline does not claim causality from a simple
before/after difference.

## Adaptive discovery: baseline и feedback

Воспроизводимый baseline перед изменением правил или порогов:

```
python multi_crawl.py ... --discovery-metrics
python baseline_report.py --db prospects.db \
    --state-dir crawl_states \
    --manifest crawl_states/CC-MAIN-2026-25.jsonl \
    --validation-csv quality_sample_validated.csv \
    --json-out baseline.json
```

`baseline_report.py` открывает SQLite только для чтения. Без
`--discovery-metrics` старые checkpoint-файлы тоже поддерживаются, но для них
доступен выход на завершённую Parquet-часть, а не на миллион строк индекса.

Обратная связь по discovery-паттернам строится после пилотного fetch:

```
python feedback_report.py --db pilot.db \
    --manifest crawl_states/CC-MAIN-2026-25.jsonl \
    --minimum-samples 20 \
    --output pattern-priorities.json

python multi_crawl.py --target-total 100000 \
    --discovery-profile broad \
    --pattern-priorities pattern-priorities.json \
    --state-dir crawl_states-feedback \
    --db prospects.db --source s3
```

`feedback_report.py` читает SQLite в read-only режиме и считает фактический
выход по `pattern_id × discovery_tier × bucket`: число решений, долю
`stored/domain_cap`, уникальные домены, средний score и retryable fetch-ошибки.
`--manifest` нужен только старым БД без attribution-колонок; новые запуски
сохраняют эти поля непосредственно в `processed_urls`. Если legacy-manifest
содержит tier, но ещё не содержит `pattern_id`, отчёт восстанавливает его из
URL текущей версией таксономии и не меняет исходные файлы.
Вес ограничен диапазоном ±15 и влияет только на порядок WARC-fetch. Он не
заменяет финальную HTML-классификацию, не понижает `--min-score` и не меняет
лимит `--max-per-domain 10`. Паттерны с выборкой меньше `--minimum-samples`
получают нейтральный вес, поэтому новые сигналы продолжают исследоваться.
Вес применяется при чтении fetch-очереди, поэтому готовый JSONL/checkpoint
можно переиспользовать без повторного Parquet-сканирования; уже обработанные
URL по-прежнему исключаются через SQLite. Для воспроизводимого A/B на одном
manifest доступен опциональный `--fetch-limit N`. Для legacy-manifest без
`pattern_id` очередь восстанавливает его из URL в памяти; JSONL на диске не
перезаписывается.

Внешнюю библиотеку GSA Engines можно использовать как источник кандидатов для
таксономии, не копируя её целиком в репозиторий:

```
python mine_engine_signatures.py --engines-dir /path/to/Engines \
    --out data/engine-signatures-review.json
```

Скрипт только формирует отчёт. Он не добавляет широкие search terms в Common
Crawl discovery автоматически: URL- и HTML-сигналы сначала проходят ручную
проверку и тесты на известных положительных/отрицательных примерах.

## Установка

```
pip install -r requirements.txt
```

## Сбор данных

### Режим 1: конкретные домены (через CDX Index API)

```
python pipeline.py domains --domains example.com another.org --crawl CC-MAIN-2026-25 --limit 50 --db links.db
```

- `--crawl` — id индекса Common Crawl (список актуальных: https://index.commoncrawl.org/collinfo.json)
- `--limit` — сколько страниц на домен забирать
- `--exclude-file` — свой JSON с доменами, которые нужно исключить дополнительно (см. ниже)

### Режим 2: поиск по странам (ccTLD) с приоритетами, без Athena

```
python pipeline.py countries --countries ru de fr --total-limit 300 \
    --priorities priorities.example.json --max-parts 40 --crawl CC-MAIN-2026-25 --db links.db
```

- `--countries` — список ccTLD (`ru`, `de`, `fr`, ...)
- `--priorities` — JSON вида `{"ru": 3, "de": 2, "fr": 1}` — соотношение приоритетов между странами (без файла — равный вес всем)
- `--total-limit` — общий бюджет страниц, который распределяется между странами пропорционально весам
- `--max-parts` — сколько частей колоночного индекса (parquet) сканировать. Индекс краула разбит на ~300 частей; чем больше `--max-parts`, тем полнее покрытие страны, но дольше и больше трафика. Части не идут по алфавиту доменов подряд (шардирование Spark), поэтому скрипт сэмплирует их равномерно по всему индексу, а не берёт только первые N.
- `--index-source` — источник Parquet для DuckDB: `https`, `s3` или `auto`. В режиме `auto` используется S3 вместе с `--source s3`, иначе HTTPS.
- `--no-links` — не сохранять отдельные исходящие ссылки в таблицу `links`, только их количество (`pages.outlink_count`). Для скоринга движков по странам сама таблица ссылок не нужна, а именно она отвечает за почти весь объём базы: ~100+ строк ссылок на страницу означает ~50+ ГБ на 1.4 млн страниц против пары сотен МБ без неё. Сами HTML-страницы на диск никогда не пишутся — они разбираются в памяти и сразу отбрасываются.
- `--proxy` / `--proxy-file` — маршрутизация запросов через прокси (одиночный rotating-gateway URL, либо файл со списком `host:port:user:pass` — тогда запросы идут по кругу по всему пулу). Троттлинг CloudFront у `data.commoncrawl.org` привязан к IP отправителя (~35-40 req/с — безопасный потолок на один IP, подтверждено эмпирически), так что пул прокси нужен, чтобы поднять `--rate-limit` выше этого потолка. Прокси должны поддерживать HTTPS CONNECT-туннель — обычный HTTP-проксирование не подойдёт, так как `data.commoncrawl.org` отдаёт данные только по HTTPS.
- `--rate-limit` — общий лимит запросов/сек по всем потокам (не на поток). При устойчивой серии сбоев подряд пайплайн сам снижает лимит вдвое и делает паузу 90с (защита от троттлинга).

Как это работает:
1. `cc_links/cdx.py` — запрос к CDX Index API (`index.commoncrawl.org`) для доменного режима: находит offset/length WARC-записи по конкретному домену.
2. `cc_links/cc_index.py` — для странового режима: то же самое, что делает Athena, но локально. DuckDB (`httpfs`) читает `cc-index` Parquet либо по HTTPS, либо напрямую из `s3://commoncrawl`; на EC2 credentials автоматически берутся из instance role. Результат сразу содержит offset/length WARC-записи.
3. `cc_links/fetch.py` — S3 GetObject Range на EC2 (либо HTTP Range вне AWS) забирает только нужный кусок WARC-файла и парсит HTML.
4. `cc_links/engines.py` + `cc_links/footprints.json` — эвристическая классификация страницы по движку (meta generator, характерные URL-пути, текст страницы) для 9 категорий: Article, Blog Comment, Directory, Forum, Guestbook, Image Comment, Microblog, Trackback, Social Network. Это не гарантированное определение CMS, а расширяемый набор сигнатур (как у W3Techs/Wappalyzer) — дополняйте `footprints.json` по необходимости.
5. `cc_links/exclusions.py` + `cc_links/exclusions.json` — глобальные мега-платформы (facebook, twitter/x, telegram, youtube, tiktok, instagram, linkedin, reddit, ...) исключаются и из обхода, и из сохранённых исходящих ссылок, чтобы не искажать статистику по движкам и не создавать им нагрузку. Список редактируется свободно — например, добавьте туда `vk.com`, если нужно исключить и его.
6. `cc_links/countries.py` — сопоставление ccTLD → страна и распределение бюджета страниц между странами по приоритетам.
7. `cc_links/db.py` — SQLite-схема: `pages` (url, domain, страна, tld, движок) и `links` (source_url, target_url, target_domain, anchor).

## Анализ

```
python analyze.py --db links.db --report summary
python analyze.py --db links.db --report top-domains
python analyze.py --db links.db --report top-pages-by-outlinks
python analyze.py --db links.db --report external-vs-internal
python analyze.py --db links.db --report engine-distribution      # доля страниц по категориям движков
python analyze.py --db links.db --report engine-detail            # детализация по конкретным движкам
python analyze.py --db links.db --report engine-by-country        # движки в разрезе стран
python analyze.py --db links.db --report country-coverage         # сколько страниц собрано/классифицировано на страну
python analyze.py --db links.db --report unclassified-rate        # доля страниц, для которых движок не определён
python analyze.py --db links.db --sql "SELECT * FROM links LIMIT 10"
```

Готовые отчёты в `analyze.py` — это SQL-запросы, эквивалентные тому, что обычно делают в Athena, но выполняются локально через `sqlite3`.

## Заметки

- CDX-индекс часто содержит несколько снимков одного и того же URL в разные даты — в `pages` они схлопываются по `url` (`INSERT OR IGNORE`), это нормально для MVP.
- Классификация движков — эвристика на паблик-сигнатурах (generator-тег, характерные пути, текст страницы), не 100% точная; расширяйте `cc_links/footprints.json` под свои категории.
