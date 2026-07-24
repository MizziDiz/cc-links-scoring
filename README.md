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
manifest доступен опциональный `--fetch-limit N`.

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
