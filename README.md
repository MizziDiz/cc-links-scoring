# cc-links — сбор и анализ ссылок из Common Crawl без Athena

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
- `--no-links` — не сохранять отдельные исходящие ссылки в таблицу `links`, только их количество (`pages.outlink_count`). Для скоринга движков по странам сама таблица ссылок не нужна, а именно она отвечает за почти весь объём базы: ~100+ строк ссылок на страницу означает ~50+ ГБ на 1.4 млн страниц против пары сотен МБ без неё. Сами HTML-страницы на диск никогда не пишутся — они разбираются в памяти и сразу отбрасываются.
- `--proxy` / `--proxy-file` — маршрутизация запросов через прокси (одиночный rotating-gateway URL, либо файл со списком `host:port:user:pass` — тогда запросы идут по кругу по всему пулу). Троттлинг CloudFront у `data.commoncrawl.org` привязан к IP отправителя (~35-40 req/с — безопасный потолок на один IP, подтверждено эмпирически), так что пул прокси нужен, чтобы поднять `--rate-limit` выше этого потолка. Прокси должны поддерживать HTTPS CONNECT-туннель — обычный HTTP-проксирование не подойдёт, так как `data.commoncrawl.org` отдаёт данные только по HTTPS.
- `--rate-limit` — общий лимит запросов/сек по всем потокам (не на поток). При устойчивой серии сбоев подряд пайплайн сам снижает лимит вдвое и делает паузу 90с (защита от троттлинга).

Как это работает:
1. `cc_links/cdx.py` — запрос к CDX Index API (`index.commoncrawl.org`) для доменного режима: находит offset/length WARC-записи по конкретному домену.
2. `cc_links/cc_index.py` — для странового режима: то же самое, что делает Athena, но локально. Common Crawl хранит колоночный индекс (`cc-index` Parquet) в двух местах — в S3 (это то, что обычно сканирует Athena) и зеркалом на `data.commoncrawl.org` по обычному HTTPS. DuckDB (`httpfs`) читает этот Parquet прямо оттуда, без AWS-ключей и без Athena, и сразу отдаёт offset/length WARC-записи, отфильтрованные по `url_host_tld`.
3. `cc_links/fetch.py` — HTTP Range-запрос к `data.commoncrawl.org` забирает только нужный кусок WARC-файла и парсит HTML.
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

## Мониторинг A-Parser → Telegram

Отдельный самодостаточный инструмент в папке [`aparser_monitor/`](aparser_monitor/): уведомления в Telegram о завершении заданий, авариях (> 50% ошибок) и недоступности A-Parser, с кулдауном 8 часов. Настройка и запуск — в [`aparser_monitor/README.md`](aparser_monitor/README.md).

## Заметки

- CDX-индекс часто содержит несколько снимков одного и того же URL в разные даты — в `pages` они схлопываются по `url` (`INSERT OR IGNORE`), это нормально для MVP.
- Классификация движков — эвристика на паблик-сигнатурах (generator-тег, характерные пути, текст страницы), не 100% точная; расширяйте `cc_links/footprints.json` под свои категории.
