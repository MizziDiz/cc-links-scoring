# Fetch benchmark

[`benchmark_fetch_modes.py`](benchmark_fetch_modes.py) сравнивает threads и
async на одном наборе Common Crawl records без записи в БД. Скрипт включает
полный путь: Range-fetch, gzip/WARC-разбор, классификацию и, с `--with-links`,
извлечение outbound links.

Пример:

```bash
python benchmarks/benchmark_fetch_modes.py \
  --url-pattern 'commoncrawl.org/*' \
  --crawl CC-MAIN-2026-25 \
  --limit 100 \
  --workers 32 \
  --cpu-workers 1 \
  --rate-limit 60 \
  --with-links
```

Для стабильного повторного теста вместо CDX-запроса можно передать сохранённый
manifest через `--candidates-file`. Gateway берётся только из
`CC_GATEWAY_PROXY`; его значение скрипт не выводит.

## Контрольный CloudFront-прогон

Дата: 2026-07-23. Среда: Windows, Python 3.12.13, 16 logical CPU. Источник:
CloudFront, `CC-MAIN-2026-25`, 100 HTML records для `commoncrawl.org/*`,
concurrency 32, rate limit 60 requests/s, один CPU worker, `--with-links`.
Каждый режим успешно обработал 100/100 records.

| Порядок | Режим | Время, с | URL/с |
|---|---:|---:|---:|
| threads → async | threads | 2.196 | 45.529 |
| threads → async | async | 1.964 | 50.908 |
| async → threads | async | 2.084 | 47.993 |
| async → threads | threads | 2.156 | 46.381 |

Медиана двух измерений: threads — 45.955 URL/с, async — 49.451 URL/с,
то есть async был быстрее примерно на 7.6%. На прогретом classify-only прогоне
разница почти исчезла: 51.165 против 51.543 URL/с (около 0.7% в пользу async).

Это локальный CloudFront-бенчмарк, а не эмуляция 1 vCPU. Он показывает умеренный
I/O-bound выигрыш, но не доказывает такой же прирост на CPU-bound машине. Gateway
не тестировался: безопасные credentials для этого benchmark-run не
предоставлялись. Поэтому в проекте threads остаётся дефолтом, а выбор режима
нужно подтверждать на целевой машине и типичном manifest.
