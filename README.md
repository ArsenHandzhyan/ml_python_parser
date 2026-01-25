# Books to Scrape Parser (Async + Prometheus + Grafana)

Небольшой учебный проект для парсинга сайта `books.toscrape.com` в асинхронном режиме.
Скрипт собирает данные по книгам, сохраняет JSON/CSV и отдаёт метрики Prometheus.

## Что делает скрипт
1. Загружает главную страницу сайта.
2. Находит категории книг.
3. Для каждой категории собирает ссылки на книги.
4. Асинхронно загружает страницы книг и парсит данные:
   - `title`, `category`
   - `upc`, `product_type`
   - `price_excl_tax`, `price_inc_tax`, `tax`
   - `availability`, `num_reviews`
5. Сохраняет результат в JSON/CSV.
6. Отдаёт метрики Prometheus на `/metrics`.

## Структура проекта
- `async_parser_my.py` — основной асинхронный парсер
- `prometheus.yml` — конфиг Prometheus
- `grafana_dashboard.json` — готовый дашборд Grafana
- `parser_my.py` — синхронный парсер (если нужен)

## Установка
```bash
python --version  # требуется Python 3.9.x
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск асинхронного парсера
```bash
python async_parser_my.py
```
Результаты:
- `labirint_YYYYMMDD_HHMMSS_async.json`
- `labirint_YYYYMMDD_HHMMSS_async.csv`

## Запуск простого парсера
```bash
python simple_parser_my.py
```
Результаты:
- `labirint_YYYYMMDD_HHMMSS.json`
- `labirint_YYYYMMDD_HHMMSS.csv`

Метрики доступны по адресу:
```
http://localhost:8000/metrics
```

## Версии для сборки и запуска
- Полный список закреплённых версий находится в `requirements.txt`
- В `requirements.txt` зафиксирован `urllib3==1.26.20`, чтобы избежать предупреждения про LibreSSL

## Prometheus (локально)
Создай файл `prometheus.yml` (он уже есть в проекте), затем запусти Prometheus:
```bash
docker run -p 9090:9090 \
  -v "$(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml" \
  prom/prometheus
```
Проверка:
```
http://localhost:9090/targets
```

## Grafana (локально)
```bash
docker run -p 3000:3000 grafana/grafana
```
Логин: `admin` / `admin`.

Добавь источник данных Prometheus:
- URL: `http://host.docker.internal:9090`

Импортируй дашборд:
- `grafana_dashboard.json`

## Метрики
- `scrape_duration_seconds` — общее время
- `categories_count` — количество категорий
- `books_found_total` — найдено книг
- `books_parsed_total` — успешно распарсено
- `books_errors_total` — ошибки
- `http_requests_total` — запросы
- `http_request_duration_seconds` — гистограмма времени запросов
- `category_books_count{category="..."}` — книги по категориям

## Примечания
- Асинхронность реализована через `aiohttp` и `asyncio`.
- Одновременно выполняется до 10 запросов к страницам книг (Semaphore).
- Проект учебный, без защиты от блокировок/лимитов.
