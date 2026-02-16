import csv
import json
import time
from datetime import datetime
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
import os
from urllib.parse import urljoin
import logging

# Глобальное хранилище результатов
books_data = []
# Время старта всего скрипта
start_time = time.time()

# --- Метрики Prometheus ---
scrape_duration = Gauge("scrape_duration_seconds", "Общее время работы скрипта")
categories_count = Gauge("categories_count", "Количество категорий")
books_found_total = Gauge("books_found_total", "Количество уникальных книг")
books_parsed_total = Counter("books_parsed_total", "Количество успешно распарсенных книг")
books_errors_total = Counter("books_errors_total", "Количество ошибок при парсинге книг")
http_requests_total = Counter("http_requests_total", "Количество HTTP запросов")
http_request_errors_total = Counter("http_request_errors_total", "Количество ошибок HTTP")
http_request_duration = Histogram("http_request_duration_seconds", "Время HTTP запросов")
category_books_count = Gauge("category_books_count", "Книг в категории", ["category"])

# --- HTTP: получить текст страницы ---
async def fetch_text(session, url, headers):
    start = time.time()
    try:
        async with session.get(url, headers=headers) as resp:
            text = await resp.text()
            http_requests_total.inc()
            http_request_duration.observe(time.time() - start)
            return text
    except Exception:
        http_request_errors_total.inc()
        http_request_duration.observe(time.time() - start)
        raise

# --- Блок: получить ссылки книг из одной категории ---
async def get_category_book_links(session, name, url, base_url, headers, logger):
    book_urls = []
    page_url = url
    base_catalogue = base_url + "catalogue/"

    while True:
        text = await fetch_text(session, page_url, headers)
        soup = BeautifulSoup(text, "html.parser")
        links = [a["href"] for a in soup.select("article.product_pod h3 a")]
        for rel in links:
            book_urls.append(base_catalogue + rel.replace("../../../", ""))

        next_link = soup.select_one("li.next a")
        if not next_link:
            break
        page_url = urljoin(page_url, next_link.get("href"))

    logger.info("Категория '%s': %d книг", name, len(book_urls))
    category_books_count.labels(category=name).set(len(book_urls))
    return book_urls

# --- Блок: получить данные одной книги ---
async def get_book_data(session, book_url, headers):
    # Скачиваем HTML книги
    text = await fetch_text(session, book_url, headers)
    soup = BeautifulSoup(text, "html.parser")

    # Извлекаем таблицу Product Information
    info = {}
    for row in soup.select("table.table.table-striped tr"):
        key = row.find("th").get_text(strip=True)
        val = row.find("td").get_text(strip=True)
        info[key] = val

    # Название и категория книги
    title = soup.select_one("div.product_main h1").get_text(strip=True)
    category = soup.select("ul.breadcrumb li a")[-1].get_text(strip=True)

    # Возвращаем словарь с нужными полями
    return {
        "title": title,
        "category": category,
        "upc": info.get("UPC"),
        "product_type": info.get("Product Type"),
        "price_excl_tax": info.get("Price (excl. tax)"),
        "price_inc_tax": info.get("Price (incl. tax)"),
        "tax": info.get("Tax"),
        "availability": info.get("Availability"),
        "num_reviews": info.get("Number of reviews"),
    }

# --- Блок: обработка страницы каталога (если понадобится) ---
async def get_page_data(session, page, base_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }

    url = f"{base_url}catalogue/page-{page}.html"
    text = await fetch_text(session, url, headers)
    soup = BeautifulSoup(text, "html.parser")
    # Здесь можно добавить логику для обработки данных страницы

# --- Главная асинхронная функция ---
async def gather_data(base_url, logger):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }

    # Создаем сессию aiohttp
    async with aiohttp.ClientSession() as session:
        # 1) Скачиваем главную страницу
        text = await fetch_text(session, base_url, headers)
        soup = BeautifulSoup(text, "html.parser")

        # 2) Собираем категории
        categories = []
        cat_list = soup.select_one(".side_categories ul.nav.nav-list")
        for a in cat_list.select("li ul li a"):
            name = a.get_text(strip=True)
            url = base_url + a.get("href")
            categories.append((name, url))
        categories_count.set(len(categories))

        # 3) Получаем ссылки на книги из всех категорий
        t_cat = time.time()
        category_tasks = [
            get_category_book_links(session, name, url, base_url, headers, logger)
            for name, url in categories
        ]
        category_results = await asyncio.gather(*category_tasks)
        logger.info("Категории обработаны за %.2f сек", time.time() - t_cat)

        # 4) Убираем дубли ссылок на книги
        seen = set()
        book_urls = []
        for urls in category_results:
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    book_urls.append(u)
        books_found_total.set(len(book_urls))

        # 5) Парсим книги (ограничим одновременные запросы)
        t_books = time.time()
        max_concurrency = int(os.getenv("MAX_CONCURRENCY", "10"))
        sem = asyncio.Semaphore(max_concurrency)

        async def bounded_get(book_url):
            async with sem:
                return await get_book_data(session, book_url, headers)

        book_tasks = [asyncio.create_task(bounded_get(u)) for u in book_urls]
        errors_count = 0
        processed_count = 0
        progress_step = int(os.getenv("LOG_PROGRESS_EVERY", "50"))
        log_each_book = os.getenv("LOG_EACH_BOOK", "1") != "0"
        for task in asyncio.as_completed(book_tasks):
            try:
                item = await task
            except Exception as e:
                logger.info("Ошибка при обработке книги: %s", e)
                books_errors_total.inc()
                errors_count += 1
            else:
                books_data.append(item)
                if log_each_book:
                    logger.info("Обработана книга: %s", item["title"])
                books_parsed_total.inc()
            processed_count += 1
            if progress_step > 0 and processed_count % progress_step == 0:
                logger.info(
                    "Прогресс: %d/%d книг обработано",
                    processed_count,
                    len(book_urls),
                )

        logger.info("Книги обработаны за %.2f сек", time.time() - t_books)
        return {
            "categories": len(categories),
            "books_found": len(book_urls),
            "books_parsed": len(books_data),
            "books_errors": errors_count,
        }

# --- Точка входа ---
def main():
    # print(f"Дата и время начала: {time.time()}")
    base_url = "https://books.toscrape.com/"
    output_dir = os.path.join("data", "async")
    os.makedirs(output_dir, exist_ok=True)
    logger, log_path, run_number = init_logging("async_parser")
    logger.info("Старт. Лог: %s, запуск #%d", log_path, run_number)
    # Запускаем /metrics на localhost с настраиваемым портом
    metrics_port = int(os.getenv("PROM_PORT", "8000"))
    metrics_ttl = int(os.getenv("METRICS_TTL_SECONDS", "3600"))
    start_http_server(metrics_port)
    stats = asyncio.run(gather_data(base_url, logger))

    # Сохраняем JSON
    cur_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(output_dir, f"labirint_{cur_time}_async.json")
    with open(json_path, "w") as file:
        json.dump(books_data, file, indent=4, ensure_ascii=False)

    # Сохраняем CSV
    csv_path = os.path.join(output_dir, f"labirint_{cur_time}_async.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            (
                "title",
                "category",
                "upc",
                "product_type",
                "price_excl_tax",
                "price_inc_tax",
                "tax",
                "availability",
                "num_reviews"
            )
        )
        for book in books_data:
            writer.writerow(
                (
                    book["title"],
                    book["category"],
                    book["upc"],
                    book["product_type"],
                    book["price_excl_tax"],
                    book["price_inc_tax"],
                    book["tax"],
                    book["availability"],
                    book["num_reviews"]
                )
            )

    # Лог времени
    finish_time = time.time() - start_time
    scrape_duration.set(finish_time)
    logger.info("Дата и время окончания: %s", cur_time)
    logger.info("Время выполнения скрипта: %.2f секунд", finish_time)
    if stats:
        logger.info(
            "Готово: категории=%d, найдено=%d, распарсено=%d, ошибок=%d, время=%.2f сек",
            stats["categories"],
            stats["books_found"],
            stats["books_parsed"],
            stats["books_errors"],
            finish_time,
        )
    metrics_path = write_metrics_snapshot("async_parser", run_number)
    logger.info("Снимок метрик сохранен: %s", metrics_path)
    if metrics_ttl > 0:
        logger.info("Метрики будут доступны еще %d секунд", metrics_ttl)
        time.sleep(metrics_ttl)


def init_logging(run_label):
    logs_dir = os.path.join(os.path.dirname(__file__), "Logs")
    os.makedirs(logs_dir, exist_ok=True)

    counter_path = os.path.join(logs_dir, f"{run_label}.run_counter")
    try:
        with open(counter_path, "r", encoding="utf-8") as f:
            run_number = int(f.read().strip())
    except Exception:
        run_number = 0
    run_number += 1
    with open(counter_path, "w", encoding="utf-8") as f:
        f.write(str(run_number))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{run_label}_{timestamp}_run{run_number}.log"
    log_path = os.path.join(logs_dir, log_filename)

    logger = logging.getLogger(run_label)
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, log_path, run_number


def write_metrics_snapshot(run_label, run_number):
    metrics_dir = os.path.join(os.path.dirname(__file__), "Metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_filename = f"{run_label}_{timestamp}_run{run_number}.prom"
    metrics_path = os.path.join(metrics_dir, metrics_filename)
    with open(metrics_path, "wb") as f:
        f.write(generate_latest())
    return metrics_path


if __name__ == "__main__":
    main()
