import csv
import json
import os
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
import logging

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


def fetch_text(session, url):
    start = time.time()
    try:
        resp = session.get(url)
        http_requests_total.inc()
        http_request_duration.observe(time.time() - start)
        return resp.text
    except Exception:
        http_request_errors_total.inc()
        http_request_duration.observe(time.time() - start)
        raise


def scrape_books(base_url, logger):
    t0 = time.time()
    output_dir = os.path.join("data", "sync")
    os.makedirs(output_dir, exist_ok=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }

    session = requests.Session()
    session.headers.update(headers)

    # 1) Главная страница
    reg_text = fetch_text(session, base_url)
    soup = BeautifulSoup(reg_text, "html.parser")

    # 2) Ссылки на категории
    categories = []
    cat_list = soup.select_one(".side_categories ul.nav.nav-list")
    for a in cat_list.select("li ul li a"):
        name = a.get_text(strip=True)
        url = base_url + a.get("href")
        categories.append((name, url))
    categories_count.set(len(categories))
    # 3) Сбор ссылок на книги (без записи на диск)
    t_cat = time.time()
    book_rel_links = []
    for name, url in categories:
        page_url = url
        category_links = []
        while True:
            page_text = fetch_text(session, page_url)
            s = BeautifulSoup(page_text, "html.parser")
            links = [a["href"] for a in s.select("article.product_pod h3 a")]
            category_links.extend(links)
            next_link = s.select_one("li.next a")
            if not next_link:
                break
            page_url = urljoin(page_url, next_link.get("href"))
        book_rel_links.extend(category_links)
        logger.info("Категория '%s': %d книг", name, len(category_links))
        category_books_count.labels(category=name).set(len(category_links))
    logger.info("Категории обработаны за %.2f сек", time.time() - t_cat)

    # 4) Приводим ссылки к абсолютным и убираем дубли
    base_catalogue = base_url + "catalogue/"
    seen = set()
    book_urls = []
    for rel in book_rel_links:
        full = base_catalogue + rel.replace("../../../", "")
        if full not in seen:
            seen.add(full)
            book_urls.append(full)
    books_found_total.set(len(book_urls))
    # 5) Парсим книги
    t_books = time.time()
    results = []
    errors_count = 0
    progress_step = int(os.getenv("LOG_PROGRESS_EVERY", "50"))
    log_each_book = os.getenv("LOG_EACH_BOOK", "1") != "0"
    for idx, book_url in enumerate(book_urls, start=1):
        try:
            book_text = fetch_text(session, book_url)
            s = BeautifulSoup(book_text, "html.parser")

            info = {}
            for row in s.select("table.table.table-striped tr"):
                key = row.find("th").get_text(strip=True)
                val = row.find("td").get_text(strip=True)
                info[key] = val

            title = s.select_one("div.product_main h1").get_text(strip=True)
            category = s.select("ul.breadcrumb li a")[-1].get_text(strip=True)

            results.append({
                "title": title,
                "category": category,
                "upc": info.get("UPC"),
                "product_type": info.get("Product Type"),
                "price_excl_tax": info.get("Price (excl. tax)"),
                "price_inc_tax": info.get("Price (incl. tax)"),
                "tax": info.get("Tax"),
                "availability": info.get("Availability"),
                "num_reviews": info.get("Number of reviews"),
            })
            books_parsed_total.inc()
            if log_each_book:
                logger.info("Обработана книга: %s", title)
        except Exception as e:
            logger.info("Ошибка %s: %s", book_url, e)
            books_errors_total.inc()
            errors_count += 1
        if progress_step > 0 and idx % progress_step == 0:
            logger.info("Прогресс: %d/%d книг обработано", idx, len(book_urls))

    # 6) CSV + JSON
    if results:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = os.path.join(output_dir, f"books_{ts}.csv")
        json_name = os.path.join(output_dir, f"books_{ts}.json")
        with open(csv_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        with open(json_name, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

    finish_time = time.time() - t0
    scrape_duration.set(finish_time)
    logger.info(
        "Готово: категории=%d, найдено=%d, распарсено=%d, ошибок=%d, время=%.2f сек",
        len(categories),
        len(book_urls),
        len(results),
        errors_count,
        finish_time,
    )

def main():
    base_url = "https://books.toscrape.com/"
    metrics_port = int(os.getenv("PROM_PORT", "8000"))
    metrics_ttl = int(os.getenv("METRICS_TTL_SECONDS", "3600"))
    logger, log_path, run_number = init_logging("simple_parser")
    logger.info("Старт. Лог: %s, запуск #%d", log_path, run_number)
    start_http_server(metrics_port)
    scrape_books(base_url, logger)
    metrics_path = write_metrics_snapshot("simple_parser", run_number)
    logger.info("Снимок метрик сохранен: %s", metrics_path)
    if metrics_ttl > 0:
        logger.info("Метрики будут доступны еще %d секунд", metrics_ttl)
        time.sleep(metrics_ttl)


if __name__ == "__main__":
    main()
