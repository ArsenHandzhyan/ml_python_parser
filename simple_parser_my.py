import csv
import json
import os
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup

def scrape_books(base_url):
    t0 = time.time()
    output_dir = os.path.join("data", "sync")
    os.makedirs(output_dir, exist_ok=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }

    session = requests.Session()
    session.headers.update(headers)

    # 1) Главная страница
    print(f"")

    reg = session.get(base_url)
    soup = BeautifulSoup(reg.text, "html.parser")

    # 2) Ссылки на категории
    categories = []
    cat_list = soup.select_one(".side_categories ul.nav.nav-list")
    for a in cat_list.select("li ul li a"):
        name = a.get_text(strip=True)
        url = base_url + a.get("href")
        categories.append((name, url))
    # 3) Сбор ссылок на книги (без записи на диск)
    t_cat = time.time()
    book_rel_links = []
    for name, url in categories:
        r = session.get(url)
        s = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in s.select("article.product_pod h3 a")]
        book_rel_links.extend(links)
        print(f"Категория '{name}': {len(links)} книг")
    print(f"Категории обработаны за {time.time() - t_cat:.2f} сек")

    # 4) Приводим ссылки к абсолютным и убираем дубли
    base_catalogue = base_url + "catalogue/"
    seen = set()
    book_urls = []
    for rel in book_rel_links:
        full = base_catalogue + rel.replace("../../../", "")
        if full not in seen:
            seen.add(full)
            book_urls.append(full)
    # 5) Парсим книги
    t_books = time.time()
    results = []
    for book_url in book_urls:
        try:
            r = session.get(book_url)
            s = BeautifulSoup(r.text, "html.parser")

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
        except Exception as e:
            print(f"Ошибка {book_url}: {e}")

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

    print(f"Общее время: {time.time() - t0:.2f} сек")

scrape_books("https://books.toscrape.com/")
