import os
import json
import time
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values


WB_URL = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/table/details"


def msk_today() -> dt.date:
    return dt.datetime.now(ZoneInfo("Europe/Moscow")).date()


def safe_get(obj: Dict[str, Any], path: Tuple[str, ...], default=None):
    cur: Any = obj
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def fetch_page(
    token: str,
    report_date: dt.date,
    offset: int,
    limit: int,
    position_cluster: str,
    include_substituted_skus: bool,
    include_search_texts: bool,
    order_field: str,
    order_mode: str,
    timeout_sec: int = 60,
) -> List[Dict[str, Any]]:
    past_date = report_date - dt.timedelta(days=1)

    payload = {
        "currentPeriod": {"start": report_date.isoformat(), "end": report_date.isoformat()},
        "pastPeriod": {"start": past_date.isoformat(), "end": past_date.isoformat()},
        "orderBy": {"field": order_field, "mode": order_mode},
        "positionCluster": position_cluster,
        "includeSubstitutedSKUs": include_substituted_skus,
        "includeSearchTexts": include_search_texts,
        "limit": limit,
        "offset": offset,
    }

    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }

    r = requests.post(WB_URL, headers=headers, json=payload, timeout=timeout_sec)
    if r.status_code == 429:
        raise RuntimeError("HTTP 429 Too Many Requests")
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")

    data = r.json()
    products = safe_get(data, ("data", "products"), default=[])
    return products if isinstance(products, list) else []


def upsert_raw_items(
    conn,
    report_date: dt.date,
    position_cluster: str,
    include_substituted_skus: bool,
    include_search_texts: bool,
    order_field: str,
    order_mode: str,
    products: List[Dict[str, Any]],
) -> int:
    if not products:
        return 0

    rows = []
    for p in products:
        nm_id = p.get("nmId")
        if nm_id is None:
            continue
        rows.append([
            report_date,
            position_cluster,
            include_substituted_skus,
            include_search_texts,
            order_field,
            order_mode,
            int(nm_id),
            json.dumps(p, ensure_ascii=False),
        ])

    sql = """
    insert into public.wb_search_products_daily_raw
      (report_date, position_cluster, include_substituted_skus, include_search_texts, order_field, order_mode, nm_id, raw_item)
    values %s
    on conflict (report_date, position_cluster, include_substituted_skus, include_search_texts, order_field, order_mode, nm_id)
    do update set
      load_dttm = now(),
      raw_item  = excluded.raw_item::jsonb
    ;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    token = os.environ["WB_SA_TOKEN"]
    dsn = os.environ["SUPABASE_DSN"]

    position_cluster = os.getenv("WB_POSITION_CLUSTER", "all")
    include_substituted = os.getenv("WB_INCLUDE_SUBSTITUTED", "true").lower() == "true"
    include_search_texts = os.getenv("WB_INCLUDE_SEARCH_TEXTS", "true").lower() == "true"
    order_field = os.getenv("WB_ORDER_FIELD", "orders")
    order_mode = os.getenv("WB_ORDER_MODE", "desc")

    days_back = int(os.getenv("DAYS_BACK", "2"))  # вчера+позавчера по умолчанию
    limit = int(os.getenv("WB_LIMIT", "1000"))
    sleep_sec = float(os.getenv("WB_SLEEP_SEC", "21"))  # 3 req/min => ~20 сек пауза

    today = msk_today()
    dates = [(today - dt.timedelta(days=i)) for i in range(1, days_back + 1)]

    print(f"MSK today: {today} | Reload dates: {dates}")

    with psycopg2.connect(dsn) as conn:
        for report_date in dates:
            print(f"\n=== report_date={report_date} ===")
            offset = 0
            total_upserted = 0

            while True:
                print(f"Fetch offset={offset} limit={limit}")
                try:
                    products = fetch_page(
                        token=token,
                        report_date=report_date,
                        offset=offset,
                        limit=limit,
                        position_cluster=position_cluster,
                        include_substituted_skus=include_substituted,
                        include_search_texts=include_search_texts,
                        order_field=order_field,
                        order_mode=order_mode,
                    )
                except RuntimeError as e:
                    if "429" in str(e):
                        print("Rate limit. Sleep 30s and retry...")
                        time.sleep(30)
                        continue
                    raise

                if not products:
                    break

                n = upsert_raw_items(
                    conn=conn,
                    report_date=report_date,
                    position_cluster=position_cluster,
                    include_substituted_skus=include_substituted,
                    include_search_texts=include_search_texts,
                    order_field=order_field,
                    order_mode=order_mode,
                    products=products,
                )
                total_upserted += n

                offset += limit
                time.sleep(sleep_sec)

            print(f"Done report_date={report_date}. Upserted: {total_upserted}")


if __name__ == "__main__":
    main()
