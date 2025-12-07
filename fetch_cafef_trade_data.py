import requests
import pandas as pd
import datetime
from typing import List, Dict, Any


def _fetch_json_rows(
    url: str,
    symbol: str,
    list_key: str,
    start_date: str = "",
    end_date: str = "",
    page_size: int = 2000,
) -> List[Dict[str, Any]]:
    """
    Internal helper to iterate through CaféF's paginated JSON API.

    - Không bắt buộc truyền StartDate/EndDate (giống request trên web).
    - Phân trang đến khi API trả về records rỗng.
    """
    page_index = 1
    rows: List[Dict[str, Any]] = []
    total_count = None

    while True:
        # Các tham số cơ bản giống như request trên web
        params: Dict[str, Any] = {
            "Symbol": symbol,
            "PageIndex": page_index,
            "PageSize": page_size,
        }

        # Chỉ truyền StartDate/EndDate nếu có thiết lập
        if start_date:
            params["StartDate"] = start_date  # dd/MM/yyyy
        if end_date:
            params["EndDate"] = end_date      # dd/MM/yyyy

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            "Referer": "https://cafef.vn/",
            "Accept": "application/json, text/plain, */*",
        }

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data_json = response.json()
        data_section = data_json.get("Data", {})

        if total_count is None:
            total_count = data_section.get("TotalCount", 0)

        # Lấy danh sách record theo cấu trúc JSON của CafeF
        if list_key in data_section:
            records = data_section.get(list_key, [])
        else:
            inner = data_section.get("Data", {})
            records = inner.get(list_key, [])

        # Nếu page hiện tại không còn bản ghi -> dừng
        if not records:
            break

        rows.extend(records)

        # Nếu TotalCount là tổng số bản ghi thật sự thì điều kiện này giúp tránh gọi thừa
        if total_count and len(rows) >= total_count:
            break

        page_index += 1

    return rows


def fetch_cafef_foreign_trades(
    symbol: str,
    start_date: str = "",
    end_date: str = "",
    page_size: int = 2000,
) -> pd.DataFrame:
    """
    Download all foreign trading records for a given ticker from CaféF.

    API: https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx
    """
    base_url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx"

    rows = _fetch_json_rows(
        base_url,
        symbol,
        list_key="Data",
        start_date=start_date,
        end_date=end_date,
        page_size=page_size,
    )

    df = pd.DataFrame(rows)

    if not df.empty and "Ngay" in df.columns:
        # Cột Ngày dạng dd/MM/yyyy
        df["Ngay"] = pd.to_datetime(df["Ngay"], format="%d/%m/%Y")

    return df


def fetch_cafef_self_trades(
    symbol: str,
    start_date: str = "",
    end_date: str = "",
    page_size: int = 1000,
) -> pd.DataFrame:
    """
    Download all proprietary trading records (tự doanh) from CaféF.

    API: https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx
    """
    base_url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx"

    rows = _fetch_json_rows(
        base_url,
        symbol,
        list_key="ListDataTudoanh",
        start_date=start_date,
        end_date=end_date,
        page_size=page_size,
    )

    df = pd.DataFrame(rows)

    if not df.empty and "Date" in df.columns:
        # Cột Date dạng dd/MM/yyyy
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    return df


if __name__ == "__main__":
    symbol = "HPG"

    # Nếu muốn giống hệt khi mở link trên web: KHÔNG cần start/end date
    # start_date = ""   # vd: "01/01/2022"
    # end_date   = ""   # vd: "05/12/2025"

    # Nếu muốn giới hạn theo khoảng ngày, dùng dạng dd/MM/yyyy:
    # start_date = "01/01/2022"
    # end_date = datetime.datetime.now().strftime("%d/%m/%Y")
    start_date = ""
    end_date = ""

    try:
        foreign_df = fetch_cafef_foreign_trades(
            symbol, start_date=start_date, end_date=end_date
        )
        self_df = fetch_cafef_self_trades(
            symbol, start_date=start_date, end_date=end_date
        )

        print(f"Fetched {len(foreign_df)} foreign trade rows for {symbol}.")
        print(f"Fetched {len(self_df)} self trade rows for {symbol}.")

        if not foreign_df.empty:
            print("Max Ngay (foreign):", foreign_df["Ngay"].max())
            print("Latest foreign trades:")
            print(foreign_df.sort_values("Ngay", ascending=False).head())

        if not self_df.empty:
            print("Max Date (self):", self_df["Date"].max())
            print("Latest self trades:")
            print(self_df.sort_values("Date", ascending=False).head())

    except Exception as e:
        print("An error occurred while fetching data from CafeF:", e)
