"""
fetch_cafef_trade_data
=======================

This module provides convenience functions to download historical trading
data for Vietnamese stocks from CaféF's public endpoints.  CaféF displays
separate tables for foreign investor trades (``khối ngoại``) and for
proprietary or self-directed trades (``tự doanh``).  While the website
loads these tables via JavaScript, the underlying data can be accessed
directly through two JSON endpoints:

* ``GDKhoiNgoai.ashx`` – lists daily foreign trading statistics (volume
  bought/sold, net volume, value, room remaining, etc.).
* ``GDTuDoanh.ashx`` – lists daily proprietary trading statistics (volume
  and value bought/sold).

The functions in this module hide the pagination logic used by CaféF and
return the complete dataset as a :class:`pandas.DataFrame`.

Because these endpoints are unofficial and may change without notice,
consider caching the results locally.  The functions accept optional
``start_date`` and ``end_date`` parameters (strings in ``dd/mm/yyyy``
format) to filter the results server‑side; leaving them blank returns
all available history.

Example
-------

>>> from fetch_cafef_trade_data import fetch_cafef_foreign_trades, fetch_cafef_self_trades
>>> df_foreign = fetch_cafef_foreign_trades('HPG')
>>> df_self    = fetch_cafef_self_trades('HPG')
>>> print(df_foreign.head())
           Ngay  KLGDRong    ...   DangSoHuu
0 2025-12-05  3161300    ...      19.44
1 2025-12-02   337200    ...      19.36

The returned DataFrames use ``datetime`` for the date column and leave
other fields as numbers or strings according to the JSON.  You can
further clean or resample the data using pandas.
"""

import math
import requests
import pandas as pd
from typing import List, Dict, Any


def _fetch_json_rows(url: str, symbol: str, list_key: str,
                     start_date: str = "", end_date: str = "",
                     page_size: int = 2000) -> List[Dict[str, Any]]:
    """Internal helper to iterate through CaféF's paginated JSON API.

    Parameters
    ----------
    url : str
        Base URL for the CaféF endpoint (``GDKhoiNgoai.ashx`` or
        ``GDTuDoanh.ashx``).
    symbol : str
        Stock ticker, e.g. ``"HPG"``.
    list_key : str
        JSON key pointing to the list of records (``"Data"`` for
        foreign trades, ``"ListDataTudoanh"`` for self trades).
    start_date, end_date : str, optional
        Date filters in ``dd/mm/yyyy`` format.  Use empty string to
        request all available data.
    page_size : int, optional
        Number of records per page.  CaféF allows large values (e.g.
        2000) so the entire dataset can often be fetched in a single
        request.  If the dataset contains more rows than ``page_size``,
        this function automatically increments the page index.

    Returns
    -------
    List[Dict[str, Any]]
        Aggregated list of all rows across pages.
    """
    page_index = 1
    rows: List[Dict[str, Any]] = []
    total_count = None

    # Some endpoints wrap the list under another "Data" key.  We'll
    # detect that in the loop.
    while True:
        params = {
            "Symbol": symbol,
            "StartDate": start_date,
            "EndDate": end_date,
            "PageIndex": page_index,
            "PageSize": page_size,
        }
        headers = {
            # Use a realistic User-Agent to avoid being blocked.
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            "Referer": "https://cafef.vn/",  # mimic browser context
            "Accept": "application/json, text/plain, */*",
        }
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data_json = response.json()

        # The top-level JSON has the form {"Data": {...}, "Success": true, ...}
        data_section = data_json.get("Data", {})
        if total_count is None:
            total_count = data_section.get("TotalCount", 0)
        # Extract the list of rows using the provided key.  Some endpoints
        # wrap it inside another level (e.g. ``{"Data": {"ListDataTudoanh": [...]}}``)
        if list_key in data_section:
            # For foreign trades, list_key='Data' maps directly to a list
            records = data_section.get(list_key, [])  # type: ignore
        else:
            # For self trades, list_key='ListDataTudoanh' lives inside
            inner = data_section.get("Data", {})
            records = inner.get(list_key, [])

        rows.extend(records)

        # If we've fetched all rows, break
        if len(rows) >= total_count:
            break
        page_index += 1
    return rows


def fetch_cafef_foreign_trades(symbol: str,
                               start_date: str = "",
                               end_date: str = "",
                               page_size: int = 2000) -> pd.DataFrame:
    """Download all foreign trading records for a given ticker from CaféF.

    Parameters
    ----------
    symbol : str
        Stock ticker (e.g. ``"HPG"``).
    start_date, end_date : str, optional
        Date filters in ``dd/mm/yyyy`` format.  Leave blank for full
        history available on CaféF.
    page_size : int, optional
        Number of records per page.  Large values reduce the number of
        API calls.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns matching the JSON keys: ``Ngay`` (date),
        ``KLGDRong`` (net volume), ``GTDGRong`` (net value), ``ThayDoi``
        (price and percent change), ``KLMua``, ``GtMua``, ``KLBan``,
        ``GtBan``, ``RoomConLai`` (free room), and ``DangSoHuu`` (ownership
        percentage).  The ``Ngay`` column is converted to
        ``datetime64[ns]``.
    """
    base_url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx"
    rows = _fetch_json_rows(base_url, symbol, list_key="Data",
                            start_date=start_date, end_date=end_date,
                            page_size=page_size)
    df = pd.DataFrame(rows)
    # Convert date column to datetime (format dd/mm/YYYY)
    if not df.empty and "Ngay" in df.columns:
        df["Ngay"] = pd.to_datetime(df["Ngay"], format="%d/%m/%Y")
    return df


def fetch_cafef_self_trades(symbol: str,
                            start_date: str = "",
                            end_date: str = "",
                            page_size: int = 1000) -> pd.DataFrame:
    """Download all proprietary trading records for a given ticker from CaféF.

    Parameters
    ----------
    symbol : str
        Stock ticker (e.g. ``"HPG"``).
    start_date, end_date : str, optional
        Date filters in ``dd/mm/yyyy`` format.  Leave blank for full
        history available on CaféF.
    page_size : int, optional
        Number of records per page.  The history for self trading tends
        to be shorter than foreign trading, so the default is smaller.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns ``Symbol``, ``Date`` (converted to
        ``datetime64[ns]``), ``KLcpMua`` (shares bought), ``KlcpBan``
        (shares sold), ``GtMua`` (value bought), and ``GtBan`` (value sold).
    """
    base_url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx"
    rows = _fetch_json_rows(base_url, symbol, list_key="ListDataTudoanh",
                            start_date=start_date, end_date=end_date,
                            page_size=page_size)
    df = pd.DataFrame(rows)
    if not df.empty and "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    return df


if __name__ == "__main__":
    # Example usage: fetch full history for HPG and print summary
    symbol = "HPG"
    try:
        foreign_df = fetch_cafef_foreign_trades(symbol)
        self_df = fetch_cafef_self_trades(symbol)
        print(f"Fetched {len(foreign_df)} foreign trade rows for {symbol}.")
        print(f"Fetched {len(self_df)} self trade rows for {symbol}.")
        print(foreign_df.head())
        print(self_df.head())
    except Exception as e:
        print("An error occurred while fetching data from CafeF:", e)