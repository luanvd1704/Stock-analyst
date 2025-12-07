"""
Utility functions to fetch and aggregate foreign and self‑trading data from
Smoney for Vietnamese stocks.

The Smoney website embeds the most recent foreign‑trade and self‑trade data
inside `<script type="application/json">` tags on the stock detail page.  The
``fetch_smoney_trade_data`` function grabs this HTML and parses the JSON into
Pandas DataFrames.  Smoney exposes daily data for roughly the last month.  For
longer reporting intervals (6M, 1Y, 2Y, 3Y, 5Y) the website itself displays
weekly, monthly or quarterly aggregates on demand via client‑side JavaScript.
We don't have access to Smoney's internal API, so this module implements
approximate aggregations from the available daily data.  Keep in mind that
these aggregates will only cover the range of dates returned by Smoney (usually
the past 30 days), so weekly/monthly/quarterly results may contain far fewer
rows than expected when looking at longer horizons.

Example usage:

    from fetch_smoney_trade_data import fetch_smoney_trade_data, aggregate_trade_data

    # Fetch raw daily data for HPG
    foreign_df, self_df = fetch_smoney_trade_data("HPG")
    
    # Build DataFrames for each period (1M, 6M, 1Y, 2Y, 3Y, 5Y)
    foreign_1m  = aggregate_trade_data(foreign_df, period="1M")
    foreign_6m  = aggregate_trade_data(foreign_df, period="6M")
    foreign_1y  = aggregate_trade_data(foreign_df, period="1Y")
    foreign_2y  = aggregate_trade_data(foreign_df, period="2Y")
    foreign_3y  = aggregate_trade_data(foreign_df, period="3Y")
    foreign_5y  = aggregate_trade_data(foreign_df, period="5Y")
    
    # Similarly for self‑trading data
    self_1m = aggregate_trade_data(self_df, period="1M")
    ...

This module relies on the requests and pandas libraries; ensure they are
installed in your environment.  If Smoney changes its HTML structure or
limits access, the parsing logic may need to be updated.
"""

import json
import re
from typing import Tuple, Dict

import pandas as pd
import requests


def fetch_smoney_trade_data(symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch daily foreign‑trade and self‑trade data for a given stock symbol.

    This function requests the stock detail page on Smoney and extracts the
    JSON data embedded in the ``foreign‑trade‑data`` and ``self‑trade‑data``
    script tags.  It returns two DataFrames: one for foreign trading and one
    for self trading.  Both DataFrames have a ``date`` column converted to
    ``datetime64[ns]``.  If either script tag is missing, an exception is
    raised.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol as used on Smoney (e.g. "HPG" for Hòa Phát).

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        A tuple of (foreign_df, self_df) where each DataFrame contains the
        following columns: ``group_date``, ``date``, ``total_bva``, ``total_sva``,
        ``total_nva``, ``total_nv``, ``entryPriceBuy``, ``entryPriceSell``,
        ``cumulative_total_nva``, ``cumulative_total_nv``, ``entryPriceBuyAvg``,
        and ``entryPriceSellAvg``.

    Raises
    ------
    ValueError
        If the required JSON script tags are not found in the HTML.
    requests.HTTPError
        If the HTTP request fails.
    """
    url = f"https://smoney.com.vn/co-phieu/{symbol}"
    headers = {
        # Use a common desktop User‑Agent to avoid Smoney returning 403/302
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    html = resp.text

    def _extract_json(script_id: str) -> pd.DataFrame:
        pattern = rf'<script id="{script_id}"[^>]*>(.*?)</script>'
        match = re.search(pattern, html, flags=re.DOTALL)
        if not match:
            raise ValueError(f"Could not find JSON data for {script_id}")
        json_text = match.group(1)
        data = json.loads(json_text)
        df = pd.DataFrame(data)
        # Convert 'date' and 'group_date' to datetime if present
        for col in ["date", "group_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        return df

    foreign_df = _extract_json("foreign-trade-data")
    self_df = _extract_json("self-trade-data")
    return foreign_df, self_df


def aggregate_trade_data(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Aggregate trade data into the specified timeframe.

    Smoney displays different aggregation intervals depending on the selected
    timeframe: daily for 1M, weekly for 6M, monthly for 1Y/2Y and quarterly for
    3Y/5Y.  This function mirrors that logic using Pandas resampling.  When
    aggregating, it sums volume and value fields and takes the mean of price
    fields.  Cumulative fields are forward‑filled and then downsampled by
    taking the last value in each period.

    Because Smoney only provides roughly 30 days of daily data, aggregations
    longer than one month will produce fewer rows than the full period implies.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame returned by ``fetch_smoney_trade_data``.
    period : {"1M", "6M", "1Y", "2Y", "3Y", "5Y"}
        The desired aggregation horizon.  ``1M`` returns the original daily
        data; ``6M`` aggregates by week (``'W'``); ``1Y`` and ``2Y`` aggregate by
        month (``'M'``); ``3Y`` and ``5Y`` aggregate by quarter (``'Q'``).

    Returns
    -------
    pd.DataFrame
        A new DataFrame aggregated according to the specified period.  The
        ``date`` column is set to the period end (consistent with Pandas
        resampling) and other columns are aggregated as described.

    Raises
    ------
    ValueError
        If an unsupported period is supplied.
    """
    # Map Smoney periods to Pandas resample frequencies and expected durations
    period = period.upper()
    if period == "1M":
        # No aggregation needed; return a copy to avoid modifying the original
        return df.copy().reset_index(drop=True)
    elif period == "6M":
        freq = "W"
    elif period in {"1Y", "2Y"}:
        freq = "M"
    elif period in {"3Y", "5Y"}:
        freq = "Q"
    else:
        raise ValueError(
            "Unsupported period. Choose from '1M', '6M', '1Y', '2Y', '3Y', '5Y'."
        )

    df = df.copy()
    if "date" not in df.columns:
        raise ValueError("DataFrame must contain a 'date' column for resampling")
    df = df.set_index("date")

    # Forward fill cumulative columns before resampling to ensure we always
    # downsample the latest value within a period.
    cum_cols = [col for col in df.columns if "cumulative" in col]
    if cum_cols:
        df[cum_cols] = df[cum_cols].ffill()

    # Define aggregation rules: sum volumes/values, mean prices, last for cumulatives
    agg_map: Dict[str, str] = {}
    for col in df.columns:
        if col in cum_cols:
            agg_map[col] = "last"
        elif col.startswith("total_"):
            agg_map[col] = "sum"
        elif "price" in col.lower():
            agg_map[col] = "mean"
        else:
            # Default to mean for any other numeric columns
            agg_map[col] = "mean"

    aggregated = df.resample(freq).agg(agg_map)
    # Reset index to have 'date' as a column again
    aggregated = aggregated.reset_index()
    return aggregated


if __name__ == "__main__":
    # Example invocation for HPG.  This block will only run when the module
    # is executed directly, not when imported.
    stock_symbol = "HPG"
    foreign_df, self_df = fetch_smoney_trade_data(stock_symbol)
    for name, df in {"foreign": foreign_df, "self": self_df}.items():
        for period in ["1M", "6M", "1Y", "2Y", "3Y", "5Y"]:
            agg_df = aggregate_trade_data(df, period=period)
            print(f"{name} trade data for {stock_symbol} – {period}:")
            print(agg_df.head())
            print("-" * 60)