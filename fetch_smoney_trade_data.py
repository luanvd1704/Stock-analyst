import re
import json
import requests
import pandas as pd

def fetch_valuation_history(symbol):
    url = f"https://smoney.com.vn/co-phieu/{symbol}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    html = resp.text

    # Regex linh hoạt hơn
    pattern = r"valuationHistory\s*=\s*JSON\.parse\('(.+?)'\)"
    m = re.search(pattern, html, re.DOTALL)
    if not m:
        raise ValueError("Không tìm thấy valuationHistory trong HTML.")

    raw = m.group(1)
    decoded = raw.encode().decode("unicode_escape")
    valuation_data = json.loads(decoded)

    # Dùng get() để tránh lỗi khi khóa không tồn tại
    dates = list(valuation_data.get("date", {}).values())
    pes   = list(valuation_data.get("pe", {}).values())
    pbs   = list(valuation_data.get("pb", {}).values())
    pcfs  = list(valuation_data.get("pcf_index", {}).values())

    # Nếu không có PCF, tạo danh sách None tương ứng số dòng
    if not pcfs:
        pcfs = [None] * len(dates)

    df = pd.DataFrame({
        "date": dates,
        "pe": pes,
        "pb": pbs,
        "pcfs": pcfs
    })
    return df

# Sử dụng:
df_hpg = fetch_valuation_history("HPG")
print(df_hpg.head())
df_hpg.to_csv("HPG_data.csv", index=False, encoding="utf-8-sig")
