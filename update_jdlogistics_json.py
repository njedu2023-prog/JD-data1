import os
import json
import datetime
import tushare as ts


TS_CODE = "02618.HK"
OUT_FILE = "jd-logistics-latest.json"


def bj_now():
    """北京时间（UTC+8），不依赖系统时区。"""
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def prev_weekday(d: datetime.date) -> datetime.date:
    """简单回退到上一个工作日（不处理港股节假日，只处理周末）。"""
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= datetime.timedelta(days=1)
    return d


def expected_trade_date_bj(now_bj: datetime.datetime) -> str:
    """
    估算“应该能拿到的最新交易日”：
    - 周末：回退到周五
    - 工作日：
      - 17:30（北京时间）之后：期望今天
      - 17:30 之前：期望上一个工作日
    """
    today = now_bj.date()
    if today.weekday() >= 5:
        return prev_weekday(today).strftime("%Y%m%d")

    cutoff = now_bj.replace(hour=17, minute=30, second=0, microsecond=0)
    if now_bj >= cutoff:
        return today.strftime("%Y%m%d")
    else:
        return prev_weekday(today - datetime.timedelta(days=1)).strftime("%Y%m%d")


def load_existing_date():
    """读取现有 json 的 date 字段（YYYY-MM-DD），转为 YYYYMMDD。不存在返回 None。"""
    try:
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        d = obj.get("date")
        if not d or len(d) != 10:
            return None
        return d.replace("-", "")
    except FileNotFoundError:
        return None
    except Exception:
        return None


def fetch_hk_daily_latest(pro, ts_code: str, days_back: int = 30):
    """抓取最近 N 天，返回 (df, latest_row)."""
    now_bj_dt = bj_now()
    end_date = now_bj_dt.date().strftime("%Y%m%d")
    start_date = (now_bj_dt.date() - datetime.timedelta(days=days_back)).strftime("%Y%m%d")

    df = pro.hk_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    print(f"[INFO] ts_code={ts_code}")
    print(f"[INFO] request_range={start_date}~{end_date}")

    if df is None or df.empty:
        print("[WARN] Tushare returned empty dataframe.")
        return None, None

    df["trade_date"] = df["trade_date"].astype(str)
    df = df.sort_values("trade_date")

    min_d = df.iloc[0]["trade_date"]
    max_d = df.iloc[-1]["trade_date"]
    print(f"[INFO] returned_rows={len(df)} returned_range={min_d}~{max_d}")

    latest = df.iloc[-1]
    return df, latest


def row_to_json(latest, ts_code: str):
    open_p = float(latest["open"])
    high_p = float(latest["high"])
    low_p = float(latest["low"])
    close_p = float(latest["close"])

    vol = latest.get("vol", 0)
    amount = latest.get("amount", None)

    volume_i = int(float(vol)) if vol is not None else 0
    if amount is None or amount == "":
        amount_f = round(close_p * volume_i, 2)
    else:
        amount_f = float(amount)

    trade_date = str(latest["trade_date"])
    date_fmt = f"{trade_date[0:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

    return {
        "symbol": ts_code,
        "date": date_fmt,
        "open": round(open_p, 2),
        "high": round(high_p, 2),
        "low": round(low_p, 2),
        "close": round(close_p, 2),
        "volume": volume_i,
        "amount": round(amount_f, 2),
    }


def main():
    # ✅ 关键修改：使用新的 Token 名
    token = os.getenv("HK_MARKET_API_TOKEN")
    if not token:
        raise RuntimeError("Missing HK_MARKET_API_TOKEN in environment")

    ts.set_token(token)
    pro = ts.pro_api()

    now_bj_dt = bj_now()
    expected_td = expected_trade_date_bj(now_bj_dt)
    print(f"[INFO] now_bj={now_bj_dt.strftime('%Y-%m-%d %H:%M:%S')} expected_trade_date={expected_td}")

    df, latest = fetch_hk_daily_latest(pro, TS_CODE, days_back=30)
    if latest is None:
        print("[INFO] No data from Tushare. Exit without updating.")
        return

    latest_td = str(latest["trade_date"])
    print(f"[INFO] tushare_latest_trade_date={latest_td}")

    force = os.getenv("FORCE_UPDATE", "").strip().lower() in ("1", "true", "yes")
    if (latest_td < expected_td) and (not force):
        print("[INFO] Tushare NOT updated to expected trade_date yet.")
        print(f"[INFO] latest_td={latest_td} < expected_td={expected_td} -> skip writing JSON.")
        return

    if latest_td > expected_td:
        print("[WARN] latest_td later than expected_td, proceed anyway.")

    data = row_to_json(latest, TS_CODE)

    existing_td = load_existing_date()
    if existing_td == latest_td and not force:
        print(f"[INFO] Output already at trade_date={latest_td}. No changes.")
        return

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("[OK] Updated:", data)


if __name__ == "__main__":
    main()
