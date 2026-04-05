import argparse
import asyncio
import httpx
import os
from datetime import date, timedelta

HEADERS = {"User-Agent": "yourname@example.com"}
IDX_DIR = "idx"


def _quarter(d: date) -> str:
    return f"QTR{(d.month - 1) // 3 + 1}"


def _url(d: date) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/daily-index/"
        f"{d.year}/{_quarter(d)}/form.{d.strftime('%Y%m%d')}.idx"
    )


async def _download_one(client: httpx.AsyncClient, d: date) -> None:
    date_str = d.strftime("%Y%m%d")
    local_path = os.path.join(IDX_DIR, f"form.{date_str}.idx")

    if os.path.exists(local_path):
        print(f"[EXISTS ] {local_path}", flush=True)
        return

    r = await client.get(_url(d), headers=HEADERS)

    if r.status_code in (403, 404):
        print(f"[SKIPPED] {date_str} (weekend/holiday)", flush=True)
        return

    if r.status_code != 200:
        print(f"[ ERROR ] {date_str} status={r.status_code}", flush=True)
        return

    with open(local_path, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"[  OK   ] {local_path}", flush=True)


async def _download_all(dates: list) -> None:
    os.makedirs(IDX_DIR, exist_ok=True)
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[_download_one(client, d) for d in dates])


def _parse_args():
    parser = argparse.ArgumentParser(description="Download SEC EDGAR daily index files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single date: YYYY-MM-DD")
    group.add_argument("--from", dest="date_from", help="Start of date range: YYYY-MM-DD")
    group.add_argument("--days", type=int, help="Last N calendar days")
    parser.add_argument("--to", dest="date_to", help="End of range for --from (default: today)")
    return parser.parse_args()


def main():
    args = _parse_args()
    today = date.today()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.days:
        dates = [today - timedelta(days=i) for i in range(args.days - 1, -1, -1)]
    else:
        start = date.fromisoformat(args.date_from)
        end = date.fromisoformat(args.date_to) if args.date_to else today
        dates = []
        d = start
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)

    asyncio.run(_download_all(dates))


if __name__ == "__main__":
    main()
