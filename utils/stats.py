"""
stats.py — Print pipeline stats based on current CSV state.
"""
import os
import pandas as pd

PARSED = "parsed"


def section(title):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


def load(filename, **kwargs):
    path = os.path.join(PARSED, filename)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path, **kwargs)


def main():
    # ── 8k.csv ────────────────────────────────────────
    df_8k = load("8k.csv")
    if df_8k is not None:
        section("8k.csv")
        print(f"  Total 8-K filings:  {len(df_8k)}")

    # ── 8k_ex99.csv ───────────────────────────────────
    df_ex99 = load("8k_ex99.csv")
    if df_ex99 is not None:
        section("8k_ex99.csv")
        has_ex99 = df_ex99["ex99_url"].notna() & (df_ex99["ex99_url"] != "")
        print(f"  Total rows:         {len(df_ex99)}")
        print(f"  Has EX-99:          {has_ex99.sum()}")
        print(f"  No EX-99:           {(~has_ex99).sum()}")

    # ── prs.csv ───────────────────────────────────────
    df_prs = load("prs.csv")
    if df_prs is not None:
        section("prs.csv — PR classifications")
        print(f"  Total PRs:          {len(df_prs)}")

        if "heuristic" in df_prs.columns:
            print("\n  By heuristic label:")
            for label, count in df_prs["heuristic"].value_counts().items():
                pct = count / len(df_prs) * 100
                print(f"    {label:<12} {count:>5}  ({pct:.1f}%)")

        heuristics = [h for h in ["H1", "H2", "H3", "H4", "H5", "H6", "H7"] if h in df_prs.columns]
        if heuristics:
            print("\n  Heuristic fire rates:")
            for h in heuristics:
                fired = df_prs[h].sum()
                pct = fired / len(df_prs) * 100
                print(f"    {h}  fired {fired:>5}/{len(df_prs)}  ({pct:.1f}%)")

    # ── price_data.csv ────────────────────────────────
    df_prices = load("price_data.csv")
    if df_prices is not None:
        section("price_data.csv")
        print(f"  Total rows:         {len(df_prices)}")
        has_price = df_prices["price_t0"].notna()
        print(f"  Has price data:     {has_price.sum()}")
        print(f"  No price data:      {(~has_price).sum()}")

        for col in ["change_5m_pct", "change_30m_pct", "change_1h_pct", "change_4h_pct", "change_1d_pct"]:
            if col in df_prices.columns:
                s = df_prices[col].dropna()
                if len(s):
                    print(f"\n  {col}:")
                    print(f"    mean={s.mean():.3f}%  median={s.median():.3f}%  std={s.std():.3f}%")


if __name__ == "__main__":
    main()
