import glob
import os
import re
import pandas as pd

IDX_DIR = "idx"
OUTPUT_DIR = "parsed"


def _parse_fixed(path):
    """Parse fixed-width IDX format."""
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # Data starts after the dashed separator line
    start = next(
        (i + 1 for i, line in enumerate(lines) if line.startswith("---")), 11
    )

    for line in lines[start:]:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = re.split(r"  +", line.strip())
        if len(parts) < 5:
            continue
        form, company, cik, date, filename = parts[0], parts[1], parts[2], parts[3], parts[4]
        rows.append({
            "CIK": cik,
            "Company Name": company,
            "Form Type": form,
            "Date Filed": date,
            "File Name": filename,
        })

    return pd.DataFrame(rows)


def parse_idx_file(path):
    """Parse a single IDX file. Returns DataFrame of 8-K rows only."""
    df = _parse_fixed(path)
    return df[df["Form Type"].str.strip() == "8-K"].copy()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    idx_files = sorted(glob.glob(os.path.join(IDX_DIR, "*.idx")))
    if not idx_files:
        print("No IDX files found in idx/")
        return

    all_8k = []
    for path in idx_files:
        df = parse_idx_file(path)
        print(f"{os.path.basename(path)} -> {len(df)} 8-K filings", flush=True)
        all_8k.append(df)

    combined = pd.concat(all_8k, ignore_index=True).drop_duplicates(subset="File Name")
    combined.to_parquet(os.path.join(OUTPUT_DIR, "8k.parquet"), index=False)
    combined.to_csv(os.path.join(OUTPUT_DIR, "8k.csv"), index=False)
    print(f"\nTotal: {len(combined)} unique 8-K filings saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
