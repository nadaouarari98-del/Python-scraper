import pdfplumber
import pandas as pd
from src.parser.normalizer import normalize_dataframe, detect_fy_columns, parse_amount

# FY column detection test  
test_cols = [
    "Sr. No", "FolioNo", "Name", "Current Holding",
    "Final Dividend\nAmount FY 2017-\n2018",
    "Final Dividend\nAmount FY 2018-\n2019",
    "Interim Dividend\nAmount FY 2019-2020",
    "Final Dividend Amount\nFY 2019-2020",
]
fy = detect_fy_columns(test_cols)
print("FY columns detected:")
for k, v in fy.items():
    print(f"  {repr(k)} -> {v}")

# parse_amount spaced-digit test
print("\nparse_amount tests:")
print("  '3 9 2 . 0 0' ->", parse_amount("3 9 2 . 0 0"))
print("  '8 8 4 8 . 0 0' ->", parse_amount("8 8 4 8 . 0 0"))
print("  '2 0' ->", parse_amount("2 0"))
print("  '1,23,456.78' ->", parse_amount("1,23,456.78"))

# Full normalize on page 1 of the real PDF
pdf_path = r"tests\sample_pdfs\iepf-unclaimed-dividend-and-corresponding-shares-data-fy-2017-18.pdf"
with pdfplumber.open(pdf_path) as pdf:
    table = pdf.pages[0].extract_table()

header = [str(h) if h else f"col_{i}" for i, h in enumerate(table[0])]
df_raw = pd.DataFrame(table[1:], columns=header)
print(f"\nRaw rows page 1: {len(df_raw)}")

result = normalize_dataframe(df_raw, "tech-mahindra", "test.pdf", "2017-18")
fy_cols = [c for c in result.columns if c.startswith("dividend_") or c.startswith("interim_") or c.startswith("final_")]
print(f"After normalize: {len(result)} records")
print(f"FY columns in output: {fy_cols}")
if not result.empty:
    print("\nFirst 3 records:")
    print(result[["folio_no", "name", "current_holding", "total_dividend"]].head(3).to_string())
