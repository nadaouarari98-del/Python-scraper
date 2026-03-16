"""
src/parser/normalizer.py
-------------------------
Normalize raw parsed DataFrames into the canonical shareholder schema.

Canonical columns
-----------------
folio_no, name, address, demat_account, pan_number, current_holding,
dividend_fy_* (one per FY detected), total_dividend,
company_name, source_file, year, parsed_at
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Synonym dictionaries — raw column name → canonical column name
# ---------------------------------------------------------------------------

# Each list contains lowercase substrings that, if found anywhere in a raw
# column header, map it to the canonical name.
_COLUMN_SYNONYMS: dict[str, list[str]] = {
    "folio_no": [
        "folio", "folio no", "folio number", "investor id",
        "register folio", "reg. folio",
    ],
    "name": [
        "name of share", "name of investor", "investor name",
        "shareholder name", "first holder", "name of the share",
        "^name$",
    ],
    "address": [
        "address", "registered address", "add.", "addr",
    ],
    "demat_account": [
        "demat", "dp id", "client id", "dp id-client", "dp-id",
        "beneficiary", "nsdl", "cdsl",
    ],
    "pan_number": [
        "pan", "pan no", "permanent account",
    ],
    "current_holding": [
        "current holding", "no. of shares", "no of shares",
        "number of shares", "share", "holding", "qty",
    ],
}

# Regex that identifies a dividend / FY amount column
_FY_PATTERN = re.compile(
    r"(?:dividend|div|amount|unpaid|unclaimed).*?(\d{4}[-–]\d{2,4})"
    r"|(\d{4}[-–]\d{2,4}).*?(?:dividend|div|amount|unpaid|unclaimed)",
    re.IGNORECASE,
)
# Simpler: any column containing a 4-digit year range
_YEAR_RANGE_RE = re.compile(r"(20\d{2})[-–](1[0-9]|20\d{2}|\d{2})")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def clean_name(raw: Any) -> str:
    """Return title-cased, whitespace-collapsed shareholder name.

    Removes corrupted PDF parsing codes that appear at the start of names
    through two patterns:
    1. Mixed digit-letter corruption: '4I8V7An' -> 'An', '3P3E2Ter' -> 'Ter'
    2. Pure digit prefixes: '332PETER' -> 'PETER'

    These corruptions occur when PDF extraction fails to separate folio number
    from shareholder name (e.g., 'IN30023911818332PETER').

    Args:
        raw: Raw value from PDF cell (string, float, None, …).

    Returns:
        Cleaned string, or empty string if unusable.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    
    text = str(raw).strip().upper()  # Normalise to ALL CAPS so case doesn't interfere
    words = text.split()
    if not words:
        return ""
    
    # Corruption detection: iteratively strip folio-number remnants from the
    # front of the name.  The PDF sometimes bleeds multiple chunks, e.g.
    # '12482 0SAVITHA' → first pass drops '12482', second pass sees '0SAVITHA'
    # and extracts 'SAVITHA'.  Loop until the leading word is clean (no digits).
    MAX_PASSES = 4
    for _ in range(MAX_PASSES):
        if not words:
            break
        first = words[0]
        if not any(c.isdigit() for c in first):
            break  # leading word is clean — done

        last_digit_pos = max(i for i, c in enumerate(first) if c.isdigit())
        name_fragment = first[last_digit_pos + 1:]  # letters after last digit

        if len(name_fragment) >= 2:
            # Meaningful partial real name fragment: '3P3E2TER' → 'TER'
            words = [name_fragment] + words[1:]
            break  # fragment is the start of the real name — stop here
        else:
            # Fragment too short (0 or 1 char) — drop the whole first word
            # e.g. '9K' → drop; '332' → drop; '0SAVITHA' will be re-evaluated
            words = words[1:]

    text = ' '.join(words).strip()
    text = re.sub(r"\s+", " ", text)
    return text.title()


def clean_address(lines: list[Any]) -> str:
    """Join a list of address fragment strings into a single clean line.

    Args:
        lines: List of raw address parts (may contain None / NaN).

    Returns:
        Single-line address string with normalised whitespace.
    """
    parts = []
    for line in lines:
        if line is None or (isinstance(line, float) and pd.isna(line)):
            continue
        part = re.sub(r"\s+", " ", str(line).strip())
        if part:
            parts.append(part)
    return ", ".join(parts)


def parse_amount(raw: Any) -> float:
    """Parse an Indian-format currency string to float.

    Handles commas, ₹ symbol, leading/trailing whitespace, empty strings,
    and spaced digits as produced by some PDF renderers (e.g. '3 9 2 . 0 0').

    Args:
        raw: Raw cell value.

    Returns:
        Float value, or 0.0 if the input is empty / unparseable.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return 0.0
    text = str(raw).strip()
    # Remove currency symbol and commas
    text = re.sub(r"[₹,]", "", text)
    # Collapse spaces that appear between digits/decimal points
    # e.g. '3 9 2 . 0 0' → '392.00'
    text = re.sub(r"(?<=\d) (?=[\d.])|(?<=[.]) (?=\d)", "", text)
    # Finally remove any remaining whitespace
    text = text.strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def split_name_parts(full_name: str) -> tuple[str, str, str, str]:
    """Split a full name into first, middle, last names and preserve full name.
    
    Handles Indian names with multiple parts. Logic:
    - full_name: keep as-is
    - last_name: take the last word
    - first_name: take the first word
    - middle_name: everything in between
    
    Args:
        full_name: Full name string (may be empty).
    
    Returns:
        Tuple of (first_name, middle_name, last_name, full_name)
    """
    if not full_name or not str(full_name).strip():
        return "", "", "", ""
    
    full_name = str(full_name).strip()
    words = full_name.split()
    
    if len(words) == 0:
        return "", "", "", ""
    elif len(words) == 1:
        return words[0], "", "", full_name
    elif len(words) == 2:
        return words[0], "", words[1], full_name
    else:
        first = words[0]
        last = words[-1]
        middle = " ".join(words[1:-1])
        return first, middle, last, full_name


def map_column_name(raw_col: str) -> str | None:
    """Map a raw PDF column header to a canonical column name.

    Args:
        raw_col: Raw column header string from the PDF table.

    Returns:
        Canonical column name if matched, otherwise ``None``.
    """
    lower = raw_col.lower().strip()
    for canonical, synonyms in _COLUMN_SYNONYMS.items():
        for syn in synonyms:
            # Support anchored regex like "^name$"
            if syn.startswith("^"):
                if re.match(syn, lower):
                    return canonical
            elif syn in lower:
                return canonical
    return None


def detect_fy_columns(columns: list[str]) -> dict[str, str]:
    """Identify dividend FY columns from a list of column headers.

    Handles multi-line headers (e.g. 'Final Dividend\\nAmount FY 2017-\\n2018')
    by flattening newlines before searching for year ranges.

    When two columns would produce the same FY label (e.g. 'Interim Dividend
    FY 2021-22' and 'Final Dividend FY 2021-22'), disambiguates by prepending
    a prefix derived from the column text ('interim_' / 'final_' etc.).

    Args:
        columns: List of raw column names.

    Returns:
        Dict mapping raw column name → normalised FY label like
        ``"dividend_fy_2017_18"``.
    """
    result: dict[str, str] = {}
    label_counts: dict[str, int] = {}  # track label collisions

    for col in columns:
        # Flatten newlines so '2017-\n2018' becomes '2017-2018'
        flat = re.sub(r"\s*\n\s*", "", str(col))
        m = _YEAR_RANGE_RE.search(flat)
        if m:
            start = m.group(1)
            end_raw = m.group(2)
            end = end_raw[-2:] if len(end_raw) >= 2 else end_raw
            base_label = f"dividend_fy_{start}_{end}"

            # Disambiguate collisions: derive prefix from column text
            col_lower = flat.lower()
            if "interim" in col_lower:
                prefix = "interim_"
            elif "final" in col_lower:
                prefix = "final_"
            elif "special" in col_lower:
                prefix = "special_"
            else:
                prefix = ""

            label = f"{prefix}{base_label}"

            # If still colliding, append a numeric suffix
            if label in label_counts.values() or list(result.values()).count(label) > 0:
                existing_count = list(result.values()).count(label)
                label = f"{label}_{existing_count + 1}"

            result[col] = label

    return result


def extract_year_from_filename(filename: str) -> str:
    """Extract a financial year string from a PDF filename.

    Args:
        filename: Bare filename or full path string.

    Returns:
        Year string like ``"2017-18"`` or ``"unknown"``.
    """
    m = _YEAR_RANGE_RE.search(filename)
    if m:
        start = m.group(1)
        end = m.group(2)[-2:]
        return f"{start}-{end}"
    # Fallback: just a 4-digit year
    m2 = re.search(r"(20\d{2})", filename)
    return m2.group(1) if m2 else "unknown"


def extract_company_from_filename(filename: str) -> str:
    """Derive a clean company slug from the PDF filename.

    Args:
        filename: Bare filename (without directory).

    Returns:
        Lowercase, hyphen-separated slug or a descriptive name for IEPF files.
    """
    # Recognize specific known PDFs
    if 'techmahindra' in filename.lower():
        return 'Tech Mahindra'
    
    # For IEPF/dividend PDFs, extract a meaningful label from the filename
    # e.g. 'iepf-unclaimed-dividend-and-corresponding-shares-data-fy-2017-18.pdf'
    #   -> 'IEPF Unclaimed Dividend 2017-18'
    if any(keyword in filename.lower() for keyword in ['iepf', 'unclaimed', 'unpaid']):
        stem = re.sub(r"\.[pP][dD][fF]$", "", filename)
        
        # Extract year/FY if present
        year_match = re.search(r"(\d{4}[-–]\d{2,4}|\d{4})", stem)
        year_str = ""
        if year_match:
            year_str = f" {year_match.group(1)}"
        
        # Categorize based on keywords
        if 'unclaimed' in stem.lower() or 'unpaid' in stem.lower():
            return f"IEPF Unclaimed Dividend{year_str}".strip()
        else:
            return f"IEPF Dividend{year_str}".strip()
    
    # Strip date/year/FY tokens for non-IEPF PDFs
    stem = re.sub(r"\.[pP][dD][fF]$", "", filename)
    stem = re.sub(r"(fy[-_]?\d{4}[-–]\d{2,4}|\d{4}[-–]\d{2,4}|20\d{2})", "",
                  stem, flags=re.IGNORECASE)
    # Strip common suffixes and payment-related keywords
    stem = re.sub(
        r"[-_]?(iepf|unclaimed|dividend|shareholder|shareholding|pattern|data|report|pmt|trf|payout|payment|transfer)[-_]?",
        " ", stem, flags=re.IGNORECASE,
    )
    stem = re.sub(r"[-_]+", " ", stem).strip()
    slug = re.sub(r"[^a-z0-9 ]+", "", stem.lower())
    slug = re.sub(r"\s+", "-", slug.strip())
    
    # If slug is too short or mostly empty, return descriptive generic label
    if not slug or len(slug) < 3:
        return 'Generic Shareholder Data'
    
    return slug.title()


# ---------------------------------------------------------------------------
# Core normalizer
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = [
    "folio_no", "name", "address", "demat_account", "pan_number",
    "current_holding", "total_dividend", "company_name",
    "source_file", "year", "parsed_at",
]


def normalize_dataframe(
    df: pd.DataFrame,
    company_name: str,
    source_file: str,
    year: str,
) -> pd.DataFrame:
    """Transform a raw parsed DataFrame into the canonical schema.

    Args:
        df:           Raw DataFrame from any extractor.
        company_name: Company slug (used for the ``company_name`` column).
        source_file:  Original PDF filename.
        year:         Financial year string.

    Returns:
        Normalized :class:`pd.DataFrame` with canonical columns.
        Returns an empty DataFrame with correct columns if input is empty.
    """
    if df is None or df.empty:
        return _empty_canonical_df()

    # --- 1. Map raw column names to canonical ones ---
    col_map: dict[str, str] = {}

    for raw_col in df.columns:
        canonical = map_column_name(str(raw_col))
        if canonical:
            # Avoid mapping two different raw cols to the same canonical name
            # (e.g. two columns both mapping to 'name') — keep first match
            if canonical not in col_map.values():
                col_map[raw_col] = canonical

    # FY / dividend columns (not caught by synonym dict)
    remaining = [c for c in df.columns if c not in col_map]
    fy_col_map = detect_fy_columns(remaining)
    col_map.update(fy_col_map)

    # Safe rename: if pandas would produce duplicate column names, skip the rename
    # for the later occurrence to avoid KeyError downstream
    seen: set[str] = set()
    safe_col_map: dict[str, str] = {}
    unmapped_cols = []  # Track columns that don't map to canonical names
    
    for raw, mapped in col_map.items():
        if mapped not in seen:
            safe_col_map[raw] = mapped
            seen.add(mapped)
        # else: leave column with its original name to preserve it

    df = df.rename(columns=safe_col_map)

    # --- 2. Drop ONLY completely empty or purely unnamed columns ---
    # REQUIREMENT 1: Never drop columns from PDFs — preserve everything
    unnamed_re = re.compile(r"^unnamed|^col_?\d+$", re.IGNORECASE)
    
    # Mark columns for keeping: anything not matching unnamed pattern
    cols_to_keep = []
    for col in df.columns:
        # Skip only truly unnamed/unnamed columns
        if not unnamed_re.match(str(col)):
            cols_to_keep.append(col)
        # But keep non-empty unnamed columns
        elif not df[col].dropna().empty or (df[col] != "").any():
            cols_to_keep.append(col)
    
    df = df[cols_to_keep]
    df = df.dropna(axis=1, how="all")  # Only drop if entirely NaN

    # --- 3. Remove header-echo rows (row where folio_no == 'Folio No' etc.) ---
    if "folio_no" in df.columns:
        mask = df["folio_no"].astype(str).str.lower().str.strip().isin(
            {"folio no", "folio number", "investor id", "sr no", "sr.", "no."}
        )
        df = df[~mask]

    # --- 4. Fill missing canonical columns with defaults ---
    for col in ["folio_no", "name", "address", "demat_account", "pan_number"]:
        if col not in df.columns:
            df[col] = ""

    # --- 5. Clean string fields and split name into components ---
    df["name"] = df["name"].apply(clean_name)
    df["folio_no"] = df["folio_no"].apply(
        lambda x: re.sub(r"\s+", "", str(x)) if pd.notna(x) else ""
    )
    df["address"] = df["address"].apply(
        lambda x: re.sub(r"\s+", " ", str(x)).strip() if pd.notna(x) else ""
    )
    df["demat_account"] = df["demat_account"].apply(
        lambda x: str(x).strip() if pd.notna(x) else ""
    )
    df["pan_number"] = df["pan_number"].apply(
        lambda x: str(x).strip().upper() if pd.notna(x) else ""
    )
    
    # REQUIREMENT 2: Split name into first_name, middle_name, last_name, full_name
    name_split = df["name"].apply(split_name_parts)
    df["first_name"] = name_split.apply(lambda x: x[0])
    df["middle_name"] = name_split.apply(lambda x: x[1])
    df["last_name"] = name_split.apply(lambda x: x[2])
    df["full_name"] = name_split.apply(lambda x: x[3])
    
    # Add father/husband name placeholder (empty for now, can be enriched later)
    df["father_husband_name"] = ""
    
    # Add address line columns placeholder (can be enriched later)
    if "address_line1" not in df.columns:
        df["address_line1"] = df["address"]
    if "address_line2" not in df.columns:
        df["address_line2"] = ""
    
    # Add share_type placeholder (can be enriched from PDF headers)
    if "share_type" not in df.columns:
        df["share_type"] = ""

    # --- 6. Numeric fields ---
    if "current_holding" in df.columns:
        df["current_holding"] = (
            df["current_holding"]
            .apply(lambda x: int(parse_amount(x)) if x != "" else 0)
        )
    else:
        df["current_holding"] = 0

    # --- 7. FY dividend columns → float ---
    fy_labels = [c for c in df.columns if c.startswith("dividend_fy_")]
    for col in fy_labels:
        df[col] = df[col].apply(lambda x: parse_amount(x) if pd.notna(x) and x != "" else 0.0)

    # --- 8. total_dividend ---
    if fy_labels:
        df["total_dividend"] = df[fy_labels].sum(axis=1)
    elif "total_dividend" not in df.columns:
        df["total_dividend"] = 0.0

    # --- 9. REQUIREMENT 1: Replace all NaN with empty string (text cols) or 0 (numeric cols) ---
    for col in df.columns:
        if df[col].dtype == 'object':
            # Text column: replace NaN with empty string
            df[col] = df[col].apply(lambda x: "" if (pd.isna(x) or x is None) else x)
        elif 'float' in str(df[col].dtype) or 'int' in str(df[col].dtype):
            # Numeric column: replace NaN with 0
            df[col] = df[col].fillna(0)
    
    # Metadata columns ---
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["company_name"] = company_name
    df["source_file"] = source_file
    df["year"] = year
    df["parsed_at"] = now_str

    # --- 10. Drop rows with no useful data ---
    df = df[
        df["folio_no"].str.strip().astype(bool)
        | df["name"].str.strip().astype(bool)
    ]

    df = df.reset_index(drop=True)
    
    # REQUIREMENT 1: Split name column into components
    df = split_name_column(df, name_col="name")
    
    return df


def split_name_column(df, name_col="name"):
    """Split full name into first, middle, last name components."""
    if name_col not in df.columns:
        return df
    df["full_name"] = df[name_col].astype(str).str.strip()
    parts = df["full_name"].str.split(r"\s+", expand=True)
    df["first_name"] = parts[0].fillna("")
    df["last_name"] = parts.apply(lambda r: r[r.last_valid_index()] if r.last_valid_index() and r.last_valid_index() > 0 else "", axis=1)
    df["middle_name"] = parts.apply(lambda r: " ".join(r[1:r.last_valid_index()].dropna()) if r.last_valid_index() and r.last_valid_index() > 1 else "", axis=1)
    df["father_husband_name"] = ""
    return df


def _empty_canonical_df() -> pd.DataFrame:
    """Return an empty DataFrame with all required canonical columns."""
    return pd.DataFrame(
        columns=_REQUIRED_COLUMNS + ["current_holding", "total_dividend"]
    )


# ---------------------------------------------------------------------------
# Unified Master Schema
# ---------------------------------------------------------------------------

UNIFIED_MASTER_COLUMNS = [
    "sr_no",
    "folio_no",
    "name",
    "address",
    "state",
    "pincode",
    "country",
    "current_holding",
    "dividend_fy_2017_18",
    "dividend_fy_2018_19",
    "dividend_fy_2019_20",
    "dividend_fy_2020_21",
    "dividend_fy_2021_22",
    "dividend_fy_2022_23",
    "dividend_fy_2023_24",
    "total_dividend",
    "company_name",
    "source_pdf",
    "year",
    "date_processed",
]


def normalize_to_unified_master(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Transform a canonical DataFrame to unified master schema.
    
    The unified master schema has fixed columns in a consistent order,
    with all dividend FY columns explicitly listed. Missing columns are
    filled with empty strings or 0 values as appropriate.
    
    Args:
        df: DataFrame with canonical columns from normalize_dataframe().
        
    Returns:
        DataFrame with exactly the UNIFIED_MASTER_COLUMNS in that order.
    """
    # Reset index to ensure consistent indexing
    df = df.reset_index(drop=True)
    
    result = {}
    
    # Serial number
    result["sr_no"] = list(range(1, len(df) + 1))
    
    # Core identifier columns
    folio_series = df.get("folio_no", pd.Series([""] * len(df), index=df.index))
    result["folio_no"] = folio_series.fillna("").astype(str).str.strip().tolist()
    
    # Merge first/middle/last name if available
    name_series = _merge_name_columns(df)
    # Explicitly convert NaN to empty strings before tolist()
    name_list = name_series.fillna("").astype(str).str.strip().tolist()
    result["name"] = name_list
    
    # Address and location — try multiple column name variants
    address_series = df.get("address", pd.Series([""] * len(df), index=df.index))
    result["address"] = address_series.fillna("").astype(str).str.strip().tolist()
    
    # State: try 'State', 'state'
    state_series = None
    for col_name in ["State", "state"]:
        if col_name in df.columns:
            state_series = df[col_name]
            break
    result["state"] = state_series.fillna("").astype(str).str.strip().tolist() if state_series is not None else [""] * len(df)
    
    # Pincode: try 'PINCode', 'pincode'
    pincode_series = None
    for col_name in ["PINCode", "pincode"]:
        if col_name in df.columns:
            pincode_series = df[col_name]
            break
    result["pincode"] = pincode_series.fillna("").astype(str).str.strip().tolist() if pincode_series is not None else [""] * len(df)
    
    # Country: try 'Country', 'country'
    country_series = None
    for col_name in ["Country", "country"]:
        if col_name in df.columns:
            country_series = df[col_name]
            break
    result["country"] = country_series.fillna("").astype(str).str.strip().tolist() if country_series is not None else [""] * len(df)
    
    # Holdings
    holding_series = df.get("current_holding", pd.Series([0] * len(df), index=df.index))
    result["current_holding"] = holding_series.fillna(0).astype(int).tolist()
    
    # Dividend columns — need to consolidate interim/final variants and sum for total
    # Map FY year labels to target column name
    fy_map = {
        "dividend_fy_2017_18": ["dividend_fy_2017_18", "final_dividend_fy_2017_18", "interim_dividend_fy_2017_18"],
        "dividend_fy_2018_19": ["dividend_fy_2018_19", "final_dividend_fy_2018_19", "interim_dividend_fy_2018_19"],
        "dividend_fy_2019_20": ["dividend_fy_2019_20", "final_dividend_fy_2019_20", "interim_dividend_fy_2019_20"],
        "dividend_fy_2020_21": ["dividend_fy_2020_21", "final_dividend_fy_2020_21", "interim_dividend_fy_2020_21"],
        "dividend_fy_2021_22": ["dividend_fy_2021_22", "final_dividend_fy_2021_22", "interim_dividend_fy_2021_22"],
        "dividend_fy_2022_23": ["dividend_fy_2022_23", "final_dividend_fy_2022_23", "interim_dividend_fy_2022_23"],
        "dividend_fy_2023_24": ["dividend_fy_2023_24", "final_dividend_fy_2023_24", "interim_dividend_fy_2023_24"],
    }
    
    fy_dividend_totals = []  # Track totals for each row to calculate total_dividend
    
    for unified_col, source_cols in fy_map.items():
        combined_values = []
        for idx in range(len(df)):
            val = 0
            # Try each possible source column in order (prefer exact match, then final, then interim)
            for src_col in source_cols:
                if src_col in df.columns and pd.notna(df[src_col].iloc[idx]):
                    try:
                        # Remove spaces from numbers like "2 8 0 . 0 0"
                        numeric_val = float(str(df[src_col].iloc[idx]).replace(" ", "").replace(",", ""))
                        if numeric_val > 0:
                            val = numeric_val
                            break
                    except (ValueError, TypeError):
                        pass
            combined_values.append(int(val) if val == int(val) else val)
        result[unified_col] = combined_values
    
    # Calculate total_dividend as sum of all FY dividend columns
    # For Unpaid Dividend PDF, also check for "Amount Due (in Rs.)" column
    total_dividend_list = []
    for idx in range(len(df)):
        total_div = 0
        
        # Sum all FY dividend columns
        for fy_col in [c for c in result.keys() if c.startswith("dividend_fy_")]:
            val = result[fy_col][idx]
            if val and val > 0:
                total_div += val
        
        # For Unpaid Dividend PDF, use "Amount Due (in Rs.)" if no FY dividends
        if total_div == 0 and "Amount Due\n(in Rs.)" in df.columns:
            try:
                amount_val = float(str(df["Amount Due\n(in Rs.)"].iloc[idx]).replace(" ", "").replace(",", ""))
                if amount_val > 0:
                    total_div = amount_val
            except (ValueError, TypeError):
                pass
        
        total_dividend_list.append(int(total_div) if total_div == int(total_div) else total_div)
    
    result["total_dividend"] = total_dividend_list
    
    # Metadata columns
    company_series = df.get("company_name", pd.Series(["unknown"] * len(df), index=df.index))
    result["company_name"] = company_series.fillna("unknown").astype(str).tolist()
    
    source_series = df.get("source_file", pd.Series([""] * len(df), index=df.index))
    result["source_pdf"] = source_series.fillna("").astype(str).tolist()
    
    year_series = df.get("year", pd.Series([""] * len(df), index=df.index))
    result["year"] = year_series.fillna("").astype(str).tolist()
    
    parsed_series = df.get("parsed_at", pd.Series([""] * len(df), index=df.index))
    result["date_processed"] = parsed_series.fillna("").astype(str).tolist()
    
    # Convert dict to DataFrame with columns in correct order
    unified_df = pd.DataFrame(result)
    unified_df = unified_df[UNIFIED_MASTER_COLUMNS]
    
    return unified_df


def _merge_name_columns(df: pd.DataFrame) -> pd.Series:
    """Merge first/middle/last name columns into a single name field.
    
    Handles both:
    - Unpaid Dividend format: First Name + Middle Name + Last Name
    - IEPF format: single 'name' column
    - Tata Finance format: 1st Holder Name
    
    Args:
        df: DataFrame that may have name-related columns.
        
    Returns:
        Series with merged full names.
    """
    # Check if we have explicit name fields (Unpaid Dividend format)
    # but only if they have actual data
    if "First Name" in df.columns:
        # Check if any First Name values are non-empty
        first_name_filled = df["First Name"].fillna("").astype(str).str.strip().astype(bool).any()
        
        if first_name_filled:
            parts = []
            for col in ["First Name", "Middle Name", "Last Name"]:
                if col in df.columns:
                    part = df[col].fillna("").astype(str).str.strip()
                    parts.append(part)
            
            # Join non-empty parts with space
            def merge_parts(row_parts):
                return " ".join([p for p in row_parts if p]).strip()
            
            # Transpose and apply merge
            merged_list = []
            for idx in range(len(df)):
                row_vals = [parts[i].iloc[idx] if i < len(parts) else "" for i in range(len(parts))]
                merged_list.append(merge_parts(row_vals))
            
            result = pd.Series(merged_list, index=df.index).str.replace(r'\s+', ' ', regex=True).str.strip()
            # If result is all empty, continue to check other name columns
            if not result.astype(bool).any():
                pass  # Continue to next check
            else:
                return result
    
    # Check for 1st Holder Name (Tata Finance format)
    if "1st Holder Name" in df.columns:
        holder_names = df["1st Holder Name"].fillna("").astype(str).str.strip()
        if holder_names.astype(bool).any():
            return holder_names
    
    # Otherwise use single name column
    return df.get("name", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
