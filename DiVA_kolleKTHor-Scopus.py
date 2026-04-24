#!/usr/bin/env python3
import time
import re
import requests
import pandas as pd
from tqdm import tqdm  # pip install tqdm
from urllib.parse import quote
from datetime import datetime

# -------------------- CONFIG --------------------

FROM_YEAR = 2001
TO_YEAR = 2002

# which DiVA portal to use: e.g. "kth", "uu", "umu", "lnu", etc.
DIVA_PORTAL = "kth"
DIVA_BASE = f"https://{DIVA_PORTAL}.diva-portal.org/smash/export.jsf"

# We only care about records with missing Scopus IDs (EID we will fill)
MISSING_SCOPUS_ONLY = True  # rows with empty ScopusId

# Scopus Search API
SCOPUS_BASE = "https://api.elsevier.com/content/search/scopus"
SCOPUS_API_KEY = ""  # put your API key here or inject from env/CLI later
SCOPUS_VIEW = "STANDARD"  # or "COMPLETE" if you need more metadata
SCOPUS_COUNT = 5
SLEEP_SECONDS = 1.0

# Matching
SIM_THRESHOLD = 0.9
MAX_ACCEPTED = 9999

# Filenames: portal + year range (+ timestamp for outputs)
TIMESTAMP = datetime.now().strftime("%Y%m%d-%H%M%S")
PREFIX = f"{DIVA_PORTAL}_{FROM_YEAR}-{TO_YEAR}"

DOWNLOADED_CSV = f"{PREFIX}_diva_raw.csv"                            # input snapshot
OUTPUT_CSV = f"{PREFIX}_diva_scopus_eid_candidates_{TIMESTAMP}.csv"   # output with timestamp
EXCEL_OUT = f"{PREFIX}_diva_scopus_eid_candidates_{TIMESTAMP}.xlsx"   # output with timestamp

ISBN_RE = re.compile(r"\b(?:97[89][- ]?)?\d[-\d ]{8,}\d\b")


# -------------------- HELPERS --------------------


def build_diva_url(from_year: int, to_year: int) -> str:
    aq = f'[[{{"dateIssued":{{"from":"{from_year}","to":"{to_year}"}}}}]]'
    aq2 = (
        '[[{"publicationTypeCode":["bookReview","review","article","book",'
        '"chapter","conferencePaper"]}]]'
    )

    params = {
        "format": "csv",
        "addFilename": "true",
        "aq": aq,
        "aqe": "[]",
        "aq2": aq2,
        "onlyFullText": "false",
        "noOfRows": "99999",
        "sortOrder": "title_sort_asc",
        "sortOrder2": "title_sort_asc",
        "csvType": "publication",
        "fl": (
            "PID,ArticleId,DOI,EndPage,ISBN,ISBN_ELECTRONIC,ISBN_PRINT,ISBN_UNDEFINED,"
            "ISI,Issue,Journal,JournalEISSN,JournalISSN,Pages,PublicationType,PMID,"
            "ScopusId,SeriesEISSN,SeriesISSN,StartPage,Title,Name,Volume,Year,Notes"
        ),
    }

    encoded = [f"{k}={quote(v, safe='')}" for k, v in params.items()]
    return DIVA_BASE + "?" + "&".join(encoded)


def download_diva_csv(url: str, out_path: str):
    print(f"Downloading DiVA CSV from {url}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        )
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print(f"Saved DiVA CSV to {out_path}")


def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = "".join(ch for ch in s if ch.isprintable())
    return s.strip()


def normalize_title(t: str) -> list[str]:
    t = clean_text(t).lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return [tok for tok in t.split() if tok]


def title_similarity(a: str, b: str) -> float:
    ta = set(normalize_title(a))
    tb = set(normalize_title(b))
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union


def normalize_page(page_str: str) -> str:
    if not page_str:
        return ""
    page_str = str(page_str).strip()
    if page_str.isdigit():
        return str(int(page_str))
    return page_str


def norm_issn(s: str) -> str:
    s = (s or "").strip()
    return s.replace("-", "")


def norm_isbn(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9Xx]", "", s)
    return s.upper()


# ---- Publication type mapping ----


def diva_pubtype_category(diva_type: str) -> str | None:
    t = (diva_type or "").strip().lower()

    if t in {
        "article",
        "article in journal",
        "review",
        "bookreview",
        "book review",
    }:
        return "article"

    if t in {
        "conferencepaper",
        "conference paper",
        "paper in conference proceeding",
        "paper in conference proceedings",
    }:
        return "conference"

    if t in {
        "chapter",
        "chapter in book",
        "chapter in anthology",
    }:
        return "chapter"

    if t in {
        "book",
        "monograph",
    }:
        return "book"

    if t == "article":
        return "article"
    if t == "conferencepaper":
        return "conference"
    if t == "book":
        return "book"
    if t == "chapter":
        return "chapter"

    return None


def scopus_document_type_category(entry: dict) -> str | None:
    """
    Map Scopus subtype/subtypeDescription/aggregationType to our coarse categories.
    """
    subtype_desc = (entry.get("subtypeDescription") or "").strip().lower()
    agg = (entry.get("prism:aggregationType") or "").strip().lower()
    subtype = (entry.get("subtype") or "").strip().lower()

    # Conference paper
    if "conference" in subtype_desc or subtype in {"cp"}:
        return "conference"

    # Articles / reviews in journals
    if agg == "journal":
        if subtype in {"ar", "re"} or "article" in subtype_desc or "review" in subtype_desc:
            return "article"

    # Book / book chapter (Scopus may mark aggregationType=book)
    if agg == "book":
        if "chapter" in subtype_desc:
            return "chapter"
        return "book"

    return None


# ---- Author helpers ----


def extract_diva_author_names(raw: str) -> list[str]:
    if not raw:
        return []
    authors: list[str] = []
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        part = re.split(r"\s\(", part, maxsplit=1)[0]
        part = re.sub(r"\[[^\]]*\]", "", part).strip()
        part = re.sub(r"\s+", " ", part)
        if part:
            authors.append(part)
    return authors


def extract_diva_authors(row) -> set[str]:
    raw = (row.get("Name", "") or "").strip()
    names = extract_diva_author_names(raw)
    surnames: set[str] = set()
    for n in names:
        fam = n.split(",", 1)[0].strip().lower()
        if fam:
            surnames.add(fam)
    return surnames


def extract_scopus_authors(entry: dict) -> set[str]:
    """
    Scopus Search 'entry' may contain author info in 'author' list (for some views),
    or only dc:creator as string; here we use dc:creator (first author) if that's all we have.
    """
    names: set[str] = set()
    creator = (entry.get("dc:creator") or "").strip()
    if creator:
        # creator is usually "Surname I."
        clean = re.sub(r"\s+", " ", creator)
        parts = clean.split()
        if parts:
            fam = parts[0].strip().strip(",").lower()
            if fam:
                names.add(fam)

    # If author list present (in some views), use it too
    authors = entry.get("author") or []
    for a in authors:
        if not isinstance(a, dict):
            continue
        fam = (a.get("surname") or "").strip().lower()
        if fam:
            names.add(fam)

    return names


def authors_match(diva_row, entry: dict) -> bool:
    diva_auth = extract_diva_authors(diva_row)
    scopus_auth = extract_scopus_authors(entry)

    if not diva_auth or not scopus_auth:
        print(" ⚠ Missing authors on one side; skipping author check")
        return False

    inter = diva_auth & scopus_auth
    print(f" DiVA authors: {sorted(diva_auth)}")
    print(f" Scopus authors: {sorted(scopus_auth)}")
    print(f" Author intersection: {sorted(inter)}")
    return bool(inter)


# ---- Host ISBN helpers for conference/chapter ----


def extract_host_isbns(row) -> set[str]:
    candidates: list[str] = []

    for col in ["ISBN", "ISBN_PRINT", "ISBN_ELECTRONIC"]:
        v = (row.get(col, "") or "").strip()
        if v:
            candidates.append(v)

    notes = (row.get("Notes", "") or "")
    for match in ISBN_RE.findall(notes):
        candidates.append(match)

    norm = {norm_isbn(c) for c in candidates if c}
    return {x for x in norm if len(x) >= 10}


def extract_diva_book_isbns(row) -> set[str]:
    candidates: list[str] = []
    for col in ["ISBN", "ISBN_PRINT", "ISBN_ELECTRONIC"]:
        v = (row.get(col, "") or "").strip()
        if v:
            candidates.append(v)
    norm = {norm_isbn(c) for c in candidates if c}
    return {x for x in norm if len(x) >= 10}


def extract_scopus_isbns(entry: dict) -> set[str]:
    """
    ISBNs are not usually present in Scopus Search results, but if they appear
    (e.g. as prism:isbn array), normalize them here.
    """
    candidates: list[str] = []
    isbns = entry.get("prism:isbn")
    if isinstance(isbns, list):
        candidates.extend(str(x) for x in isbns if x)
    elif isbns:
        candidates.append(str(isbns))

    norm = {norm_isbn(s or "") for s in candidates}
    return {x for x in norm if len(x) >= 10}


# ---- Scopus helpers ----


def scopus_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "X-ELS-APIKey": SCOPUS_API_KEY,
    }


def scopus_get(params: dict) -> dict:
    r = requests.get(SCOPUS_BASE, headers=scopus_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json() or {}


def search_scopus(query: str, count: int = 5, start: int = 0) -> list[dict]:
    params = {
        "query": query,
        "count": str(count),
        "start": str(start),
        "view": SCOPUS_VIEW,
    }
    data = scopus_get(params)
    # entries live under "search-results" -> "entry"
    sr = data.get("search-results") or {}
    entries = sr.get("entry") or []
    if isinstance(entries, dict):
        entries = [entries]
    return entries


def search_scopus_doi(doi: str, count: int = 5):
    q = f"doi({doi.strip()})"
    entries = search_scopus(q, count=count, start=0)
    return q, entries


def search_scopus_title(title: str, year: int | None = None, max_results: int = 5):
    # TITLE() + PUBYEAR logic; you can tweak to TITLE-ABS-KEY if needed
    base = f'TITLE("{clean_text(title)}")'
    if year is not None:
        base += f" AND PUBYEAR = {year}"
    q = base
    entries = search_scopus(q, count=max_results, start=0)
    return q, entries


def extract_scopus_title(entry: dict) -> str:
    return clean_text(entry.get("dc:title", "") or "")


def extract_scopus_year(entry: dict) -> int | None:
    # prism:coverDate is YYYY-MM-DD
    date_str = (entry.get("prism:coverDate") or "").strip()
    if date_str:
        try:
            return int(date_str.split("-")[0])
        except Exception:
            pass
    # fall back to prism:publicationName year isn't directly available; skip if missing
    return None


def extract_scopus_eid(entry: dict) -> str:
    return (entry.get("eid", "") or "").strip()


def extract_scopus_doi(entry: dict) -> str:
    return (entry.get("prism:doi", "") or "").strip()


def extract_scopus_biblio(entry: dict) -> dict:
    volume = entry.get("prism:volume", "") or ""
    issue = entry.get("prism:issueIdentifier", "") or ""

    start_page = ""
    end_page = ""
    page_range = entry.get("prism:pageRange") or ""

    if isinstance(page_range, str) and page_range:
        if "-" in page_range:
            parts = page_range.split("-", 1)
            start_page = parts[0].strip()
            if len(parts) > 1:
                end_page = parts[1].strip()
        else:
            start_page = page_range.strip()

    issn_candidates: list[str] = []
    for key in ["prism:issn", "prism:eIssn"]:
        v = entry.get(key)
        if isinstance(v, list):
            issn_candidates.extend(str(x) for x in v if x)
        elif v:
            issn_candidates.append(str(v))

    issn_set = {norm_issn(x) for x in issn_candidates if norm_issn(x)}
    container_title = clean_text(entry.get("prism:publicationName", "") or "")

    return {
        "volume": normalize_page(volume),
        "issue": normalize_page(issue),
        "start_page": normalize_page(start_page),
        "end_page": normalize_page(end_page),
        "issns": issn_set,
        "container_title": container_title,
    }


def issn_match(diva_row, scopus_biblio: dict) -> bool:
    diva_issns = {
        norm_issn(diva_row.get(col, ""))
        for col in ["JournalISSN", "JournalEISSN", "SeriesISSN", "SeriesEISSN"]
        if norm_issn(diva_row.get(col, ""))
    }

    scopus_issns = scopus_biblio.get("issns", set()) or set()

    if not diva_issns or not scopus_issns:
        print(" ⚠ Missing ISSN on one side; cannot ISSN-match")
        return False

    inter = diva_issns & scopus_issns
    print(f" DiVA ISSNs: {sorted(diva_issns)}")
    print(f" Scopus ISSNs: {sorted(scopus_issns)}")
    print(f" ISSN intersection: {sorted(inter)}")
    return bool(inter)


def bibliographic_match(diva_row, scopus_biblio: dict) -> bool:
    diva_volume = normalize_page(diva_row.get("Volume", ""))
    diva_issue = normalize_page(diva_row.get("Issue", ""))
    diva_start = normalize_page(diva_row.get("StartPage", ""))
    diva_end = normalize_page(diva_row.get("EndPage", ""))

    sc_volume = scopus_biblio.get("volume", "")
    sc_issue = scopus_biblio.get("issue", "")
    sc_start = scopus_biblio.get("start_page", "")
    sc_end = scopus_biblio.get("end_page", "")

    checks = []

    if diva_volume and sc_volume:
        checks.append(("Volume", diva_volume == sc_volume, diva_volume, sc_volume))
    if diva_issue and sc_issue:
        checks.append(("Issue", diva_issue == sc_issue, diva_issue, sc_issue))
    if diva_start and sc_start:
        checks.append(("StartPage", diva_start == sc_start, diva_start, sc_start))
    if diva_end and sc_end:
        checks.append(("EndPage", diva_end == sc_end, diva_end, sc_end))

    for field, matches, diva_val, sc_val in checks:
        status = "✓" if matches else "✗"
        print(f" {status} {field}: DiVA='{diva_val}' vs Scopus='{sc_val}'")

    if not checks:
        print(" ⚠ No bibliographic fields to compare")
        return False

    return all(check[1] for check in checks)


def make_doi_url(doi: str) -> str:
    doi = (doi or "").strip()
    if not doi:
        return ""
    return f"https://doi.org/{doi}"


def make_pid_url(pid: str) -> str:
    pid = (pid or "").strip()
    if not pid:
        return ""
    if pid.isdigit():
        pid_value = f"diva2:{pid}"
    else:
        pid_value = pid
    encoded_pid = quote(pid_value, safe="")
    return f"https://{DIVA_PORTAL}.diva-portal.org/smash/record.jsf?pid={encoded_pid}"


def make_scopus_eid_url(eid: str) -> str:
    eid = (eid or "").strip()
    if not eid:
        return ""
    encoded_eid = quote(eid, safe="")
    return f"https://www.scopus.com/record/display.uri?eid={encoded_eid}&origin=resultslist"


# -------------------- MAIN --------------------


def main():
    if not SCOPUS_API_KEY.strip():
        raise ValueError("Please set SCOPUS_API_KEY before running the script")

    url = build_diva_url(FROM_YEAR, TO_YEAR)
    download_diva_csv(url, DOWNLOADED_CSV)

    df = pd.read_csv(DOWNLOADED_CSV, dtype=str).fillna("")
    df["ScopusId"] = df["ScopusId"].astype(str).str.strip()
    df["DOI"] = df["DOI"].astype(str).str.strip()
    df["Title"] = df["Title"].apply(clean_text)

    for col in [
        "Possible_Scopus_EID",
        "Verified_Scopus_EID",
        "Possible_Scopus_DOI",
        "Verified_Scopus_DOI",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "Scopus_Query",
        "Scopus_Match_Method",
    ]:
        if col not in df.columns:
            df[col] = ""

    def to_int_or_none(s: str):
        try:
            return int(str(s).strip())
        except Exception:
            return None

    year_int = df["Year"].apply(to_int_or_none)
    year_mask = year_int.between(FROM_YEAR, TO_YEAR, inclusive="both")
    df = df[year_mask].copy()
    print(f"After Year filter {FROM_YEAR}-{TO_YEAR}: {len(df)} rows")

    exclude_titles = {"foreword", "preface"}
    df = df[~df["Title"].str.strip().str.lower().isin(exclude_titles)].copy()
    print(f"After excluding Foreword/Preface: {len(df)} rows")

    missing_scopus_mask = df["ScopusId"].str.strip() == ""
    if MISSING_SCOPUS_ONLY:
        working_mask = missing_scopus_mask
    else:
        raise ValueError("This script is intended to run with MISSING_SCOPUS_ONLY=True only")

    working_mask &= (df["Title"].str.strip() != "") & (df["Year"].str.strip() != "")
    df_work = df[working_mask].copy()
    print(f"Working rows (missing ScopusId): {len(df_work)}")

    accepted_count = 0

    # ---- ROUND 1: DOI -> Scopus EID ----
    round1_mask = df_work["DOI"].str.strip() != ""
    round1_index = list(df_work[round1_mask].index)
    print(f"Round 1 rows (DOI present, missing ScopusId): {len(round1_index)}")

    for idx in tqdm(round1_index, desc="Round 1 DOI -> Scopus"):
        if accepted_count >= MAX_ACCEPTED:
            print(f"\nReached MAX_ACCEPTED={MAX_ACCEPTED}, stopping early.")
            break

        try:
            row = df_work.loc[idx]
            pid = row["PID"].strip()
            doi = row["DOI"].strip()
            title = row["Title"].strip()
            year_str = row["Year"].strip()

            try:
                pub_year = int(year_str)
            except Exception:
                pub_year = None

            print(f"\n[ROUND1 {idx}] PID={pid}")
            print(f" Title: '{title}'")
            print(f" DOI: {doi}")
            print(f" Year: {pub_year}")
            print(" -> querying Scopus by DOI...")

            try:
                query_used, entries = search_scopus_doi(doi, count=SCOPUS_COUNT)
            except Exception as e:
                print(f" ERROR querying Scopus: {e}")
                time.sleep(SLEEP_SECONDS)
                continue

            df_work.at[idx, "Scopus_Query"] = query_used
            df_work.at[idx, "Scopus_Match_Method"] = "doi_round"

            if not entries:
                print(" No DOI hits found in Scopus")
                time.sleep(SLEEP_SECONDS)
                continue

            best_eid = None
            best_doi = None
            best_title_ok = False
            best_year_ok = False

            for entry in entries:
                eid = extract_scopus_eid(entry)
                hit_doi = extract_scopus_doi(entry)
                hit_title = extract_scopus_title(entry)
                hit_year = extract_scopus_year(entry)
                sim = title_similarity(title, hit_title)
                year_ok = (pub_year is not None and hit_year == pub_year)
                title_ok = (sim >= SIM_THRESHOLD) if hit_title else False

                print(f" cand eid={eid} doi={hit_doi} year={hit_year} sim={sim:.3f}")

                if hit_doi and hit_doi.lower() == doi.lower():
                    best_eid = eid
                    best_doi = hit_doi
                    best_title_ok = title_ok
                    best_year_ok = year_ok
                    break

            if best_eid:
                df_work.at[idx, "Verified_Scopus_EID"] = best_eid
                df_work.at[idx, "Verified_Scopus_DOI"] = best_doi
                df_work.at[idx, "Possible_Scopus_EID"] = ""
                df_work.at[idx, "Possible_Scopus_DOI"] = ""
                df_work.at[idx, "Check_Title_OK"] = str(best_title_ok)
                df_work.at[idx, "Check_Year_OK"] = str(best_year_ok)
                # For DOI-round we mark all checks as "doi_round" like in your WoS script
                df_work.at[idx, "Check_ISSN_OK"] = "doi_round"
                df_work.at[idx, "Check_Biblio_OK"] = "doi_round"
                df_work.at[idx, "Check_Authors_OK"] = "doi_round"
                df_work.at[idx, "Check_HostISBN_OK"] = "doi_round"
                df_work.at[idx, "Check_BookISBN_OK"] = "doi_round"
                accepted_count += 1
                print(f" ✓✓✓ ACCEPT VERIFIED Scopus EID={best_eid} via DOI")
            else:
                print(" No exact DOI-based EID acceptance in round 1")

            print(f" -> accepted so far: {accepted_count}/{MAX_ACCEPTED}")
            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"\n[ERROR] Unexpected failure on round1 index {idx}, PID={row.get('PID','?')}: {e}")
            time.sleep(SLEEP_SECONDS)
            continue

    # ---- ROUND 2: Title/year -> Scopus EID ----
    remaining_mask = (df_work["Verified_Scopus_EID"].str.strip() == "")
    round2_index = list(df_work[remaining_mask].index)
    print(f"Round 2 rows (still missing Scopus EID): {len(round2_index)}")

    for idx in tqdm(round2_index, desc="Round 2 Title -> Scopus"):
        if accepted_count >= MAX_ACCEPTED:
            print(f"\nReached MAX_ACCEPTED={MAX_ACCEPTED}, stopping early.")
            break

        try:
            row = df_work.loc[idx]
            pid = row["PID"].strip()
            title = row["Title"].strip()
            year_str = row["Year"].strip()
            diva_pubtype = (row.get("PublicationType", "") or "").strip()
            diva_cat = diva_pubtype_category(diva_pubtype)

            try:
                pub_year = int(year_str)
            except Exception:
                pub_year = None

            print(f"\n[ROUND2 {idx}] PID={pid} PubType={diva_pubtype} (cat={diva_cat})")
            print(f" Title: '{title}'")
            print(f" Year: {pub_year}")
            print(
                f" DiVA biblio: Vol={row.get('Volume','')} "
                f"Issue={row.get('Issue','')} "
                f"Start={row.get('StartPage','')} End={row.get('EndPage','')}"
            )

            print(" -> querying Scopus by title/year...")

            try:
                query_used, candidates = search_scopus_title(title, pub_year, max_results=SCOPUS_COUNT)
            except Exception as e:
                print(f" ERROR querying Scopus: {e}")
                time.sleep(SLEEP_SECONDS)
                continue

            df_work.at[idx, "Scopus_Query"] = query_used
            df_work.at[idx, "Scopus_Match_Method"] = "title_round"

            if not candidates or pub_year is None:
                print(" No candidates found or no valid year")
                time.sleep(SLEEP_SECONDS)
                continue

            cand_sims: list[tuple[str, float, int | None, str]] = []

            best_verified_eid = None
            best_verified_doi = None
            best_verified_score = 0.0
            best_possible_eid = None
            best_possible_doi = None
            best_possible_score = 0.0
            best_year_verified = None
            best_year_possible = None
            best_possible_checks = {}
            best_verified_checks = {}

            for entry in candidates:
                eid = extract_scopus_eid(entry)
                cand_title = extract_scopus_title(entry)
                cand_year = extract_scopus_year(entry)
                cand_doi = extract_scopus_doi(entry)

                print(
                    f" cand: '{cand_title}' (Scopus year={cand_year}, "
                    f"subtype={entry.get('subtype')}, eid={eid})"
                )
                if cand_year != pub_year:
                    print(" -> skip (year mismatch)")
                    continue

                scopus_cat = scopus_document_type_category(entry)
                if diva_cat and scopus_cat and scopus_cat != diva_cat:
                    print(f" -> skip (type mismatch: DiVA={diva_cat}, Scopus={scopus_cat})")
                    continue

                sim = title_similarity(title, cand_title)
                print(f" DOI: {cand_doi}")
                print(f" Title sim={sim:.3f}")

                cand_sims.append((eid, sim, cand_year, cand_doi))

                if sim < SIM_THRESHOLD:
                    print(f" -> skip (similarity {sim:.3f} < {SIM_THRESHOLD})")
                    continue

                if sim > best_possible_score:
                    best_possible_score = sim
                    best_possible_eid = eid
                    best_possible_doi = cand_doi
                    best_year_possible = cand_year

                print(" -> Title similarity OK, checking for VERIFICATION...")

                # For Scopus we usually have enough in the entry itself, no extra call
                scopus_biblio = extract_scopus_biblio(entry)

                if diva_cat == "article":
                    need_issn = True
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = False
                elif diva_cat == "conference":
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = True
                    need_book_isbn = False
                elif diva_cat == "chapter":
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = True
                    need_book_isbn = False
                elif diva_cat == "book":
                    need_issn = False
                    need_biblio = False
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = True
                else:
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = False

                issn_ok = True
                biblio_ok = True
                author_ok = True
                host_isbn_ok = True
                book_isbn_ok = True

                if need_issn:
                    issn_ok = issn_match(row, scopus_biblio)

                if need_biblio:
                    biblio_ok = bibliographic_match(row, scopus_biblio)

                if need_authors:
                    author_ok = authors_match(row, entry)

                if need_host_isbn:
                    host_isbns = extract_host_isbns(row)
                    sc_isbns = extract_scopus_isbns(entry)
                    inter = host_isbns & sc_isbns
                    print(f" Host ISBNs (DiVA): {sorted(host_isbns)}")
                    print(f" Scopus ISBNs: {sorted(sc_isbns)}")
                    print(f" Host ISBN intersection: {sorted(inter)}")
                    host_isbn_ok = bool(inter)

                if need_book_isbn:
                    book_isbns = extract_diva_book_isbns(row)
                    sc_isbns = extract_scopus_isbns(entry)
                    inter = book_isbns & sc_isbns
                    print(f" Book ISBNs (DiVA): {sorted(book_isbns)}")
                    print(f" Scopus ISBNs: {sorted(sc_isbns)}")
                    print(f" Book ISBN intersection: {sorted(inter)}")
                    book_isbn_ok = bool(inter)

                all_ok = (
                    issn_ok
                    and biblio_ok
                    and (not need_authors or author_ok)
                    and (not need_host_isbn or host_isbn_ok)
                    and (not need_book_isbn or book_isbn_ok)
                )

                if all_ok:
                    print(" ✓✓✓ VERIFIED match (all required checks passed)")
                    if sim > best_verified_score:
                        best_verified_score = sim
                        best_verified_eid = eid
                        best_verified_doi = cand_doi
                        best_year_verified = cand_year
                        best_verified_checks = {
                            "Check_ISSN_OK": str(issn_ok),
                            "Check_Biblio_OK": str(biblio_ok),
                            "Check_Authors_OK": str(author_ok),
                            "Check_HostISBN_OK": str(host_isbn_ok),
                            "Check_BookISBN_OK": str(book_isbn_ok),
                            "Check_Category": diva_cat or "",
                            "Check_Title_OK": str(sim >= SIM_THRESHOLD),
                            "Check_Year_OK": str(cand_year == pub_year),
                        }
                else:
                    print(" ✗ Not all verification checks passed")
                    if sim == best_possible_score and best_possible_eid == eid:
                        best_possible_checks = {
                            "Check_ISSN_OK": str(issn_ok),
                            "Check_Biblio_OK": str(biblio_ok),
                            "Check_Authors_OK": str(author_ok),
                            "Check_HostISBN_OK": str(host_isbn_ok),
                            "Check_BookISBN_OK": str(book_isbn_ok),
                            "Check_Category": diva_cat or "",
                            "Check_Title_OK": str(sim >= SIM_THRESHOLD),
                            "Check_Year_OK": str(cand_year == pub_year),
                        }

            if best_verified_eid:
                df_work.at[idx, "Verified_Scopus_EID"] = best_verified_eid
                df_work.at[idx, "Verified_Scopus_DOI"] = best_verified_doi
                df_work.at[idx, "Possible_Scopus_EID"] = ""
                df_work.at[idx, "Possible_Scopus_DOI"] = ""
                for k, v in best_verified_checks.items():
                    df_work.at[idx, k] = v
                accepted_count += 1
                print(
                    f" ✓✓✓ ACCEPT VERIFIED Scopus EID={best_verified_eid} "
                    f"(sim={best_verified_score:.3f}, year={best_year_verified})"
                )

            elif best_possible_eid:
                for k, v in best_possible_checks.items():
                    df_work.at[idx, k] = v
                df_work.at[idx, "Possible_Scopus_EID"] = best_possible_eid
                df_work.at[idx, "Possible_Scopus_DOI"] = best_possible_doi
                df_work.at[idx, "Verified_Scopus_EID"] = ""
                df_work.at[idx, "Verified_Scopus_DOI"] = ""
                accepted_count += 1
                print(
                    f" ✓ ACCEPT POSSIBLE Scopus EID={best_possible_eid} "
                    f"(sim={best_possible_score:.3f}, year={best_year_possible})"
                )

            else:
                exact_matches = [(u, s, y, d) for (u, s, y, d) in cand_sims if s == 1.0]
                if exact_matches:
                    eid, s, y, doi = exact_matches[0]
                    df_work.at[idx, "Possible_Scopus_EID"] = eid
                    df_work.at[idx, "Possible_Scopus_DOI"] = doi
                    df_work.at[idx, "Verified_Scopus_EID"] = ""
                    df_work.at[idx, "Verified_Scopus_DOI"] = ""
                    df_work.at[idx, "Check_ISSN_OK"] = "title_only"
                    df_work.at[idx, "Check_Biblio_OK"] = "title_only"
                    df_work.at[idx, "Check_Authors_OK"] = "title_only"
                    df_work.at[idx, "Check_HostISBN_OK"] = "title_only"
                    df_work.at[idx, "Check_BookISBN_OK"] = "title_only"
                    df_work.at[idx, "Check_Category"] = diva_cat or ""
                    df_work.at[idx, "Check_Title_OK"] = "title_only"
                    df_work.at[idx, "Check_Year_OK"] = str(y == pub_year)
                    accepted_count += 1
                    print(f" ✓ FALLBACK POSSIBLE Scopus EID={eid} (perfect title match, year={y})")
                else:
                    print(" REJECT all candidates (no Scopus EID passed the minimum checks)")

            print(f" -> accepted so far: {accepted_count}/{MAX_ACCEPTED}")
            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"\n[ERROR] Unexpected failure on round2 index {idx}, PID={row.get('PID','?')}: {e}")
            time.sleep(SLEEP_SECONDS)
            continue

    mask_has_candidate = (
        df_work["Possible_Scopus_EID"].str.strip() != ""
    ) | (
        df_work["Verified_Scopus_EID"].str.strip() != ""
    )
    df_out = df_work[mask_has_candidate].copy()

    csv_col_order = [
        "PID",
        "Verified_Scopus_EID",
        "Possible_Scopus_EID",
        "Verified_Scopus_DOI",
        "Possible_Scopus_DOI",
        "Scopus_Match_Method",
        "Scopus_Query",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "DOI",
        "ISI",
        "ScopusId",
        "PMID",
        "Title",
        "Year",
        "PublicationType",
        "Journal",
        "Volume",
        "Issue",
        "Pages",
        "StartPage",
        "EndPage",
        "JournalISSN",
        "JournalEISSN",
        "SeriesISSN",
        "SeriesEISSN",
        "ISBN",
        "ISBN_PRINT",
        "ISBN_ELECTRONIC",
        "ISBN_UNDEFINED",
        "ArticleId",
        "Name",
        "Notes",
    ]
    csv_col_order = [c for c in csv_col_order if c in df_out.columns]
    remaining = [c for c in df_out.columns if c not in csv_col_order]
    csv_col_order.extend(remaining)
    df_out = df_out[csv_col_order]

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nAccepted {accepted_count} records.")
    print(f"Wrote {len(df_out)} rows with candidates to {OUTPUT_CSV}")

    df_links = df_out.copy()
    df_links["PID_link"] = df_links["PID"].apply(make_pid_url)
    df_links["Verified_Scopus_EID_link"] = df_links["Verified_Scopus_EID"].apply(make_scopus_eid_url)
    df_links["Possible_Scopus_EID_link"] = df_links["Possible_Scopus_EID"].apply(make_scopus_eid_url)
    df_links["Verified_Scopus_DOI_link"] = df_links["Verified_Scopus_DOI"].apply(make_doi_url)
    df_links["Possible_Scopus_DOI_link"] = df_links["Possible_Scopus_DOI"].apply(make_doi_url)

    excel_col_order = [
        "PID",
        "PID_link",
        "Verified_Scopus_EID",
        "Verified_Scopus_EID_link",
        "Possible_Scopus_EID",
        "Possible_Scopus_EID_link",
        "Verified_Scopus_DOI",
        "Verified_Scopus_DOI_link",
        "Possible_Scopus_DOI",
        "Possible_Scopus_DOI_link",
        "Scopus_Match_Method",
        "Scopus_Query",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "DOI",
        "ISI",
        "ScopusId",
        "PMID",
        "Title",
        "Year",
        "PublicationType",
        "Journal",
        "Volume",
        "Issue",
        "Pages",
        "StartPage",
        "EndPage",
        "JournalISSN",
        "JournalEISSN",
        "SeriesISSN",
        "SeriesEISSN",
        "ISBN",
        "ISBN_PRINT",
        "ISBN_ELECTRONIC",
        "ISBN_UNDEFINED",
        "ArticleId",
        "Name",
        "Notes",
    ]
    excel_col_order = [c for c in excel_col_order if c in df_links.columns]
    remaining = [c for c in df_links.columns if c not in excel_col_order]
    excel_col_order.extend(remaining)
    df_links = df_links[excel_col_order]

    with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as writer:
        df_links.to_excel(writer, index=False, sheet_name="Scopus EID candidates")
        ws = writer.sheets["Scopus EID candidates"]

        header = list(df_links.columns)
        col_idx = {name: i for i, name in enumerate(header)}

        for row_xl, df_idx in enumerate(df_links.index, start=1):
            if df_links.at[df_idx, "PID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["PID_link"],
                    df_links.at[df_idx, "PID_link"],
                    string="PID",
                )
            if df_links.at[df_idx, "Verified_Scopus_EID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Verified_Scopus_EID_link"],
                    df_links.at[df_idx, "Verified_Scopus_EID_link"],
                    string="Verified Scopus EID",
                )
            if df_links.at[df_idx, "Possible_Scopus_EID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Possible_Scopus_EID_link"],
                    df_links.at[df_idx, "Possible_Scopus_EID_link"],
                    string="Possible Scopus EID",
                )
            if df_links.at[df_idx, "Verified_Scopus_DOI_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Verified_Scopus_DOI_link"],
                    df_links.at[df_idx, "Verified_Scopus_DOI_link"],
                    string="Verified DOI",
                )
            if df_links.at[df_idx, "Possible_Scopus_DOI_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Possible_Scopus_DOI_link"],
                    df_links.at[df_idx, "Possible_Scopus_DOI_link"],
                    string="Possible DOI",
                )

    print(f"Wrote Excel with links to {EXCEL_OUT}")


if __name__ == "__main__":
    main()
