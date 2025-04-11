# app/main.py
from fastapi import FastAPI, UploadFile, File
import pandas as pd
import re
from difflib import SequenceMatcher
from fastapi.responses import JSONResponse
import io

app = FastAPI()

# --- Helpers for fuzzy address cleaning ---
def clean_and_extract_address(raw):
    cleaned = re.sub(r"(invoice|ref|payment|#)?\s*\d+[a-zA-Z0-9/]*", "", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"[^\w\s/]", "", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned.lower()

def extract_fallback_number_and_street(address):
    numbers = re.findall(r"\d+", address)
    number = numbers[-1] if numbers else None
    street_match = re.match(r"(?:\d+\s*)*(.*)", address)
    street = street_match.group(1).strip() if street_match else address.strip()
    return number, street.lower()

def street_only_fallback_match(df, original, target_num, target_street, min_street_len=5, min_similarity=0.5):
    all_matches = []
    candidates = df[df["Street Name"].str.contains(target_street, na=False, regex=False)]
    if target_num and not candidates.empty:
        number_matches = candidates[candidates["Address_lower"].str.contains(target_num)]
        if not number_matches.empty:
            return number_matches[["Address", "Property Manager"]]
    if not candidates.empty:
        return candidates[["Address", "Property Manager"]]
    parts = target_street.split()
    for i in range(len(parts), 0, -1):
        partial = " ".join(parts[:i])
        if len(partial) < min_street_len:
            continue
        partial_candidates = df[df["Street Name"].str.contains(partial, na=False)]
        if target_num:
            partial_candidates = partial_candidates[partial_candidates["Address_lower"].str.contains(target_num)]
        for _, row in partial_candidates.iterrows():
            sim = SequenceMatcher(None, partial, row["Street Name"]).ratio()
            if sim > min_similarity:
                all_matches.append(row)
    return pd.DataFrame(all_matches)[["Address", "Property Manager"]] if all_matches else pd.DataFrame(columns=["Address", "Property Manager"])

# --- API Route ---
@app.post("/match")
def match_addresses(database_csv: UploadFile = File(...), input_csv: UploadFile = File(...)):
    try:
        # Read database CSV
        db_df = pd.read_csv(io.BytesIO(database_csv.file.read()))
        db_df["Address_lower"] = db_df["Address"].str.lower()
        db_df["Street Name"] = db_df["Address"].str.extract(r"\d+[\/\s,]*\s*(.*)", expand=False).str.lower().str.strip()

        # Read input CSV
        input_df = pd.read_csv(io.BytesIO(input_csv.file.read()))
        input_addresses = input_df.iloc[:, 0].dropna().astype(str).tolist()

        results = []
        for raw_address in input_addresses:
            cleaned = clean_and_extract_address(raw_address)
            num, street = extract_fallback_number_and_street(cleaned)
            matches_df = street_only_fallback_match(db_df, raw_address, num, street)
            if not matches_df.empty:
                for _, row in matches_df.iterrows():
                    results.append({
                        "Original": raw_address,
                        "Matched Address": row["Address"],
                        "Property Manager": row["Property Manager"]
                    })
            else:
                results.append({
                    "Original": raw_address,
                    "Matched Address": None,
                    "Property Manager": None
                })

        return JSONResponse(content={"results": results})

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
