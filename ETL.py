# ETL.py

import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION - Change these paths as needed
# ============================================================
INPUT_FILE = r'c:\Users\nouno\Downloads\PolitiFact_DIRTY.csv'
OUTPUT_FILE = r'c:\Users\nouno\Downloads\PolitiFact_CLEAN.csv'
DATA_DICT_FILE = r'c:\Users\nouno\Downloads\PolitiFact_DataDictionary.xlsx'


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def fix_encoding(text):
    """Fix encoding issues in text - remove non-ASCII characters."""
    if pd.isna(text):
        return text
    text = str(text)
    
    # Remove any non-ASCII characters
    text = ''.join(char if ord(char) < 128 else '' for char in text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def clean_text(text):
    """Remove whitespace issues and fix encoding."""
    if pd.isna(text) or str(text).strip() == '':
        return None
    text = str(text)
    text = fix_encoding(text)
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    if text == '':
        return None
    return text


def standardize_label(label):
    """Standardize fact-check labels to official PolitiFact scale."""
    if pd.isna(label) or str(label).strip() == '':
        return 'Unknown'
    
    label = str(label).strip().lower()
    label = re.sub(r'[^a-z\-\s]', '', label).strip()
    
    if label == '':
        return 'Unknown'
    
    if label in ['true', 'tru', 't']:
        return 'TRUE'
    elif label in ['false', 'fals', 'f']:
        return 'FALSE'
    elif 'mostly' in label and 'true' in label:
        return 'mostly-true'
    elif 'barely' in label and 'true' in label:
        return 'barely-true'
    elif 'half' in label and 'true' in label:
        return 'half-true'
    elif 'pants' in label or 'fire' in label:
        return 'pants-fire'
    else:
        return 'Unknown'


def parse_date(date_str):
    """Parse various date formats and return standardized YYYY-MM-DD format."""
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    date_str = re.sub(r'\s+', ' ', date_str).strip()
    
    # Skip if it looks invalid
    if date_str.lower() in ['unknown', 'nan', 'none', '']:
        return None
    
    # List of date formats to try
    date_formats = [
        '%d-%b-%y',       # 18-Jun-20
        '%d-%b-%Y',       # 18-Jun-2020
        '%b %d, %Y',      # Jun 19, 2020
        '%B %d, %Y',      # June 19, 2020
        ' %B %d, %Y',     # June 19, 2020 (leading space)
        '%m/%d/%Y',       # 06/19/2020
        '%m/%d/%y',       # 06/19/20
        '%Y-%m-%d',       # 2020-06-19
        '%d-%m-%Y',       # 19-06-2020
        '%d-%m-%y',       # 19-06-20
        '%d/%m/%Y',       # 19/06/2020
        '%d/%m/%y',       # 19/06/20
        '%B %d,%Y',       # June 19,2020
        '%Y/%m/%d',       # 2020/06/19
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def validate_url(url):
    """Check if URL is valid."""
    if pd.isna(url) or url in ['Not Available', 'Invalid URL', '']:
        return False
    url = str(url).strip()
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(pattern, url))


# ============================================================
# MAIN ETL PIPELINE
# ============================================================

def run_etl():
    """Execute the ETL cleaning pipeline."""
    
    print("=" * 60)
    print("        ETL PIPELINE: Data Cleaning")
    print("=" * 60)
    
    # ----------------------------------------------------------
    # STEP 1: EXTRACT - Load Data
    # ----------------------------------------------------------
    print("\n[1/7] EXTRACT: Loading data...")
    
    # Try different encodings
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(INPUT_FILE, encoding=encoding)
            print(f"      Loaded with {encoding} encoding")
            break
        except:
            continue
    
    initial_rows = len(df)
    print(f"      Total rows: {initial_rows:,}")
    print(f"      Columns: {list(df.columns)}")
    
    # ----------------------------------------------------------
    # STEP 2: Handle Missing Values
    # ----------------------------------------------------------
    print("\n[2/7] TRANSFORM: Handling missing values...")
    
    rows_before = len(df)
    df = df.dropna(subset=['News_Headline'])
    print(f"      Dropped {rows_before - len(df)} rows with missing headlines")
    
    df['Source'] = df['Source'].replace('', 'Unknown')
    df['Source'] = df['Source'].fillna('Unknown')
    print("      Filled missing Source with 'Unknown'")
    
    df['Link_Of_News'] = df['Link_Of_News'].fillna('Not Available')
    df['Link_Of_News'] = df['Link_Of_News'].replace('', 'Not Available')
    print("      Filled missing URLs with 'Not Available'")
    
    df['Date'] = df['Date'].fillna('')
    df['Stated_On'] = df['Stated_On'].fillna('')
    df['Label'] = df['Label'].fillna('')
    
    # ----------------------------------------------------------
    # STEP 3: Remove Duplicates
    # ----------------------------------------------------------
    print("\n[3/7] TRANSFORM: Removing duplicates...")
    rows_before = len(df)
    duplicates = df.duplicated().sum()
    df = df.drop_duplicates(keep='first')
    print(f"      Found and removed {duplicates} duplicates")
    
    # ----------------------------------------------------------
    # STEP 4: Clean Text Fields
    # ----------------------------------------------------------
    print("\n[4/7] TRANSFORM: Cleaning text fields...")
    
    for col in ['News_Headline', 'Source', 'Stated_On']:
        df[col] = df[col].apply(clean_text)
    print("      Fixed encoding and whitespace issues")
    
    # Fill any None values created by clean_text
    df['Source'] = df['Source'].fillna('Unknown')
    
    # ----------------------------------------------------------
    # STEP 5: Standardize Labels
    # ----------------------------------------------------------
    print("\n[5/7] TRANSFORM: Standardizing labels...")
    unique_before = df['Label'].nunique()
    df['Label'] = df['Label'].apply(standardize_label)
    unique_after = df['Label'].nunique()
    print(f"      Standardized {unique_before} variations to {unique_after} labels")
    
    # ----------------------------------------------------------
    # STEP 6: Parse Dates
    # ----------------------------------------------------------
    print("\n[6/7] TRANSFORM: Parsing dates...")
    
    df['Date'] = df['Date'].apply(parse_date)
    df['Stated_On'] = df['Stated_On'].apply(parse_date)
    
    valid_dates = df['Date'].notna().sum()
    valid_stated = df['Stated_On'].notna().sum()
    print(f"      Parsed {valid_dates:,} Date values")
    print(f"      Parsed {valid_stated:,} Stated_On values")
    
    # ----------------------------------------------------------
    # STEP 7: Validate URLs
    # ----------------------------------------------------------
    print("\n[7/7] TRANSFORM: Validating URLs...")
    
    df['_url_valid'] = df['Link_Of_News'].apply(validate_url)
    valid_urls = df['_url_valid'].sum()
    invalid_urls = len(df) - valid_urls
    
    df.loc[~df['_url_valid'] & ~df['Link_Of_News'].isin(['Not Available', '']), 'Link_Of_News'] = 'Invalid URL'
    df = df.drop(columns=['_url_valid'])
    
    print(f"      Valid URLs: {valid_urls:,}")
    print(f"      Invalid URLs: {invalid_urls:,}")
    
    # ----------------------------------------------------------
    # LOAD: Export Clean Data
    # ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("LOAD: Exporting cleaned data...")
    print("=" * 60)
    
    column_order = ['News_Headline', 'Source', 'Stated_On', 'Date', 'Label', 'Link_Of_News']
    df_final = df[column_order]
    
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\nClean data saved: {OUTPUT_FILE}")
    print(f"Total records: {len(df_final):,}")
    
    # ----------------------------------------------------------
    # Generate Data Dictionary
    # ----------------------------------------------------------
    print("\nGenerating data dictionary...")
    
    descriptions = {
        'News_Headline': 'The claim or statement being fact-checked',
        'Source': 'Person or entity who made the claim',
        'Stated_On': 'Date when the claim was originally stated (YYYY-MM-DD)',
        'Date': 'Date when fact-check was published (YYYY-MM-DD)',
        'Label': 'Verdict: TRUE, mostly-true, half-true, barely-true, FALSE, pants-fire',
        'Link_Of_News': 'URL to full fact-check article'
    }
    
    data_dict = []
    for col in df_final.columns:
        samples = df_final[col].dropna().head(3).tolist()
        sample_str = ' | '.join([str(s)[:40] for s in samples])
        data_dict.append({
            'Column': col,
            'Type': str(df_final[col].dtype),
            'Description': descriptions.get(col, 'N/A'),
            'Nulls': df_final[col].isnull().sum(),
            'Unique': df_final[col].nunique(),
            'Sample': sample_str
        })
    
    dict_df = pd.DataFrame(data_dict)
    dict_df.to_excel(DATA_DICT_FILE, index=False, sheet_name='Data Dictionary')
    print(f"Data dictionary saved: {DATA_DICT_FILE}")
    
    # Summary
    print("\n" + "=" * 60)
    print("                    ETL COMPLETE")
    print("=" * 60)
    print(f"\nRows: {initial_rows:,} -> {len(df_final):,}")
    print(f"Removed: {initial_rows - len(df_final):,} rows")
    
    return df_final


if __name__ == "__main__":
    df_clean = run_etl()
