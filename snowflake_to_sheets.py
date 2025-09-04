#!/usr/bin/env python3
"""
Snowflake → Google Sheets daily uploader for LLM-as-a-Judge metrics

- Reads Snowflake credentials from environment variables
  SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

- For each metric and applicable department, fetches yesterday's rows from
  Snowflake raw tables and writes them into the corresponding Google Sheet tab
  named YYYY-MM-DD (and optional -RAW for categorizing).

- Updates department daily snapshot sheets from per-department summary tables
  (e.g., mv_resolvers_summary) by locating yesterday's date row and metric
  columns, then writing values per department (no cross-department averaging).

Run: python "Snowflake Direct to Sheets/snowflake_to_sheets.py"
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import random
from dotenv import load_dotenv

try:
    import snowflake.connector
except Exception as e:
    print(f"❌ Failed to import snowflake-connector-python: {e}")
    print("   Ensure 'snowflake-connector-python' is installed (see requirements.txt)")
    raise

# Make project root importable for config modules
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.departments import DEPARTMENTS
from config.sheets import SHEETS_CONFIG


# =============================
# Utility: Dates and Departments
# =============================

def get_yesterday_date_str() -> str:
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


def clean_department_key(dept_name: str) -> str:
    """Return the canonical clean_name from config.departments for summary table name."""
    if dept_name in DEPARTMENTS:
        return DEPARTMENTS[dept_name].get('clean_name', dept_name.lower().replace(' ', '_'))
    return dept_name.lower().replace(' ', '_')


def map_dept_for_snowflake(dept_name: str) -> Tuple[str, str]:
    """Map human department names to Snowflake DEPARTMENT values.
    Per user request, use specific tokens with underscores and AT_ prefixes.
    Returns (mapped_value, match_mode) where match_mode is 'exact' or 'prefix'.
    Defaults to underscore format + exact when not explicitly mapped.
    """
    mapping = {
        'MV Resolvers': ('MV_Resolvers', 'exact'),
        'MV Sales': ('MV_Sales', 'exact'),
        'CC Sales': ('CC_Sales', 'exact'),
        'CC Resolvers': ('CC_Resolvers', 'exact'),
        'Filipina': ('AT_Filipina', 'prefix'),
        'Ethiopian': ('AT_Ethiopian', 'prefix'),
        'African': ('AT_African', 'prefix'),
        'Doctors': ('Doctors', 'exact'),
    }
    return mapping.get(dept_name, (dept_name.replace(' ', '_'), 'exact'))


def normalize_department_input(raw: str) -> str:
    """Normalize user-provided department tokens (env) to display names in DEPARTMENTS keys.
    Accepts forms like 'MV_Resolvers', 'CC_Sales', 'AT_Filipina', etc.
    Returns the canonical display name (e.g., 'MV Resolvers').
    """
    token = raw.strip()
    # Direct match (case-insensitive) to existing keys
    for k in DEPARTMENTS.keys():
        if token.lower() == k.lower():
            return k
    # Map common underscore and AT_ variants
    mapping = {
        'MV_RESOLVERS': 'MV Resolvers',
        'MV_SALES': 'MV Sales',
        'CC_SALES': 'CC Sales',
        'CC_RESOLVERS': 'CC Resolvers',
        'AT_FILIPINA': 'Filipina',
        'AT_ETHIOPIAN': 'Ethiopian',
        'AT_AFRICAN': 'African',
        'DOCTORS': 'Doctors',
        'DELIGHTERS': 'Delighters',
    }
    upper = token.replace(' ', '_').upper()
    return mapping.get(upper, token.replace('_', ' ').title())


def log_distinct_departments(conn, table_name: str, date_str: str) -> None:
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT UPPER(TRIM(DEPARTMENT)) AS DEPT
            FROM {table_name}
            WHERE TO_DATE(DATE) = %s
            ORDER BY DEPT
            """,
            (date_str,)
        )
        rows = [r[0] for r in cur.fetchall()]
        print(f"   🔎 Available departments for {table_name} on {date_str}: {rows}")
    except Exception as e:
        print(f"   ⚠️ Failed to list distinct departments: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


# =============================
# Snowflake helpers
# =============================

def get_snowflake_connection():
    """Create a Snowflake connection from environment variables."""
    # Load .env if present so users don't need to export variables manually
    try:
        load_dotenv()
    except Exception:
        pass
    required_env = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_ROLE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    missing = [k for k in required_env if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    print("🔌 Connecting to Snowflake …")
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        role=os.environ['SNOWFLAKE_ROLE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
    )
    print("✅ Snowflake connection established")
    return conn


def fetch_table_df(conn, table_name: str, date_str: str, department: str) -> pd.DataFrame:
    """Fetch rows from a raw Snowflake table for a given date and department.

    Expects columns: DATE, DEPARTMENT plus metric-specific columns (e.g. llm_output).
    """
    print(f"   📥 Fetching: {table_name} for {department} on {date_str}")
    # Map department to Snowflake DEPARTMENT representation (uppercase compare)
    mapped, mode = map_dept_for_snowflake(department)
    dept_key = mapped.upper().strip()
    if mode == 'prefix':
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE TO_DATE(DATE) = %s AND UPPER(TRIM(DEPARTMENT)) LIKE %s
        """
        dept_param = dept_key + '%'
    else:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE TO_DATE(DATE) = %s AND UPPER(TRIM(DEPARTMENT)) = %s
        """
        dept_param = dept_key
    cur = conn.cursor()
    try:
        cur.execute(query, (date_str, dept_param))
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        print(f"   ✅ Retrieved {len(df)} row(s)")
        if df.empty:
            log_distinct_departments(conn, table_name, date_str)
        else:
            # Debug: show a sample of distinct DEPARTMENT values found
            try:
                distinct = sorted({str(x).strip() for x in df['DEPARTMENT'].unique()}) if 'DEPARTMENT' in df.columns else []
                print(f"   🧪 Sample DEPARTMENT values: {distinct[:5]}")
            except Exception:
                pass
        return df
    finally:
        cur.close()


def fetch_summary_row(conn, dept_name: str, date_str: str) -> Optional[pd.Series]:
    """Fetch a single summary row for department and date from per-dept summary table.
    Table name pattern: {dept_clean_name}_summary
    Returns a pandas Series or None.
    """
    # Some departments use prefixed summary table names in Snowflake
    summary_overrides = {
        'Ethiopian': 'AT_Ethiopian_summary',
        'Filipina': 'AT_Filipina_summary',
        'African': 'AT_African_summary',
    }
    if dept_name in summary_overrides:
        table_name = summary_overrides[dept_name]
    else:
        dept_clean = clean_department_key(dept_name)
        table_name = f"{dept_clean}_summary"
    print(f"   📊 Fetching summary from: {table_name} for {dept_name} on {date_str}")
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE TO_DATE(DATE) = %s
        ORDER BY TIMESTAMP DESC
        LIMIT 1
    """
    cur = conn.cursor()
    try:
        cur.execute(query, (date_str,))
        row = cur.fetchone()
        if not row:
            print("   ⚠️ No summary row found")
            return None
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame([row], columns=cols)
        print("   ✅ Summary row fetched")
        return df.iloc[0]
    finally:
        cur.close()


def fetch_total_chats(conn, date_str: str, department: str) -> int:
    """Return count of chats from sa_raw_data for the given date and department."""
    mapped, mode = map_dept_for_snowflake(department)
    dept_key = mapped.upper().strip()
    if mode == 'prefix':
        query = """
            SELECT COUNT(*)
            FROM sa_raw_data
            WHERE TO_DATE(DATE) = %s AND UPPER(TRIM(DEPARTMENT)) LIKE %s
        """
        dept_param = dept_key + '%'
    else:
        query = """
            SELECT COUNT(*)
            FROM sa_raw_data
            WHERE TO_DATE(DATE) = %s AND UPPER(TRIM(DEPARTMENT)) = %s
        """
        dept_param = dept_key
    cur = conn.cursor()
    try:
        cur.execute(query, (date_str, dept_param))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        cur.close()

# =============================
# Google Sheets helpers
# =============================

class SheetsClient:
    def __init__(self, credentials_path: str = 'credentials.json'):
        # Try environment variable first (for GitHub Actions), then file
        google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        
        if google_creds_json:
            # Load from environment variable (GitHub Actions)
            import json
            import tempfile
            creds_dict = json.loads(google_creds_json)
            creds = Credentials.from_service_account_info(
                creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            print("🔐 Google Sheets API initialized (from environment)")
        else:
            # Load from file (local development)
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
            creds = Credentials.from_service_account_file(
                credentials_path, scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            print("🔐 Google Sheets API initialized (from file)")
            
        self.service = build('sheets', 'v4', credentials=creds)
        # Cache to avoid redundant create checks/requests
        self._sheet_exists_cache = set()

    def _execute_with_retry(self, func_desc: str, func, max_attempts: int = 5, base_delay: float = 1.5, suppress_messages: list | None = None):
        """Execute a Sheets API call with retries on transient errors (429/500/503)."""
        for attempt in range(1, max_attempts + 1):
            try:
                return func()
            except HttpError as he:
                status = getattr(he, 'resp', None).status if hasattr(he, 'resp') else None
                msg = str(he)
                if status in (429, 500, 503) or 'RATE_LIMIT' in msg or 'quota' in msg.lower():
                    if attempt < max_attempts:
                        delay = base_delay * (2 ** (attempt - 1))
                        delay += random.uniform(0, 0.5)
                        print(f"   ⏳ {func_desc} transient error {status or ''}. Retrying in {delay:.1f}s … (attempt {attempt}/{max_attempts})")
                        time.sleep(delay)
                        continue
                # Non-retriable or maxed
                if suppress_messages and any(s in msg.lower() for s in suppress_messages):
                    # Suppress noisy error output; bubble up for caller to handle
                    raise
                print(f"   ❌ {func_desc} failed: {msg}")
                raise
            except Exception as e:
                if attempt < max_attempts:
                    delay = base_delay * (2 ** (attempt - 1))
                    delay += random.uniform(0, 0.5)
                    print(f"   ⏳ {func_desc} error. Retrying in {delay:.1f}s … (attempt {attempt}/{max_attempts})")
                    time.sleep(delay)
                    continue
                print(f"   ❌ {func_desc} failed: {e}")
                raise

    def create_sheet_if_missing(self, spreadsheet_id: str, sheet_name: str) -> bool:
        cache_key = (spreadsheet_id, sheet_name)
        if cache_key in self._sheet_exists_cache:
            return True
        req = {
            'requests': [{
                'addSheet': {
                    'properties': {'title': sheet_name}
                }
            }]
        }
        backoffs = [2, 5, 8]
        for attempt in range(len(backoffs) + 1):
            try:
                self._execute_with_retry(
                    f"Create sheet '{sheet_name}'",
                    lambda: self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute(),
                    suppress_messages=['already exists', 'already_exists']
                )
                print(f"   🆕 Created sheet: {sheet_name}")
                self._sheet_exists_cache.add(cache_key)
                return True
            except HttpError as he:
                msg = str(he)
                status = getattr(he, 'resp', None).status if hasattr(he, 'resp') else None
                if status in (429, 500, 503) or 'RATE_LIMIT' in msg or 'quota' in msg.lower():
                    if attempt < len(backoffs):
                        sleep_s = backoffs[attempt]
                        print(f"   ⏳ Sheets rate-limit (429). Retrying in {sleep_s}s …")
                        time.sleep(sleep_s)
                        continue
                if 'already exists' in msg.lower() or 'ALREADY_EXISTS' in msg:
                    # Be concise and non-fatal
                    # print(f"📄 Sheet '{sheet_name}' already exists")
                    self._sheet_exists_cache.add(cache_key)
                    return True
                print(f"   ❌ Failed creating sheet '{sheet_name}': {msg}")
                return False
            except Exception as e:
                if 'already exists' in str(e).lower():
                    self._sheet_exists_cache.add(cache_key)
                    return True
                print(f"   ❌ Failed creating sheet '{sheet_name}': {e}")
                return False

    def upload_dataframe(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> bool:
        try:
            def clean_cell_value(v, column_name=""):
                if pd.isna(v) or v is None:
                    return ""
                s = str(v)
                # Replace various line endings with standard newlines
                s = s.replace('\r\n', '\n').replace('\r', '\n')
                
                # Google Sheets has a 50,000 character limit per cell
                # Use different limits based on column type
                if column_name.upper() in ['CONVERSATION_CONTENT', 'LLM_RESPONSE', 'LLM_OUTPUT']:
                    # More aggressive truncation for content-heavy columns
                    max_chars = 30000
                else:
                    # Standard limit for other columns
                    max_chars = 49000
                    
                if len(s) > max_chars:
                    # Try to truncate at a word boundary if possible
                    truncated = s[:max_chars]
                    last_space = truncated.rfind(' ')
                    if last_space > max_chars * 0.8:  # Only use word boundary if it's not too far back
                        s = truncated[:last_space] + '... [TRUNCATED]'
                    else:
                        s = truncated + '... [TRUNCATED]'
                    print(f"   ⚠️ Truncated {column_name} content from {len(str(v))} to {len(s)} characters")
                return s

            cleaned = df.copy()
            for col in cleaned.columns:
                cleaned[col] = cleaned[col].apply(lambda x: clean_cell_value(x, col))

            values = [cleaned.columns.tolist()] + cleaned.values.tolist()
            
            # Check for extremely large payloads and warn
            total_chars = sum(len(str(cell)) for row in values for cell in row)
            if total_chars > 10_000_000:  # 10MB worth of text
                print(f"   ⚠️ Large dataset detected: {total_chars:,} characters, {len(values)} rows")
            
            # Check if we need to expand the sheet for large datasets
            required_rows = len(values)
            if required_rows > 1000:  # Google Sheets default limit is ~1000 rows
                print(f"   📏 Dataset requires {required_rows} rows, checking sheet size...")
                try:
                    # Get current sheet properties
                    sheet_metadata = self._execute_with_retry(
                        f"Get sheet metadata for {sheet_name}",
                        lambda: self.service.spreadsheets().get(
                            spreadsheetId=spreadsheet_id,
                            fields="sheets.properties"
                        ).execute()
                    )
                    
                    # Find the target sheet
                    target_sheet = None
                    for sheet in sheet_metadata.get('sheets', []):
                        if sheet['properties']['title'] == sheet_name:
                            target_sheet = sheet
                            break
                    
                    if target_sheet:
                        current_rows = target_sheet['properties']['gridProperties']['rowCount']
                        sheet_id = target_sheet['properties']['sheetId']
                        
                        if required_rows > current_rows:
                            new_row_count = required_rows + 100  # Add buffer
                            print(f"   📈 Expanding sheet from {current_rows} to {new_row_count} rows...")
                            
                            # Expand the sheet
                            self._execute_with_retry(
                                f"Expand sheet {sheet_name}",
                                lambda: self.service.spreadsheets().batchUpdate(
                                    spreadsheetId=spreadsheet_id,
                                    body={
                                        'requests': [{
                                            'updateSheetProperties': {
                                                'properties': {
                                                    'sheetId': sheet_id,
                                                    'gridProperties': {
                                                        'rowCount': new_row_count
                                                    }
                                                },
                                                'fields': 'gridProperties.rowCount'
                                            }
                                        }]
                                    }
                                ).execute()
                            )
                            print(f"   ✅ Sheet expanded to {new_row_count} rows")
                        else:
                            print(f"   ✅ Sheet has sufficient rows ({current_rows})")
                    
                except Exception as e:
                    print(f"   ⚠️ Could not expand sheet (will proceed anyway): {e}")
                    
                    # Fallback: Create a new sheet with a timestamp suffix
                    if "exceeds grid limits" in str(e).lower() or required_rows > 10000:
                        import datetime
                        timestamp = datetime.datetime.now().strftime("%H%M")
                        fallback_sheet = f"{sheet_name}-{timestamp}"
                        print(f"   🔄 Creating fallback sheet: {fallback_sheet}")
                        
                        try:
                            if self.create_sheet_if_missing(spreadsheet_id, fallback_sheet):
                                sheet_name = fallback_sheet
                                print(f"   ✅ Using fallback sheet: {sheet_name}")
                        except Exception as fallback_error:
                            print(f"   ❌ Fallback sheet creation failed: {fallback_error}")
            
            # Clear and write
            self._execute_with_retry(
                f"Clear range {sheet_name}",
                lambda: self.service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:Z"
                ).execute()
            )

            # For very large datasets, consider batch processing
            if len(values) > 5000:
                print(f"   📊 Processing large dataset in batches: {len(values)} rows")
                
                # Check if dataset is too large for a single sheet (Google Sheets max ~10M cells)
                max_safe_rows = 9000  # Conservative limit to avoid hitting sheet limits
                if len(values) > max_safe_rows:
                    print(f"   ⚠️ Dataset too large for single sheet ({len(values)} rows > {max_safe_rows})")
                    print(f"   📄 Consider splitting data by date range or using multiple sheets")
                    # Truncate to safe size
                    values = values[:max_safe_rows]
                    print(f"   ✂️ Truncated to {len(values)} rows for safety")
                
                batch_size = 1000
                header = values[0]
                
                # Write header first
                self._execute_with_retry(
                    f"Update header {sheet_name}!A1",
                    lambda: self.service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A1",
                        valueInputOption='RAW',
                        body={'values': [header]}
                    ).execute()
                )
                
                # Write data in batches with better error handling
                for i in range(1, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    start_row = i + 1
                    end_row = start_row + len(batch) - 1
                    
                    try:
                        self._execute_with_retry(
                            f"Update batch {sheet_name}!A{start_row}:Z{end_row}",
                            lambda: self.service.spreadsheets().values().update(
                                spreadsheetId=spreadsheet_id,
                                range=f"{sheet_name}!A{start_row}:Z{end_row}",
                                valueInputOption='RAW',
                                body={'values': batch}
                            ).execute()
                        )
                        print(f"   📝 Batch {i//batch_size + 1}: wrote {len(batch)} rows (rows {start_row}-{end_row})")
                    except Exception as batch_error:
                        print(f"   ❌ Batch {i//batch_size + 1} failed at row {start_row}: {batch_error}")
                        if "exceeds grid limits" in str(batch_error):
                            print(f"   ⚠️ Hit grid limits at row {start_row}, stopping upload")
                            break
                        raise batch_error
            else:
                # Standard single upload for smaller datasets
                self._execute_with_retry(
                    f"Update range {sheet_name}!A1",
                    lambda: self.service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A1",
                        valueInputOption='RAW',
                        body={'values': values}
                    ).execute()
                )
                
            print(f"   ✅ Uploaded {len(cleaned)} rows to tab '{sheet_name}'")
            return True
        except Exception as e:
            print(f"   ❌ Upload failed for '{sheet_name}': {e}")
            return False

    def find_header_column(self, spreadsheet_id: str, header_candidates: List[str], sheet_candidates: List[str]) -> Tuple[Optional[int], Optional[str]]:
        for sheet_name in sheet_candidates:
            try:
                res = self._execute_with_retry(
                    f"Read headers {sheet_name}",
                    lambda: self.service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!1:1"
                    ).execute()
                )
                headers = res.get('values', [[]])[0]
                # Exact then case-insensitive
                for i, h in enumerate(headers):
                    if h and h in header_candidates:
                        return i + 1, sheet_name
                lowered = [h.lower() if h else '' for h in headers]
                for i, h in enumerate(lowered):
                    for cand in header_candidates:
                        if h == cand.lower():
                            return i + 1, sheet_name
            except Exception:
                continue
        return None, None

    def find_date_row(self, spreadsheet_id: str, date_str: str, sheet_candidates: List[str]) -> Tuple[Optional[int], Optional[str]]:
        for sheet_name in sheet_candidates:
            try:
                res = self._execute_with_retry(
                    f"Read dates {sheet_name}",
                    lambda: self.service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A:A"
                    ).execute()
                )
                values = res.get('values', [])
                for idx, row in enumerate(values):
                    if row and date_str in str(row[0]).strip():
                        return idx + 1, sheet_name
            except Exception:
                continue
        return None, None

    def ensure_date_row(self, spreadsheet_id: str, date_str: str, sheet_candidates: List[str]) -> Tuple[Optional[int], Optional[str]]:
        """Ensure yesterday's date exists in column A. If missing, append it.
        Returns (row_index, sheet_name) where the date was found or written.
        """
        for sheet_name in sheet_candidates:
            try:
                res = self._execute_with_retry(
                    f"Read dates {sheet_name}",
                    lambda: self.service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A:A"
                    ).execute()
                )
                values = res.get('values', [])
                last_non_empty_row = 0
                last_value = None
                for idx, row in enumerate(values):
                    if row and str(row[0]).strip() != "":
                        last_non_empty_row = idx + 1
                        last_value = str(row[0]).strip()
                if last_value == date_str:
                    return last_non_empty_row, sheet_name
                # Append date if different or not present
                target_row = (last_non_empty_row + 1) if last_non_empty_row > 0 else 2
                self.update_cell(spreadsheet_id, sheet_name, target_row, 1, date_str)
                return target_row, sheet_name
            except Exception:
                continue
        return None, None

    def update_cell(self, spreadsheet_id: str, sheet_name: str, row: int, col: int, value) -> bool:
        try:
            # Convert column number to A1 letter(s)
            if col <= 26:
                col_letter = chr(64 + col)
            else:
                first = chr(64 + ((col - 1) // 26))
                second = chr(64 + ((col - 1) % 26) + 1)
                col_letter = first + second
            rng = f"{sheet_name}!{col_letter}{row}"
            self._execute_with_retry(
                f"Update cell {rng}",
                lambda: self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=rng,
                    valueInputOption='RAW',
                    body={'values': [[value]]}
                ).execute()
            )
            print(f"   ✏️  Wrote '{value}' to {rng}")
            return True
        except Exception as e:
            print(f"   ❌ Failed to update cell: {e}")
            return False

    def merge_consecutive_cells(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Merge consecutive cells for loss of interest summary following the exact flow:
        1. Combine numbers and percentage into same column (already done in formatting)
        2. Merge identical cells in MAIN_REASON, then merge corresponding MAIN_REASON_COUNT cells
        3. Merge identical cells in SUB_REASON, then merge corresponding SUB_REASON_COUNT cells
        """
        try:
            print(f"   🔀 Merging consecutive cells in {sheet_name}")
            
            # Get all data from the sheet
            res = self._execute_with_retry(
                f"Read data for merging {sheet_name}",
                lambda: self.service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:Z"
                ).execute()
            )
            values = res.get('values', [])
            
            if not values or len(values) < 2:
                print(f"   ℹ️ Not enough data to merge in {sheet_name}")
                return True
            
            # Find column indices for target columns
            headers = values[0] if values else []
            target_columns = {
                'RECRUITMENT_STAGE': None,
                'MAIN_REASON': None, 
                'SUB_REASON': None,
                'MAIN_REASON_COUNT': None,
                'SUB_REASON_COUNT': None
            }
            
            for i, header in enumerate(headers):
                if header in target_columns:
                    target_columns[header] = i
                    print(f"   📍 Found column '{header}' at index {i}")
            
            # Get sheet ID for merge requests
            sheet_metadata = self._execute_with_retry(
                f"Get sheet metadata for merging",
                lambda: self.service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets.properties"
                ).execute()
            )
            
            sheet_id = None
            for sheet in sheet_metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                print(f"   ❌ Could not find sheet ID for {sheet_name}")
                return False
            
            merge_requests = []
            num_rows = len(values)
            
            def find_consecutive_ranges(column_idx: int, column_name: str):
                """Find ranges of consecutive identical non-empty values in a column."""
                if column_idx is None:
                    return []
                
                merge_ranges = []
                current_start = None
                current_value = None
                
                for row_idx in range(1, num_rows):  # Skip header row
                    # Get current cell value, handling short rows
                    if row_idx < len(values) and column_idx < len(values[row_idx]):
                        cell_value = str(values[row_idx][column_idx]).strip()
                    else:
                        cell_value = ""
                    
                    if current_start is None:
                        # Start tracking a new sequence
                        current_start = row_idx
                        current_value = cell_value
                    elif cell_value == current_value and cell_value != "":
                        # Continue the current sequence (only merge non-empty values)
                        continue
                    else:
                        # End of current sequence, check if we should create a merge
                        if (current_start is not None and 
                            row_idx - current_start > 1 and 
                            current_value != ""):
                            merge_ranges.append((current_start, row_idx))
                            print(f"   📋 {column_name} merge range: rows {current_start+1}-{row_idx} (value: '{current_value}')")
                        
                        # Start new sequence
                        current_start = row_idx
                        current_value = cell_value
                
                # Handle the final sequence
                if (current_start is not None and 
                    num_rows - current_start > 1 and 
                    current_value != ""):
                    merge_ranges.append((current_start, num_rows))
                    print(f"   📋 {column_name} merge range: rows {current_start+1}-{num_rows} (value: '{current_value}')")
                
                return merge_ranges
            
            def create_merge_request(start_row: int, end_row: int, column_idx: int):
                """Create a merge request for the specified range."""
                return {
                            'mergeCells': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': start_row,
                                    'endRowIndex': end_row,
                            'startColumnIndex': column_idx,
                            'endColumnIndex': column_idx + 1
                                },
                                'mergeType': 'MERGE_ALL'
                            }
                }
            
            # Step 1: Process RECRUITMENT_STAGE (independent column)
            recruitment_idx = target_columns.get('RECRUITMENT_STAGE')
            if recruitment_idx is not None:
                recruitment_ranges = find_consecutive_ranges(recruitment_idx, 'RECRUITMENT_STAGE')
                for start_row, end_row in recruitment_ranges:
                    merge_requests.append(create_merge_request(start_row, end_row, recruitment_idx))
                print(f"   🎯 RECRUITMENT_STAGE: {len(recruitment_ranges)} merge ranges found")
            
            # Step 2: Process MAIN_REASON and apply same merges to MAIN_REASON_COUNT
            main_reason_idx = target_columns.get('MAIN_REASON')
            main_count_idx = target_columns.get('MAIN_REASON_COUNT')
            
            if main_reason_idx is not None:
                main_ranges = find_consecutive_ranges(main_reason_idx, 'MAIN_REASON')
                
                # Apply merges to MAIN_REASON column
                for start_row, end_row in main_ranges:
                    merge_requests.append(create_merge_request(start_row, end_row, main_reason_idx))
                
                # Apply same merges to MAIN_REASON_COUNT column if it exists
                if main_count_idx is not None:
                    for start_row, end_row in main_ranges:
                        merge_requests.append(create_merge_request(start_row, end_row, main_count_idx))
                    print(f"   🔗 MAIN_REASON_COUNT follows MAIN_REASON merge pattern")
                
                print(f"   🎯 MAIN_REASON: {len(main_ranges)} merge ranges found and applied to both columns")
            
            # Step 3: Process SUB_REASON and apply same merges to SUB_REASON_COUNT
            sub_reason_idx = target_columns.get('SUB_REASON')
            sub_count_idx = target_columns.get('SUB_REASON_COUNT')
            
            if sub_reason_idx is not None:
                sub_ranges = find_consecutive_ranges(sub_reason_idx, 'SUB_REASON')
                
                # Apply merges to SUB_REASON column
                for start_row, end_row in sub_ranges:
                    merge_requests.append(create_merge_request(start_row, end_row, sub_reason_idx))
                
                # Apply same merges to SUB_REASON_COUNT column if it exists
                if sub_count_idx is not None:
                    for start_row, end_row in sub_ranges:
                        merge_requests.append(create_merge_request(start_row, end_row, sub_count_idx))
                    print(f"   🔗 SUB_REASON_COUNT follows SUB_REASON merge pattern")
                
                print(f"   🎯 SUB_REASON: {len(sub_ranges)} merge ranges found and applied to both columns")
            
            # Execute all merge requests
            if merge_requests:
                print(f"   🔀 Executing {len(merge_requests)} merge operations")
                self._execute_with_retry(
                    f"Merge cells in {sheet_name}",
                    lambda: self.service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': merge_requests}
                    ).execute()
                )
                print(f"   ✅ Successfully merged {len(merge_requests)} cell ranges")
            else:
                print(f"   ℹ️ No consecutive cells to merge in {sheet_name}")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to merge consecutive cells: {e}")
            return False


# =============================
# Metric configuration
# =============================

class Metric:
    def __init__(self, name: str, raw_table: Optional[str], departments: List[str], sheet_ids: Dict[str, str], single_sheet_id: Optional[str] = None, extra_behavior: Optional[str] = None):
        self.name = name
        self.raw_table = raw_table
        self.departments = departments
        self.sheet_ids = sheet_ids  # per-department mapping (if applicable)
        self.single_sheet_id = single_sheet_id  # used when metric uses one spreadsheet for all
        self.extra_behavior = extra_behavior  # e.g., categorizing_raw dual tabs


def build_metrics_registry() -> Dict[str, Metric]:
    """Assemble metric definitions and their target sheets.
    Note: Rule Breaking is left as placeholder with no action.
    """
    # Per-department SA sheets from config
    sa_sheets = SHEETS_CONFIG.get('sa_sheets', {})

    # Snapshot main sheets per department (used later): SHEETS_CONFIG['main_sheets']

    # Static mappings from existing uploaders for other metrics
    clarity_sheets = {
        'CC Resolvers': '167sK6mqyHYMxpUvxyFPk4fDf2ubM_S3t9gmZz7T8xxw',
        'CC Sales': '1hFknnkEbbuiyBAU2OUoAMyKp0qc9zqK3o99_Z2rECLs',
        'Delighters': '1swKfrq3kMV9-u-HLcCZRadoJffiBhndw4yv87LdIvsY',
        'Doctors': '1OZuuxlXi7c0OjWhwLbjxkHh34mZSqYXTBC-Rbo5wVAg',
        'MV Resolvers': '1AUN7_sJkFZXxhz63HM6FngRh-z3S6S5r4_D8hssAUHU',
        'MV Sales': '1xWX9BxmqqNC9q7-nBbcakeyb5kxp0sfXYVtRBu-W28k',
        'African': '1xpVYUS7Or8lPKRLM3m1_JhDWDBWg3lPC5ZD0e2--QA0',
        'Ethiopian': '1u1uZpTuEki3q8-zq86FsYw-GHccqwJShPJBuyk7SSdI',
        'Filipina': '1bC9PaP9DSicy6YqEGMPHeknhbPhNvyw-YK5GvitvFls',
    }

    csai_sheets = {
        'African': '1s-UFaQS-rHdFYD5z3G6iRW4ZiQapdwYY0Gzgl_0UHek',
        'CC Resolvers': '1Aoxijc3LdHGs0tQOeNbeM4JicApdoQ1yk4P4uH7An60',
        'CC Sales': '1odvkCDMC36YZhYOT9kb_MntxJ1PV7YnCbe5cgTTGnbQ',
        'Delighters': '1eOHy1CKst_CO2Rbq6Y4fNCxDAP2wjhEGdK7lH7mcE9M',
        'Doctors': '1RyTaeFvZ9JttxFTl3T0d07Bzs9455-fG7Gh19fgP_L0',
        'Ethiopian': '1y_A1x7ly3ye5PdOW2bi6rJEwv_5TZsxOWCHGj3N4ti8',
        'Filipina': '1ufEy3XS1XmFDIR0ZTnQ6V6LxSYnHz6p9qEODAS2o8PE',
        'MV Resolvers': '188ELCtuoKiYFYuOu8Y41hOGWPexVM03TSSAw8nOSd0Y',
        'MV Sales': '1eti8KL3y44Pmp2Yp-YKit0CkHX39X0VBBG3Kck3gZ_Q',
    }

    # Single-sheet IDs from uploaders
    false_promises_sheet = '12DXUaXOffHVVTj3ErFLmwWnCAEk1e-ljeA6OWACOen4'
    categorizing_sheet_mv = '1hJUaSX75lgtKY8tnqzWVXF7MXUBGhlltTiHBu_xSM10'  # MV Resolvers
    categorizing_sheet_doctors = '1OfEAXeSIbcQu9a9zW3KlZIR8qmhSrr2oUOkRV3LxUDU'  # Doctors
    legal_alignment_sheet = {'MV Resolvers': '1KIn8ZHk1-AxN9JPq1Iy0-bhRNIG3130Uti6EWyC_djQ'}
    call_request_sheet = {'MV Resolvers': '1uer1eNI-RhqY6jnkdpNkhUecISMNNQLGAlGaeVCWmiA'}
    threatening_sheet = {'MV Resolvers': '1ulcfC7Z748YQbX-gH3kBCHxKfcz88ZJPevR28mSgBug'}
    ftr_dept_sheet = {
        'doctors': '19EppEaiNO_9rb4HJpLEQOWvy7dB52INzIJxUEzOqqGQ',
        'delighters': '1x5gOsmg7nJfuKSxuJgGDSVC5x6Hyfse-oL-s62-WZ8k',
        'cc_sales': '16c_wgKVSwQu8L8kTBhid1nHbYK24PJOQFWOZaxPyOFg',
        'cc_resolvers': '1MMBq3OLlWnnuVGoYoSl6YtHC_zu-599OtswQpxCjrbQ',
        'filipina': '1akUETcjIoP-MsPHW9xad3Nbe67ECa7dzSj3usynZ_Ww',
        'african': '1SF9_6ucjNT3rxRyST-s1d-FtyePFMLRPC5vzx94lFoY',
        'ethiopian': '1SpVkmEQDnXyM0KtmUV0vyYfADBiJwowVdg6LdXzUiRI',
        'mv_resolvers': '1_20GZcWM5jLCNYkE8v2_CLxi7vTUvR3IRakWKcWZmfA',
        'mv_sales': '1rSJRPTOPUXKNkXNFXKy-W7SW0mKCxiybgPnpa6p7LA8'
    }
    # Snapshot sheets (daily snapshot) per department
    snapshot_sheets = SHEETS_CONFIG.get('main_sheets', {})

    # Policy escalation per-dept sheets (doctors + mv_resolvers)
    policy_sheets = {
        'mv_resolvers': '1Fv6IzSYEAoLQhfPlFMPUb8mwNP9B723lUDv9dJHODSs',
        'doctors': '1JbZOR18qYmFah-ByM0227clI0_22wfmHwymYR6zwWAE'
    }

    # Loss of Interest: from SHEETS_CONFIG.loss_of_interest_sheets
    loi_sheets = SHEETS_CONFIG.get('loss_of_interest_sheets', {})

    metrics: Dict[str, Metric] = {
        # 1 Sentiment Analysis
        'sentiment_analysis': Metric(
            name='sentiment_analysis', raw_table='sa_raw_data',
            departments=list(DEPARTMENTS.keys()), sheet_ids=sa_sheets
        ),
        # 2 Rule Breaking (placeholder only)
        'rule_breaking': Metric(
            name='rule_breaking', raw_table=None,
            departments=list(DEPARTMENTS.keys()), sheet_ids={}
        ),
        # 3 Categorizing (Doctors, MV Resolvers)
        'categorizing': Metric(
            name='categorizing', raw_table='categorizing_raw_data',
            departments=['Doctors', 'MV Resolvers'],
            sheet_ids={'MV Resolvers': categorizing_sheet_mv, 'Doctors': categorizing_sheet_doctors},
            single_sheet_id=None,
            extra_behavior='categorizing_dual_tabs'
        ),
        # 4 False Promises (MV Resolvers only)
        'false_promises': Metric(
            name='false_promises', raw_table='false_promises_raw_data',
            departments=['MV Resolvers'], sheet_ids={}, single_sheet_id=false_promises_sheet
        ),
        # 5 FTR (MV Resolvers only for raw upload sheet; snapshot later)
        'ftr': Metric(
            name='ftr', raw_table='ftr_raw_data',
            departments=['MV Resolvers'], sheet_ids={'MV Resolvers': ftr_dept_sheet['mv_resolvers']}
        ),
        # 6 Policy to Cause Escalation (Doctors, MV Resolvers)
        'policy_escalation': Metric(
            name='policy_escalation', raw_table='policy_escalation_raw_data',
            departments=['Doctors', 'MV Resolvers'], sheet_ids={}, single_sheet_id=None
        ),
        # 7 Client Suspecting AI (Doctors, MV Resolvers, MV Sales, CC Sales)
        'client_suspecting_ai': Metric(
            name='client_suspecting_ai', raw_table='client_suspecting_ai_raw_data',
            departments=['Doctors', 'MV Resolvers', 'MV Sales', 'CC Sales'], sheet_ids=csai_sheets
        ),
        # 8 Clarity Score (Doctors, MV Resolvers, MV Sales, CC Sales)
        'clarity_score': Metric(
            name='clarity_score', raw_table='clarity_score_raw_data',
            departments=['Doctors', 'MV Resolvers', 'MV Sales', 'CC Sales'], sheet_ids=clarity_sheets
        ),
        # 9 Legal Alignment (MV Resolvers only)
        'legal_alignment': Metric(
            name='legal_alignment', raw_table='legal_alignment_raw_data',
            departments=['MV Resolvers'], sheet_ids=legal_alignment_sheet
        ),
        # 10 Call Request and Rebuttal (MV Resolvers only)
        'call_request': Metric(
            name='call_request', raw_table='call_request_raw_data',
            departments=['MV Resolvers'], sheet_ids=call_request_sheet
        ),
        # 11 Threatening Case Identifier (MV Resolvers only)
        'threatening': Metric(
            name='threatening', raw_table='threatening_raw_data',
            departments=['MV Resolvers'], sheet_ids=threatening_sheet
        ),
        # 12 Misprescription (Doctors only)
        'misprescription': Metric(
            name='misprescription', raw_table='misprescription_raw_data',
            departments=['Doctors'], sheet_ids={'Doctors': '18m4ct5UZ0OBdFRfJfsSJXkjLZOY9JCK5eqqcM333z4E'}
        ),
        # 13 Unnecessary Clinic Rec (Doctors only)
        'unnecessary_clinic_rec': Metric(
            name='unnecessary_clinic_rec', raw_table='unnecessary_clinic_rec_raw_data',
            departments=['Doctors'], sheet_ids={'Doctors': '1dZw0qyFCX3L2XuG-GTdNOO2bR73BB198lfJ-zvf1OSI'}
        ),
        # 14 Loss of Interest (Filipina only)
        'loss_of_interest': Metric(
            name='loss_of_interest', raw_table='loss_interest_raw_data',
            departments=['Filipina'], sheet_ids=loi_sheets,
            single_sheet_id=None,
            extra_behavior='merge_consecutive_cells'
        ),
        # 15 Clinic Recommendation Reason (Doctors only)
        'clinic_recommendation_reason': Metric(
            name='clinic_recommendation_reason', raw_table='CLINIC_RECOMMENDATION_REASON_RAW_DATA',
            departments=['Doctors'], sheet_ids={'Doctors': '1xFSfzL82Cdg3vV5jKlgzsaqsXds0AHftKTglPeuO0qo'}
        ),
        # 16 Policy Violation (All depts use POLICY_VIOLATION_RAW_DATA except MV_Resolvers: combined MISSING/UNCLEAR tables)
        'policy_violation': Metric(
            name='policy_violation', raw_table='POLICY_VIOLATION_RAW_DATA',  # Default table for most departments
            departments=['Filipina', 'MV Resolvers', 'CC Sales'], 
            sheet_ids={
                'Filipina': '1wOSiIAbxgCPUIJXbZFXB6TwCBHAutGeSsjM9JOEgrrY',
                'MV Resolvers': '1-jWEOvfAIuGBrkSQQJIMAaYnp5phFIXHeoc83_Kzqn4',
                'CC Sales': '17qyGa_qLtDZY7TiAvlJ06-4NJv7htNAm1_25rJUJIgs'
            },
            extra_behavior='policy_violation_combined'
        ),

        'intervention': Metric(
            name='intervention',
            raw_table='CATEGORIZING_RAW_DATA',
            departments=['Doctors'],
            sheet_ids={'Doctors': '1igr9_hHgg2736wqgb9H0gfHPTMKV3I4Es5h7YlEktcs'},
            single_sheet_id=None,
            extra_behavior='intervention_categorizing_filter'
        ),

        'transfer_escalation': Metric(
            name='transfer_escalation',
            raw_table='TRANSFER_ESCALATION_RAW_DATA',
            departments=['CC Sales'],
            sheet_ids={'CC Sales': '1-iP5lEoiaVa4IpYUpxfRXmHcXKIqk2aWF4ghBw_pzmk'},
            single_sheet_id=None,
            extra_behavior=None
        ),

        'transfer_known_flow': Metric(
            name='transfer_known_flow',
            raw_table='TRANSFER_KNOWN_FLOW_RAW_DATA',
            departments=['CC Sales'],
            sheet_ids={'CC Sales': '1GgvVS1fAUKgt5coTf-QSBnUP5RDBdfGdO109W-cR27Y'},
            single_sheet_id=None,
            extra_behavior=None
        ),

        'tools': Metric(
            name='tools',
            raw_table=None,  # Multiple raw tables: MISSING_TOOL_RAW_DATA, WRONG_TOOL_RAW_DATA
            departments=['Filipina', 'CC Sales', 'Doctors', 'MV Resolvers'],
            sheet_ids={
                'Filipina': '1H5ZGg6Uw74Crk4JAi_JPgCe2wh7cET8NDLl5YYN_KS0',
                'CC Sales': '1u7r35koc9LTchOl009OPPPoAn6vbRyjxOMJUJqwhgwk',
                'Doctors': '1k6Ysk-2tKNANLGpkHt6DFei2vPR1SPhZSOWXwf7D508',
                'MV Resolvers': '1j2S0Ql1E880IcC0EnMzReQs-xttW9h05P2dAQ4U4U4E',
            },
            single_sheet_id=None,
            extra_behavior='tools_quad_tabs'
        ),
        
        'shadowing_automation': Metric(
            name='shadowing_automation',
            raw_table='UNIQUE_ISSUES_RAW_DATA',
            departments=['MV Resolvers'],
            sheet_ids={'MV Resolvers': '187CfNw9qnClDj6_HmOwQ-lWLktll5u4IV-itdsUSGOY'},
            single_sheet_id=None,
            extra_behavior='shadowing_dual_tabs'
        ),
    }

    # Attach snapshot sheets mapping for later lookups
    metrics['_snapshot_sheets'] = Metric(name='_snapshot', raw_table=None, departments=[], sheet_ids=snapshot_sheets)
    metrics['_policy_sheets'] = Metric(name='_policy_sheets', raw_table=None, departments=[], sheet_ids=policy_sheets)

    return metrics


# =============================
# Snapshot updater
# =============================

SNAPSHOT_SHEET_CANDIDATES = ['Data', 'Sheet1', 'Main']

SNAPSHOT_COLUMN_CANDIDATES: Dict[str, List[str]] = {
    'WEIGHTED_AVG_NPS': ['Sentiment Analysis', 'NPS', 'NPS Score', 'Sentiment Analysis NPS'],
    'CLIENT_SUSPECTING_AI_PERCENTAGE': ['Clients Suspecting AI', '% Client suspecting AI', 'Client suspecting AI %'],
    'ESCALATION_RATE': ['Escalation Outcome', '% Escalation', 'Escalation %', 'Policy Escalation %'],  # Legal alignment metric
    'POLICY_ESCALATION_PERCENTAGE': ['Policy to cause escalation', '% Escalation', 'Escalation %', 'Policy Escalation %'],
    'LEGAL_CONCERNS_PERCENTAGE': [
        'Clients Questioning Legalties',
        'Clients Questioning Legalities',
        'Clients Questioning Legalties %',
        'Clients Questioning Legalities %',
        '% Legal Concerns', 'Legal Alignment %', 'Legal Concerns %'
    ],
    'CALL_REQUEST_RATE': ['Call Request', '% Call Request', 'Call Request %'],
    'REBUTTAL_RESULT_RATE': ['Rebuttal Result', '% Rebuttal Success', 'Rebuttal %', 'Rebuttal Result %'],
    'INTERVENTION_PERCENTAGE': ['% Intervention', 'Intervention %', 'Intervention'],
    'TRANSFER_PERCENTAGE': ['% Transfer', 'Transfer %', 'Transfer'],
    'FALSE_PROMISES_PERCENTAGE': ['% False Promises', 'False Promises %', 'False Promises'],
    'FTR_PERCENTAGE': ['FTR'],
    'THREATENING_PERCENTAGE': ['Threatening Case Identifier', '% Threatening', 'Threatening %'],
    'CLARITY_SCORE_PERCENTAGE': ['Clarity Score', '% Clarity Score', 'Clarity Score %'],
    'MISPRESCRIPTION_PERCENTAGE': ['Medical mis-prescriptions', 'Misprescription', '% Misprescription', 'Medical misprescriptions'],
    'UNNECESSARY_CLINIC_REC_PERCENTAGE': ['Unnecessary clinic recommendations', 'Unnecessary Clinic Rec', '% Unnecessary Clinic Rec'],
    # Accept doctors_summary field naming without _REC_
    'UNNECESSARY_CLINIC_PERCENTAGE': ['Unnecessary clinic recommendations', 'Unnecessary Clinic Rec', '% Unnecessary Clinic Rec'],
    'MISSING_POLICY_COMBINED': ['Missing policy', 'Missing Policy', '% Missing Policy', 'Missing Policy %'],
    'UNCLEAR_POLICY_COMBINED': ['Unclear policy', 'Unclear Policy', '% Unclear Policy', 'Unclear Policy %'],
    'WRONG_POLICY_COMBINED': ['Wrong policy', 'Wrong Policy', '% Wrong Policy', 'Wrong Policy %'],
    'TRANSFER_ESCALATION_COMBINED': ['Transfers due to escalations', 'Transfer Escalation', '% Transfer Escalation', 'Transfer Escalation %'],
    'TRANSFER_KNOWN_FLOW_COMBINED': ['Transfers due to known flows', 'Transfer Known Flow', '% Transfer Known Flow', 'Transfer Known Flow %'],
}

PERCENT_FIELDS = {
    'CLIENT_SUSPECTING_AI_PERCENTAGE',
    'ESCALATION_RATE',
    'POLICY_ESCALATION_PERCENTAGE',
    'LEGAL_CONCERNS_PERCENTAGE',
    'CALL_REQUEST_RATE',
    'REBUTTAL_RESULT_RATE',
    'INTERVENTION_PERCENTAGE',
    'TRANSFER_PERCENTAGE',
    'FALSE_PROMISES_PERCENTAGE',
    'FTR_PERCENTAGE',
    'THREATENING_PERCENTAGE',
    'CLARITY_SCORE_PERCENTAGE',
    'MISPRESCRIPTION_PERCENTAGE',
    'UNNECESSARY_CLINIC_REC_PERCENTAGE',
    'UNNECESSARY_CLINIC_PERCENTAGE',
    'MISSING_POLICY_COMBINED',
    'UNCLEAR_POLICY_COMBINED',
    'WRONG_POLICY_COMBINED',
    'TRANSFER_ESCALATION_COMBINED',
    'TRANSFER_KNOWN_FLOW_COMBINED',
}


def update_snapshot_sheet(gc: SheetsClient, snapshot_sheet_id: str, dept_name: str, date_str: str, summary_row: pd.Series, allowed_fields: Optional[set] = None, conn=None) -> None:
    print(f"📝 Updating snapshot sheet for {dept_name}")
    date_row, found_sheet = gc.find_date_row(snapshot_sheet_id, date_str, SNAPSHOT_SHEET_CANDIDATES)
    if not date_row or not found_sheet:
        # Try to append the date row in column A
        date_row, found_sheet = gc.ensure_date_row(snapshot_sheet_id, date_str, SNAPSHOT_SHEET_CANDIDATES)
        if not date_row or not found_sheet:
            print(f"   ❌ Date {date_str} not found and could not be added for {dept_name}")
            return

    # For each supported summary field, try to update the corresponding column
    for field, candidates in SNAPSHOT_COLUMN_CANDIDATES.items():
        if allowed_fields is not None and field not in allowed_fields:
            continue
        
        # Handle combined policy fields that don't exist directly in summary_row
        if field == 'MISSING_POLICY_COMBINED':
            a_value = summary_row.get('A_MISSING_POLICY_PERCENTAGE')
            b_value = summary_row.get('B_MISSING_POLICY_PERCENTAGE')
            if a_value is None and b_value is None:
                continue
            value = (a_value, b_value)  # Store as tuple for special processing
        elif field == 'UNCLEAR_POLICY_COMBINED':
            a_value = summary_row.get('A_UNCLEAR_POLICY_PERCENTAGE')
            b_value = summary_row.get('B_UNCLEAR_POLICY_PERCENTAGE')
            if a_value is None and b_value is None:
                continue
            value = (a_value, b_value)  # Store as tuple for special processing
        elif field == 'WRONG_POLICY_COMBINED':
            a_value = summary_row.get('A_WRONG_POLICY_PERCENTAGE')
            b_value = summary_row.get('B_WRONG_POLICY_PERCENTAGE')
            if a_value is None and b_value is None:
                continue
            value = (a_value, b_value)  # Store as tuple for special processing
        elif field == 'TRANSFER_ESCALATION_COMBINED':
            a_value = summary_row.get('TRANSFER_ESCALATION_PERCENTAGE_A')
            b_value = summary_row.get('TRANSFER_ESCALATION_PERCENTAGE_B')
            if a_value is None and b_value is None:
                continue
            value = (a_value, b_value)  # Store as tuple for special processing
        elif field == 'TRANSFER_KNOWN_FLOW_COMBINED':
            a_value = summary_row.get('TRANSFER_KNOWN_FLOW_PERCENTAGE_A')
            b_value = summary_row.get('TRANSFER_KNOWN_FLOW_PERCENTAGE_B')
            if a_value is None and b_value is None:
                continue
            value = (a_value, b_value)  # Store as tuple for special processing
        else:
            if field not in summary_row.index:
                continue
            value = summary_row[field]
        
        display_value = value

        # Special combined formatting for Doctors snapshot fields
        if field in { 'MISPRESCRIPTION_PERCENTAGE' }:
            percent = value
            count = summary_row.get('MISPRESCRIPTION_COUNT')
            # format percent without scaling
            try:
                if isinstance(percent, str) and '%' in percent:
                    percent_str = percent
                else:
                    pnum = float(percent)
                    pstr_raw = ("%f" % pnum).rstrip('0').rstrip('.')
                    percent_str = f"{pstr_raw}%"
            except Exception:
                percent_str = str(percent)
            count_str = str(count) if count is not None else ''
            display_value = f"{count_str} ({percent_str} of prescriptions sent)".strip()
        elif field in { 'UNNECESSARY_CLINIC_PERCENTAGE', 'UNNECESSARY_CLINIC_REC_PERCENTAGE' }:
            percent = value
            count = summary_row.get('COULD_AVOID_COUNT')
            try:
                if isinstance(percent, str) and '%' in percent:
                    percent_str = percent
                else:
                    pnum = float(percent)
                    pstr_raw = ("%f" % pnum).rstrip('0').rstrip('.')
                    percent_str = f"{pstr_raw}%"
            except Exception:
                percent_str = str(percent)
            count_str = str(count) if count is not None else ''
            display_value = f"{count_str} ({percent_str} of clinics recommended)".strip()
        elif field == 'CALL_REQUEST_RATE':
            # Call Request: CALL_REQUEST_COUNT (CALL_REQUEST_RATE)
            percent = value
            count = summary_row.get('CALL_REQUEST_COUNT')
            try:
                if isinstance(percent, str) and '%' in percent:
                    percent_str = percent
                else:
                    pnum = float(percent)
                    pstr_raw = ("%f" % pnum).rstrip('0').rstrip('.')
                    percent_str = f"{pstr_raw}%"
            except Exception:
                percent_str = str(percent)
            count_str = str(count) if count is not None else ''
            display_value = f"{count_str} ({percent_str})".strip()
        elif field == 'REBUTTAL_RESULT_RATE':
            # Rebuttal Result: NO_RETENTION_COUNT (REBUTTAL_RESULT_RATE)
            percent = value
            count = summary_row.get('NO_RETENTION_COUNT')
            try:
                if isinstance(percent, str) and '%' in percent:
                    percent_str = percent
                else:
                    pnum = float(percent)
                    pstr_raw = ("%f" % pnum).rstrip('0').rstrip('.')
                    percent_str = f"{pstr_raw}%"
            except Exception:
                percent_str = str(percent)
            count_str = str(count) if count is not None else ''
            display_value = f"{count_str} ({percent_str})".strip()
        elif dept_name == 'Doctors' and field in {'CLIENT_SUSPECTING_AI_PERCENTAGE', 'CLARITY_SCORE_PERCENTAGE'} and conn is not None:
            # Doctors-specific format: count (percentage%) where count is out of total chats from sa_raw_data
            # Derive fraction from percentage value (supports either fraction or percent number)
            percent = value
            try:
                if isinstance(percent, str) and '%' in percent:
                    # strip and convert to fraction
                    pnum = float(percent.replace('%', '').strip())
                    frac = pnum / 100.0
                    percent_str = f"{pnum}%"
                else:
                    pnum = float(percent)
                    if pnum > 1.0:
                        frac = pnum / 100.0
                        percent_str = f"{pnum}%"
                    else:
                        frac = pnum
                        # Render without scaling decimals
                        pstr_raw = ("%f" % pnum).rstrip('0').rstrip('.')
                        percent_str = f"{pstr_raw}%"
            except Exception:
                frac = 0.0
                percent_str = str(percent)
            total_chats = fetch_total_chats(conn, date_str, dept_name)
            counted = int(round(frac * total_chats))
            display_value = f"{counted} ({percent_str})"
        elif field in {'MISSING_POLICY_COMBINED', 'UNCLEAR_POLICY_COMBINED', 'WRONG_POLICY_COMBINED', 'TRANSFER_ESCALATION_COMBINED', 'TRANSFER_KNOWN_FLOW_COMBINED'}:
            # Policy violation & transfer combined formatting: "A_value (B_value)"
            a_value, b_value = value  # Unpack the tuple
            
            def format_percentage(val):
                if val is None or pd.isna(val):
                    return "N/A"
                try:
                    if isinstance(val, str) and '%' in val:
                        return val
                    else:
                        num = float(val)
                        # Do NOT scale decimals to percent; append % as-is
                        s = ("%f" % num).rstrip('0').rstrip('.')
                        return f"{s}%"
                except Exception:
                    return str(val) if val is not None else "N/A"
            
            a_str = format_percentage(a_value)
            b_str = format_percentage(b_value)
            display_value = f"{a_str} ({b_str})"
        else:
            # Default formatting based on metric type: percent vs scalar
            try:
                if isinstance(value, str) and '%' in value:
                    display_value = value
                else:
                    num = float(value)
                    if field in PERCENT_FIELDS:
                        # Do NOT scale decimals to percent; append % as-is (e.g., 0.6 -> 0.6%)
                        s = ("%f" % num).rstrip('0').rstrip('.')
                        display_value = f"{s}%"
                    else:
                        display_value = f"{num:.2f}"
            except Exception:
                display_value = value

        col, sheet_for_col = gc.find_header_column(snapshot_sheet_id, candidates, [found_sheet])
        if not col:
            # Try across candidates sheets
            col, sheet_for_col = gc.find_header_column(snapshot_sheet_id, candidates, SNAPSHOT_SHEET_CANDIDATES)
        if col and sheet_for_col:
            gc.update_cell(snapshot_sheet_id, sheet_for_col, date_row, col, display_value)
        else:
            print(f"   ⚠️ Column not found for field '{field}' (tried headers: {candidates})")


# =============================
# Per-metric raw upload
# =============================

TARGET_LLM_COLUMNS = ['CONVERSATION_ID', 'MODEL_NAME', 'CONVERSATION_CONTENT', 'LLM_RESPONSE']

# Additional columns for specific departments
DEPT_SPECIFIC_COLUMNS = {
    'Filipina': ['SYSTEM_PROMPT_SNAPSHOT'],
    'African': ['SYSTEM_PROMPT_SNAPSHOT'], 
    'Ethiopian': ['SYSTEM_PROMPT_SNAPSHOT'],
}

COLUMN_CANDIDATES = {
    'CONVERSATION_ID': ['CONVERSATION_ID', 'conversation_id', 'Conversation_ID', 'conversationId', 'CONV_ID', 'conv_id'],
    'MODEL_NAME': ['MODEL_NAME', 'model_name', 'MODEL', 'model', 'MODEL_USED'],
    'CONVERSATION_CONTENT': ['CONVERSATION_CONTENT', 'conversation_content', 'CONVERSATION', 'conversation', 'CONTENT', 'content'],
    'LLM_RESPONSE': ['LLM_RESPONSE', 'llm_response', 'LLM_OUTPUT', 'llm_output', 'RESPONSE', 'response'],
    'SYSTEM_PROMPT_SNAPSHOT': ['SYSTEM_PROMPT_SNAPSHOT', 'system_prompt_snapshot', 'SYSTEM_PROMPT', 'system_prompt', 'PROMPT_SNAPSHOT', 'prompt_snapshot'],
    
    # Shadowing automation raw data columns
    'ISSUE_ID': ['ISSUE_ID', 'issue_id', 'Issue_ID'],
    'REPORTER': ['REPORTER', 'reporter', 'Reporter'],
    'ISSUE_STATUS': ['ISSUE_STATUS', 'issue_status', 'Issue_Status'],
    'ISSUE_TYPE': ['ISSUE_TYPE', 'issue_type', 'Issue_Type'],
    'CREATION_DATE': ['CREATION_DATE', 'creation_date', 'Creation_Date'],
    'ISSUE_DESCRIPTION': ['ISSUE_DESCRIPTION', 'issue_description', 'Issue_Description'],
    'LLM_OUTPUT_RAW': ['LLM_OUTPUT_RAW', 'llm_output_raw', 'LLM_Output_Raw', 'LLM_OUTPUT', 'llm_output'],
}


def filter_llm_columns(df: pd.DataFrame, department: str = None) -> pd.DataFrame:
    """Return DataFrame with only TARGET_LLM_COLUMNS plus department-specific columns, mapping common aliases.
    Adds empty columns if not found; logs mapping results.
    """
    result = pd.DataFrame()
    src_cols_lower = {c.lower(): c for c in df.columns}

    def pick_column(candidates: list[str]) -> Optional[str]:
        for cand in candidates:
            if cand in df.columns:
                return cand
            low = cand.lower()
            if low in src_cols_lower:
                return src_cols_lower[low]
        return None

    # Start with base columns
    target_columns = TARGET_LLM_COLUMNS.copy()
    
    # Add department-specific columns if department is specified
    if department and department in DEPT_SPECIFIC_COLUMNS:
        dept_columns = DEPT_SPECIFIC_COLUMNS[department]
        target_columns.extend(dept_columns)
        print(f"   📋 Including department-specific columns for {department}: {dept_columns}")

    for target in target_columns:
        cand_list = COLUMN_CANDIDATES.get(target, [target])
        src = pick_column(cand_list)
        if src is not None:
            result[target] = df[src]
            if target != src:
                print(f"   ↪︎ Mapped column '{src}' → '{target}'")
        else:
            print(f"   ⚠️ Source column for '{target}' not found; filling empty")
            result[target] = ''

    return result


def filter_shadowing_columns(df: pd.DataFrame, target_columns: List[str]) -> pd.DataFrame:
    """Return DataFrame with only shadowing automation specific columns, mapping common aliases.
    Adds empty columns if not found; logs mapping results.
    """
    result = pd.DataFrame()
    src_cols_lower = {c.lower(): c for c in df.columns}

    def pick_column(candidates: List[str]) -> Optional[str]:
        for cand in candidates:
            if cand in df.columns:
                return cand
            low = cand.lower()
            if low in src_cols_lower:
                return src_cols_lower[low]
        return None

    for target in target_columns:
        cand_list = COLUMN_CANDIDATES.get(target, [target])
        src = pick_column(cand_list)
        if src is not None:
            result[target] = df[src]
            if target != src:
                print(f"   ↪︎ Mapped column '{src}' → '{target}'")
        else:
            print(f"   ⚠️ Source column for '{target}' not found; filling empty")
            result[target] = ''

    return result


SUMMARY_COLUMN_TARGETS = {
    'policy_escalation': ['POLICY', 'COUNT', 'PERCENTAGE'],
    'categorizing': [
        'CATEGORY', 'COUNT', 'CATEGORY_PCT', 'COVERAGE_PER_CATEGORY_PCT',
        'INTERVENTION_BY_AGENT_PCT', 'TRANSFERRED_BY_BOT_PCT', 'CHATS_NOT_HANDLED_PCT'
    ],
    'categorizing_doctors': [
        'CATEGORY', 'COUNT', 'CATEGORY_PCT',
        'CLINIC_RECOMMENDATION_COUNT', 'CLINIC_RECOMMENDATION_PCT',
        'OTC_MEDICATION_COUNT', 'OTC_MEDICATION_PCT'
    ],
    'loss_of_interest': ['RECRUITMENT_STAGE', 'MAIN_REASON', 'MAIN_REASON_COUNT', 'SUB_REASON', 'SUB_REASON_COUNT'],
    'clinic_recommendation_reason': ['CATEGORY_NAME', 'NUMBER_OF_CHATS'],
    'shadowing_automation': ['UNIQUE_ISSUES', 'CATEGORY', 'SEVERITY', 'FREQUENCY', 'STATUS', 'ISSUE_IDS'],
}

SUMMARY_COLUMN_CANDIDATES = {
    'POLICY': ['POLICY', 'Policy', 'policy'],
    'COUNT': ['COUNT', 'Count', 'count', 'TOTAL', 'total'],
    'PERCENTAGE': ['PERCENTAGE', 'Percentage', 'percentage', '%', 'PCT', 'pct'],

    'CATEGORY': ['CATEGORY', 'Category', 'category'],
    'CATEGORY_PCT': ['CATEGORY_PCT', 'Category_Pct', 'Category %', 'CATEGORY_%', 'CATEGORY_PERCENT', 'category_pct'],
    'COVERAGE_PER_CATEGORY_PCT': ['COVERAGE_PER_CATEGORY_PCT', 'Coverage_Per_Category_Pct', 'Coverage per Category %', 'coverage_per_category_pct'],
    'INTERVENTION_BY_AGENT_PCT': ['INTERVENTION_BY_AGENT_PCT', 'Intervention_By_Agent_Pct', 'Intervention by Agent %', 'intervention_by_agent_pct'],
    'TRANSFERRED_BY_BOT_PCT': ['TRANSFERRED_BY_BOT_PCT', 'Transferred_By_Bot_Pct', 'Transferred by Bot %', 'transferred_by_bot_pct'],
    # Updated Snowflake column name (keep backward compatible candidates)
    'CHATS_NOT_HANDLED_PCT': ['CHATS_NOT_HANDLED_PCT', 'ALL_CHATS_NOT_HANDLED_PCT', 'All_Chats_Not_Handled_Pct', 'All Chats Not Handled %', 'chats_not_handled_pct', 'all_chats_not_handled_pct'],

    'CLINIC_RECOMMENDATION_COUNT': ['CLINIC_RECOMMENDATION_COUNT', 'Clinic_Recommendation_Count', 'clinic_recommendation_count'],
    'CLINIC_RECOMMENDATION_PCT': ['CLINIC_RECOMMENDATION_PCT', 'Clinic_Recommendation_Pct', 'Clinic Recommendation %', 'clinic_recommendation_pct'],
    'OTC_MEDICATION_COUNT': ['OTC_MEDICATION_COUNT', 'OTC_Medication_Count', 'otc_medication_count'],
    'OTC_MEDICATION_PCT': ['OTC_MEDICATION_PCT', 'OTC_Medication_Pct', 'OTC Medication %', 'otc_medication_pct'],

    'RECRUITMENT_STAGE': ['RECRUITMENT_STAGE', 'Recruitment_Stage', 'recruitment_stage'],
    'MAIN_REASON': ['MAIN_REASON', 'Main_Reason', 'main_reason'],
    'SUB_REASON': ['SUB_REASON', 'Sub_Reason', 'sub_reason'],
    'MAIN_REASON_COUNT': ['MAIN_REASON_COUNT', 'Main_Reason_Count', 'main_reason_count'],
    'MAIN_REASON_PCT': ['MAIN_REASON_PCT', 'Main_Reason_Pct', 'Main Reason %', 'main_reason_pct'],
    'SUB_REASON_COUNT': ['SUB_REASON_COUNT', 'Sub_Reason_Count', 'sub_reason_count'],
    'SUB_REASON_PCT': ['SUB_REASON_PCT', 'Sub_Reason_Pct', 'Sub Reason %', 'sub_reason_pct'],

    'CATEGORY_NAME': ['CATEGORY_NAME', 'Category_Name', 'category_name', 'Category Name'],
    'NUMBER_OF_CHATS': ['NUMBER_OF_CHATS', 'Number_Of_Chats', 'number_of_chats', 'Number of Chats', 'Chat Count', 'CHAT_COUNT'],
    
    # Shadowing automation columns
    'UNIQUE_ISSUES': ['UNIQUE_ISSUES', 'unique_issues', 'Unique_Issues'],
    'SEVERITY': ['SEVERITY', 'severity', 'Severity'],
    'FREQUENCY': ['FREQUENCY', 'frequency', 'Frequency', 'COUNT', 'Count', 'count'],
    'STATUS': ['STATUS', 'status', 'Status'],
    'ISSUE_IDS': ['ISSUE_IDS', 'issue_ids', 'Issue_IDs', 'Issue IDs'],
}


def filter_summary_columns(metric_key: str, df: pd.DataFrame) -> pd.DataFrame:
    """Filter summary DataFrame to metric-specific required columns.
    Missing columns are added empty; alias mapping applied.
    """
    targets = SUMMARY_COLUMN_TARGETS.get(metric_key)
    if not targets:
        return df

    result = pd.DataFrame()
    src_cols_lower = {c.lower(): c for c in df.columns}

    def pick_column(candidates: List[str]) -> Optional[str]:
        for cand in candidates:
            if cand in df.columns:
                return cand
            low = cand.lower()
            if low in src_cols_lower:
                return src_cols_lower[low]
        return None

    for target in targets:
        cands = SUMMARY_COLUMN_CANDIDATES.get(target, [target])
        src = pick_column(cands)
        if src is not None:
            result[target] = df[src]
            if target != src:
                print(f"   ↪︎ Mapped summary column '{src}' → '{target}'")
        else:
            print(f"   ⚠️ Summary source column for '{target}' not found; filling empty")
            result[target] = ''

    return result

def prettify_summary_headers(metric_key: str, dept: str, df: pd.DataFrame) -> pd.DataFrame:
    """Rename summary headers to readable labels for specific cases.
    Currently: MV Resolvers categorizing summary → human-friendly column names.
    """
    if metric_key == 'categorizing' and dept == 'MV Resolvers' and not df.empty:
        rename_map = {
            'CATEGORY_PCT': 'CATEGORY %',
            'COVERAGE_PER_CATEGORY_PCT': 'COVERAGE PER CATEGORY %',
            'INTERVENTION_BY_AGENT_PCT': 'INTERVENTION BY AGENT %',
            'TRANSFERRED_BY_BOT_PCT': 'TRANSFERRED BY BOT %',
            'CHATS_NOT_HANDLED_PCT': 'ALL CHATS NOT HANDLED %',
        }
        pretty = df.copy()
        # Only rename columns that exist
        existing = {k: v for k, v in rename_map.items() if k in pretty.columns}
        if existing:
            pretty = pretty.rename(columns=existing)
        return pretty
    return df


def format_loss_of_interest_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Format loss of interest summary data with count(percentage%) format.
    Uses percentage data as-is from Snowflake without additional formatting.
    """
    if df.empty:
        return df
    
    formatted_df = df.copy()
    
    # Format MAIN_REASON_COUNT to show count(percentage%) 
    # Use percentage data directly from Snowflake without additional formatting
    if 'MAIN_REASON_COUNT' in formatted_df.columns and 'MAIN_REASON_PCT' in formatted_df.columns:
        for idx, row in formatted_df.iterrows():
            count = row.get('MAIN_REASON_COUNT')
            pct = row.get('MAIN_REASON_PCT')
            
            if pd.notna(count) and pd.notna(pct):
                # Use percentage as-is from Snowflake (already formatted)
                pct_str = str(pct) if pct is not None else ''
                formatted_df.at[idx, 'MAIN_REASON_COUNT'] = f"{count} ({pct_str})"
    
    # Format SUB_REASON_COUNT to show count(percentage%)
    # Use percentage data directly from Snowflake without additional formatting  
    if 'SUB_REASON_COUNT' in formatted_df.columns and 'SUB_REASON_PCT' in formatted_df.columns:
        for idx, row in formatted_df.iterrows():
            count = row.get('SUB_REASON_COUNT')
            pct = row.get('SUB_REASON_PCT')
            
            if pd.notna(count) and pd.notna(pct):
                # Use percentage as-is from Snowflake (already formatted)
                pct_str = str(pct) if pct is not None else ''
                formatted_df.at[idx, 'SUB_REASON_COUNT'] = f"{count} ({pct_str})"
    
    return formatted_df
def upload_metric_raw(gc: SheetsClient, metric: Metric, policy_sheet_ids: Dict[str, str], date_str: str, conn) -> None:
    if not metric.raw_table and metric.extra_behavior not in ['policy_violation_combined', 'intervention_categorizing_filter', 'tools_quad_tabs', 'merge_consecutive_cells']:
        print(f"➡️  {metric.name}: Placeholder only. Skipping upload.")
        return

    print(f"➡️  Processing metric: {metric.name}")
    # Optional department filter via env var ONLY_DEPARTMENTS (comma-separated)
    only_depts_env = os.getenv('ONLY_DEPARTMENTS', '').strip()
    only_depts_set = None
    if only_depts_env:
        # Normalize tokens like MV_Resolvers to canonical display names
        norm = [normalize_department_input(d) for d in only_depts_env.split(',') if d.strip()]
        only_depts_set = {d.lower() for d in norm}

    for dept in metric.departments:
        if only_depts_set and dept.lower() not in only_depts_set:
            continue
        print(f" - Department: {dept}")
        # Skip initial table fetch for special behaviors (uses different logic)
        if metric.extra_behavior in ['policy_violation_combined', 'intervention_categorizing_filter', 'tools_quad_tabs']:
            df = pd.DataFrame()  # Empty df, will be handled specially
        else:
            try:
                # Choose effective raw table (some metrics use department-specific tables)
                effective_raw_table = metric.raw_table
                if metric.name == 'misprescription' and dept == 'Doctors':
                    effective_raw_table = 'doctors_misprescription_raw_data'
                if metric.name == 'unnecessary_clinic_rec' and dept == 'Doctors':
                    effective_raw_table = 'doctors_unnecessary_clinic_raw_data'
                if metric.name == 'categorizing' and dept == 'Doctors':
                    effective_raw_table = 'doctors_categorizing_raw_data'

                df = fetch_table_df(conn, effective_raw_table, date_str, dept)
            except Exception as e:
                print(f"   ❌ Snowflake query failed for {dept}: {e}")
                continue

            if df.empty:
                print("   ℹ️ No raw data found; skipping sheet upload")
                continue

        # Determine target spreadsheet id
        spreadsheet_id = None
        sheet_name = date_str

        if metric.single_sheet_id:
            spreadsheet_id = metric.single_sheet_id
        elif metric.name == 'policy_escalation':
            # Two per-dept sheet ids
            dept_key = clean_department_key(dept)
            spreadsheet_id = policy_sheet_ids.get(dept_key)
        else:
            # Per-department mapping
            spreadsheet_id = metric.sheet_ids.get(dept)

        if not spreadsheet_id:
            print(f"   ⚠️ No spreadsheet configured for {dept} in metric '{metric.name}'")
            continue

        if not gc.create_sheet_if_missing(spreadsheet_id, sheet_name):
            continue

        # Special handling for categorizing and policy_escalation summaries
        if metric.extra_behavior == 'categorizing_dual_tabs':
            # Both MV Resolvers and Doctors have categorizing; both can produce a summary
            report_tab = sheet_name
            raw_tab = f"{sheet_name}-RAW"

            # Determine table names per department
            if dept == 'Doctors':
                cat_raw_table = 'doctors_categorizing_raw_data'
                cat_summary_table = 'doctors_categorizing_summary'
            else:
                cat_raw_table = 'categorizing_raw_data'
                cat_summary_table = 'categorizing_summary'

            # Re-fetch raw using the correct raw table when overriding
            try:
                df = fetch_table_df(conn, cat_raw_table, date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching {cat_raw_table}: {e}")
                df = pd.DataFrame()

            # Fetch summary table if available
            try:
                summary_df = fetch_table_df(conn, cat_summary_table, date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching {cat_summary_table}: {e}")
                summary_df = pd.DataFrame()

            # Upload summary to report tab if present, else fallback to filtered raw
            if not summary_df.empty and gc.create_sheet_if_missing(spreadsheet_id, report_tab):
                key = 'categorizing_doctors' if dept == 'Doctors' else 'categorizing'
                filtered = filter_summary_columns(key, summary_df)
                filtered = prettify_summary_headers('categorizing' if dept == 'MV Resolvers' else key, dept, filtered)
                gc.upload_dataframe(spreadsheet_id, report_tab, filtered)
            else:
                if gc.create_sheet_if_missing(spreadsheet_id, report_tab):
                    gc.upload_dataframe(spreadsheet_id, report_tab, filter_llm_columns(df, dept))

            # Always upload filtered raw to -RAW tab for traceability
            if gc.create_sheet_if_missing(spreadsheet_id, raw_tab):
                gc.upload_dataframe(spreadsheet_id, raw_tab, filter_llm_columns(df, dept))
        elif metric.name == 'policy_escalation':
            # Upload raw to date tab
            gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(df, dept))
            # Also upload summary to date-summary tab if available
            try:
                pe_summary = fetch_table_df(conn, 'policy_escalation_summary', date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching policy_escalation_summary: {e}")
                pe_summary = pd.DataFrame()
            if not pe_summary.empty:
                summary_tab = f"{sheet_name}-summary"
                if gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                    gc.upload_dataframe(spreadsheet_id, summary_tab, filter_summary_columns('policy_escalation', pe_summary))
        elif metric.name == 'loss_of_interest':
            # Upload raw first
            gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(df, dept))
            # Then upload summary tab if available
            try:
                loi_summary = fetch_table_df(conn, 'LOSS_INTEREST_SUMMARY', date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching loss_of_interest_summary: {e}")
                loi_summary = pd.DataFrame()
            if not loi_summary.empty:
                summary_tab = f"{sheet_name}-summary"
                if gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                    # Format count columns to show count(percentage%) before filtering
                    formatted_loi = format_loss_of_interest_summary(loi_summary)
                    # Filter to get only the target columns (now with formatted count columns)
                    filtered_loi = filter_summary_columns('loss_of_interest', formatted_loi)
                    gc.upload_dataframe(spreadsheet_id, summary_tab, filtered_loi)
                    
                    # Special behavior: merge consecutive cells for Filipina
                    if dept == 'Filipina':
                        gc.merge_consecutive_cells(spreadsheet_id, summary_tab)
        elif metric.name == 'clinic_recommendation_reason':
            # Upload raw first
            gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(df, dept))
            # Then upload summary tab if available
            try:
                crr_summary = fetch_table_df(conn, 'CLINIC_RECOMMENDATION_REASON_SUMMARY', date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching clinic_recommendation_reason_summary: {e}")
                crr_summary = pd.DataFrame()
            if not crr_summary.empty:
                summary_tab = f"{sheet_name}-summary"
                if gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                    gc.upload_dataframe(spreadsheet_id, summary_tab, filter_summary_columns('clinic_recommendation_reason', crr_summary))
        elif metric.extra_behavior == 'intervention_categorizing_filter':
            # Intervention using categorizing_raw_data with department filter
            try:
                # Fetch categorizing_raw_data filtered for the department
                intervention_df = fetch_table_df(conn, 'CATEGORIZING_RAW_DATA', date_str, dept)
                if not intervention_df.empty:
                    gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(intervention_df, dept))
                else:
                    print(f"   ℹ️ No categorizing data found for {dept}")
            except Exception as e:
                print(f"   ❌ Failed fetching CATEGORIZING_RAW_DATA: {e}")
            
            # Also upload summary to date-summary tab from categorizing_summary
            try:
                intervention_summary = fetch_table_df(conn, 'categorizing_summary', date_str, dept)
            except Exception as e:
                print(f"   ❌ Failed fetching categorizing_summary: {e}")
                intervention_summary = pd.DataFrame()
            
            if not intervention_summary.empty:
                summary_tab = f"{sheet_name}-summary"
                if gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                    gc.upload_dataframe(spreadsheet_id, summary_tab, filter_summary_columns('categorizing', intervention_summary))
        elif metric.extra_behavior == 'policy_violation_combined':
            # Policy violation - upload whole table to single date tab
            if dept == 'MV Resolvers':
                # MV Resolvers uses combined MISSING_POLICY_RAW_DATA and UNCLEAR_POLICY_RAW_DATA
                combined_df = pd.DataFrame()
                
                # Fetch missing policy data
                try:
                    missing_df = fetch_table_df(conn, 'MISSING_POLICY_RAW_DATA', date_str, dept)
                    if not missing_df.empty:
                        combined_df = pd.concat([combined_df, missing_df], ignore_index=True)
                except Exception as e:
                    print(f"   ❌ Failed fetching MISSING_POLICY_RAW_DATA: {e}")
                
                # Fetch unclear policy data
                try:
                    unclear_df = fetch_table_df(conn, 'UNCLEAR_POLICY_RAW_DATA', date_str, dept)
                    if not unclear_df.empty:
                        combined_df = pd.concat([combined_df, unclear_df], ignore_index=True)
                except Exception as e:
                    print(f"   ❌ Failed fetching UNCLEAR_POLICY_RAW_DATA: {e}")
                
                # Upload combined data
                if not combined_df.empty:
                    gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(combined_df, dept))
                else:
                    print(f"   ℹ️ No policy violation data found for {dept}")
            else:
                # All other departments (Filipina, CC Sales, etc.) use POLICY_VIOLATION_RAW_DATA
                try:
                    policy_df = fetch_table_df(conn, 'POLICY_VIOLATION_RAW_DATA', date_str, dept)
                    if not policy_df.empty:
                        gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(policy_df, dept))
                    else:
                        print(f"   ℹ️ No policy violation data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching POLICY_VIOLATION_RAW_DATA: {e}")
        elif metric.extra_behavior == 'tools_quad_tabs':
            # Tools metric - different behavior per department
            print(f"   🔧 Processing tools metric for {dept}")
            
            if dept == 'MV Resolvers':
                # MV Resolvers: 4 tabs with separate missing/wrong tables
                missing_raw_tab = f"{sheet_name}-missing"
                missing_summary_tab = f"{sheet_name}-missing-summary"
                wrong_raw_tab = f"{sheet_name}-wrong"
                wrong_summary_tab = f"{sheet_name}-wrong-summary"
                
                # Process MISSING_TOOL data
                try:
                    missing_raw_df = fetch_table_df(conn, 'MISSING_TOOL_RAW_DATA', date_str, dept)
                    if not missing_raw_df.empty and gc.create_sheet_if_missing(spreadsheet_id, missing_raw_tab):
                        gc.upload_dataframe(spreadsheet_id, missing_raw_tab, filter_llm_columns(missing_raw_df, dept))
                    else:
                        print(f"   ℹ️ No missing tool raw data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching MISSING_TOOL_RAW_DATA for {dept}: {e}")
                
                try:
                    missing_summary_df = fetch_table_df(conn, 'MISSING_TOOL_SUMMARY', date_str, dept)
                    if not missing_summary_df.empty and gc.create_sheet_if_missing(spreadsheet_id, missing_summary_tab):
                        gc.upload_dataframe(spreadsheet_id, missing_summary_tab, missing_summary_df)
                    else:
                        print(f"   ℹ️ No missing tool summary data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching MISSING_TOOL_SUMMARY for {dept}: {e}")
                
                # Process WRONG_TOOL data
                try:
                    wrong_raw_df = fetch_table_df(conn, 'WRONG_TOOL_RAW_DATA', date_str, dept)
                    if not wrong_raw_df.empty and gc.create_sheet_if_missing(spreadsheet_id, wrong_raw_tab):
                        gc.upload_dataframe(spreadsheet_id, wrong_raw_tab, filter_llm_columns(wrong_raw_df, dept))
                    else:
                        print(f"   ℹ️ No wrong tool raw data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching WRONG_TOOL_RAW_DATA for {dept}: {e}")
                
                try:
                    wrong_summary_df = fetch_table_df(conn, 'WRONG_TOOL_SUMMARY', date_str, dept)
                    if not wrong_summary_df.empty and gc.create_sheet_if_missing(spreadsheet_id, wrong_summary_tab):
                        gc.upload_dataframe(spreadsheet_id, wrong_summary_tab, wrong_summary_df)
                    else:
                        print(f"   ℹ️ No wrong tool summary data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching WRONG_TOOL_SUMMARY for {dept}: {e}")
            
            else:
                # Other departments: 2 tabs with unified TOOL tables
                raw_tab = sheet_name  # yyyy-mm-dd
                summary_tab = f"{sheet_name}-summary"  # yyyy-mm-dd-summary
                
                # Process unified TOOL data
                try:
                    tool_raw_df = fetch_table_df(conn, 'TOOL_RAW_DATA', date_str, dept)
                    if not tool_raw_df.empty and gc.create_sheet_if_missing(spreadsheet_id, raw_tab):
                        gc.upload_dataframe(spreadsheet_id, raw_tab, filter_llm_columns(tool_raw_df, dept))
                    else:
                        print(f"   ℹ️ No tool raw data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching TOOL_RAW_DATA for {dept}: {e}")
                
                try:
                    tool_summary_df = fetch_table_df(conn, 'TOOL_SUMMARY', date_str, dept)
                    if not tool_summary_df.empty and gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                        gc.upload_dataframe(spreadsheet_id, summary_tab, tool_summary_df)
                    else:
                        print(f"   ℹ️ No tool summary data found for {dept}")
                except Exception as e:
                    print(f"   ❌ Failed fetching TOOL_SUMMARY for {dept}: {e}")
        elif metric.extra_behavior == 'shadowing_dual_tabs':
            # Shadowing automation: dual tabs (raw + summary)
            print(f"   🕵️ Processing shadowing automation for {dept}")
            
            # Define specific columns for shadowing automation raw data
            shadowing_raw_columns = ['CONVERSATION_ID', 'ISSUE_ID', 'REPORTER', 'ISSUE_STATUS', 
                                   'ISSUE_TYPE', 'CREATION_DATE', 'ISSUE_DESCRIPTION', 'LLM_OUTPUT_RAW']
            
            # Upload raw data to main date tab
            if not df.empty:
                # Filter to shadowing automation specific columns
                filtered_raw = filter_shadowing_columns(df, shadowing_raw_columns)
                gc.upload_dataframe(spreadsheet_id, sheet_name, filtered_raw)
            else:
                print(f"   ℹ️ No shadowing automation raw data found for {dept}")
            
            # Upload summary to date-summary tab
            try:
                summary_df = fetch_table_df(conn, 'UNIQUE_ISSUES_SUMMARY', date_str, dept)
                if not summary_df.empty:
                    summary_tab = f"{sheet_name}-summary"
                    if gc.create_sheet_if_missing(spreadsheet_id, summary_tab):
                        filtered_summary = filter_summary_columns('shadowing_automation', summary_df)
                        gc.upload_dataframe(spreadsheet_id, summary_tab, filtered_summary)
                else:
                    print(f"   ℹ️ No shadowing automation summary data found for {dept}")
            except Exception as e:
                print(f"   ❌ Failed fetching UNIQUE_ISSUES_SUMMARY: {e}")
        else:
            gc.upload_dataframe(spreadsheet_id, sheet_name, filter_llm_columns(df, dept))


# =============================
# Orchestrator
# =============================

def main():
    print("🚀 Snowflake → Google Sheets Daily Uploader")
    # Allow date override via environment variable
    date_str = os.getenv('DATE_STR', '').strip() or get_yesterday_date_str()
    print(f"📅 Target date: {date_str}")

    # Initialize clients
    gc = SheetsClient(credentials_path='credentials.json')
    conn = get_snowflake_connection()
    try:
        metrics = build_metrics_registry()
        snapshot_sheet_ids = metrics['_snapshot_sheets'].sheet_ids
        policy_sheet_ids = metrics['_policy_sheets'].sheet_ids

        # 1) Upload raw data per metric/department
        ordered_metric_keys = [
            'sentiment_analysis',
            'rule_breaking',  # placeholder
            'categorizing',
            'false_promises',
            'ftr',
            'policy_escalation',
            'client_suspecting_ai',
            'clarity_score',
            'legal_alignment',
            'call_request',
            'threatening',
            'misprescription',
            'unnecessary_clinic_rec',
            'loss_of_interest',
            'clinic_recommendation_reason',
            'policy_violation',
            'intervention',
            'transfer_escalation',
            'transfer_known_flow',
            'tools',
            'shadowing_automation',
        ]

        # Optional: run a single metric by setting ONLY_METRIC in environment/.env
        only_metric = os.getenv('ONLY_METRIC', '').strip()
        if only_metric:
            key_norm = only_metric.lower().replace(' ', '_')
            # allow simple aliases
            aliases = {
                'sa': 'sentiment_analysis',
                'rb': 'rule_breaking',
                'categorizing': 'categorizing',
                'false_promises': 'false_promises',
                'ftr': 'ftr',
                'policy': 'policy_escalation',
                'policy_escalation': 'policy_escalation',
                'csai': 'client_suspecting_ai',
                'client_suspecting_ai': 'client_suspecting_ai',
                'clarity': 'clarity_score',
                'clarity_score': 'clarity_score',
                'legal': 'legal_alignment',
                'legal_alignment': 'legal_alignment',
                'call_request': 'call_request',
                'threatening': 'threatening',
                'misprescription': 'misprescription',
                'unnecessary_clinic_rec': 'unnecessary_clinic_rec',
                'loss_of_interest': 'loss_of_interest',
                'clinic_recommendation_reason': 'clinic_recommendation_reason',
                'crr': 'clinic_recommendation_reason',
                'policy_violation': 'policy_violation',
                'pv': 'policy_violation',
                'intervention': 'intervention',
                'iv': 'intervention',
                'transfer_escalation': 'transfer_escalation',
                'te': 'transfer_escalation',
                'transfer_known_flow': 'transfer_known_flow',
                'tkf': 'transfer_known_flow',
                'tools': 'tools',
                'tool': 'tools',
                'loss_of_interest': 'loss_of_interest',
                'loi': 'loss_of_interest',
                'shadowing_automation': 'shadowing_automation',
                'shadowing': 'shadowing_automation',
                'sa_automation': 'shadowing_automation',
            }
            selected = aliases.get(key_norm, key_norm)
            if selected in ordered_metric_keys:
                ordered_metric_keys = [selected]
                print(f"🎯 ONLY_METRIC set → running only: {selected}")
            else:
                print(f"⚠️ ONLY_METRIC '{only_metric}' not recognized; running all metrics")

        for key in ordered_metric_keys:
            metric = metrics[key]
            upload_metric_raw(gc, metric, policy_sheet_ids, date_str, conn)

        # 2) Update snapshots per department using per-dept summary tables (optional)
        skip_snapshots = os.getenv('SKIP_SNAPSHOTS', '').strip().lower() in {'1', 'true', 'yes'}
        if not skip_snapshots:
            print("\n📊 Updating snapshot sheets from per-department summary tables …")

            # If ONLY_METRIC is set, limit snapshot updates to that metric's fields
            snapshot_allowed_fields: Optional[set] = None
            if only_metric:
                metric_key = selected if 'selected' in locals() else None
                metric_to_fields = {
                    'sentiment_analysis': {'WEIGHTED_AVG_NPS'},
                    'policy_escalation': {'POLICY_ESCALATION_PERCENTAGE'},
                    'client_suspecting_ai': {'CLIENT_SUSPECTING_AI_PERCENTAGE'},
                    'clarity_score': {'CLARITY_SCORE_PERCENTAGE'},
                    'legal_alignment': {'ESCALATION_RATE', 'LEGAL_CONCERNS_PERCENTAGE'},
                    'call_request': {'CALL_REQUEST_RATE', 'REBUTTAL_RESULT_RATE'},
                    'threatening': {'THREATENING_PERCENTAGE'},
                    'misprescription': {'MISPRESCRIPTION_PERCENTAGE'},
                    'unnecessary_clinic_rec': {'UNNECESSARY_CLINIC_PERCENTAGE'},
                    'ftr': {'FTR_PERCENTAGE'},
                    'false_promises': {'FALSE_PROMISES_PERCENTAGE'},
                    'policy_violation': {'MISSING_POLICY_COMBINED', 'UNCLEAR_POLICY_COMBINED', 'WRONG_POLICY_COMBINED'},
                    'intervention': {'INTERVENTION_PERCENTAGE', 'TRANSFER_PERCENTAGE'},
                    'transfer_escalation': {'TRANSFER_ESCALATION_COMBINED'},
                    'transfer_known_flow': {'TRANSFER_KNOWN_FLOW_COMBINED'},
                    'loss_of_interest': set(),  # No snapshot fields for loss_of_interest
                    'tools': set(),  # Tools snapshot columns will be added later
                    'shadowing_automation': set(),  # No snapshot fields for shadowing automation
                }
                snapshot_allowed_fields = metric_to_fields.get(metric_key)

            # Optional department filter for snapshot updates as well
            only_depts_env = os.getenv('ONLY_DEPARTMENTS', '').strip()
            only_depts_set = {d.strip().lower() for d in only_depts_env.split(',') if d.strip()} if only_depts_env else None

            for dept in DEPARTMENTS.keys():
                if only_depts_set and dept.lower() not in only_depts_set:
                    continue
                try:
                    summary_row = fetch_summary_row(conn, dept, date_str)
                    if summary_row is None:
                        continue
                    snapshot_sheet_id = snapshot_sheet_ids.get(dept)
                    if not snapshot_sheet_id:
                        print(f"   ⚠️ No snapshot sheet configured for {dept}")
                        continue
                    update_snapshot_sheet(gc, snapshot_sheet_id, dept, date_str, summary_row, allowed_fields=snapshot_allowed_fields, conn=conn)
                except Exception as e:
                    print(f"   ❌ Snapshot update failed for {dept}: {e}")
        else:
            print("\n⏭️  Skipping snapshot updates (SKIP_SNAPSHOTS=true)")

        print("\n✅ Completed Snowflake → Sheets for ", date_str)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()

