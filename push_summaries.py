#!/usr/bin/env python3
"""
Standalone script to push consolidated summaries only
Usage: python3 push_summaries.py [DATE_STR]
Example: python3 push_summaries.py 2024-01-15
"""

import sys
import os
from datetime import datetime, timedelta

# Import from the main script
from snowflake_to_sheets import (
    SheetsClient, 
    get_snowflake_connection, 
    create_consolidated_summary_sheet,
    get_yesterday_date_str
)

def main():
    print("🚀 Consolidated Summaries Only - Pushing to Google Sheets")
    
    # Get date from command line or use yesterday
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        print(f"📅 Using provided date: {date_str}")
    else:
        date_str = get_yesterday_date_str()
        print(f"📅 Using yesterday's date: {date_str}")
    
    # Consolidated sheet configuration
    consolidated_sheet_id = '1ddl4mL2D3Dc_cTGN8KorpEH0irntvYwPGrpAsv6M1P0'
    
    print(f"📋 Target sheet: {consolidated_sheet_id}")
    print(f"📄 Template sheet: 'template'")
    print(f"🎯 Target sheet name: '{date_str}'")
    
    # Initialize clients
    print("\n🔌 Connecting to services...")
    gc = SheetsClient(credentials_path='credentials.json')
    conn = get_snowflake_connection()
    
    try:
        print("\n📊 Creating consolidated summary sheet...")
        create_consolidated_summary_sheet(gc, consolidated_sheet_id, date_str, conn)
        print(f"\n✅ Successfully created consolidated summary for {date_str}")
        print(f"🔗 Check your Google Sheet: https://docs.google.com/spreadsheets/d/{consolidated_sheet_id}")
        
    except Exception as e:
        print(f"\n❌ Failed to create consolidated summary: {e}")
        sys.exit(1)
        
    finally:
        try:
            conn.close()
            print("\n🔐 Connections closed")
        except Exception:
            pass

if __name__ == '__main__':
    main()
