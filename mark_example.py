import os
os.environ['FLASK_DOTENV_LOADING'] = 'false'

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from datetime import datetime, timedelta
import csv
import io
from snowflake_client import (
    fetch_table_data, get_available_tables, get_table_info,
    get_available_schemas, discover_all_databases, test_database_connection,
    close_connections, fetch_bot_evals_by_date, fetch_all_raw_data_by_date,
    fetch_repetition_raw_data, fetch_sales_conversion_metrics_for_cc_sales,
    check_sales_conversion_metrics_structure
)
from config import get_database_config, get_available_databases, DRIVE_FOLDER_IDS, DEPARTMENT_RAW_SHEET_IDS

app = Flask(__name__)
CORS(app)

def get_worksheet_gid(sheet_id, worksheet_name):
    """
    Get the GID (tab ID) for a specific worksheet in a Google Sheet

    Args:
        sheet_id: Google Sheet ID
        worksheet_name: Name of the worksheet/tab

    Returns:
        GID string if found, None otherwise
    """
    try:
        from google_sheets_client import get_sheets_service
        service = get_sheets_service()

        # Get sheet metadata to find all worksheets
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])

        # Find the worksheet with matching name
        for sheet in sheets:
            properties = sheet.get('properties', {})
            title = properties.get('title', '')
            gid = properties.get('sheetId', 0)

            if title == worksheet_name:
                print(f"✅ Found worksheet '{worksheet_name}' with GID: {gid}")
                return str(gid)

        print(f"❌ Worksheet '{worksheet_name}' not found in sheet {sheet_id}")
        print(f"   Available worksheets: {[s.get('properties', {}).get('title', '') for s in sheets]}")
        return None

    except Exception as e:
        print(f"❌ Error getting worksheet GID: {e}")
        return None

def format_quality_rating(quality_info):
    """
    Format quality rating information for display

    Args:
        quality_info: Dictionary with 'current_quality' and 'last_change_date'

    Returns:
        Formatted string like "Green last 15 days"
    """
    if not quality_info or not quality_info.get('current_quality'):
        return ''

    current_quality = quality_info.get('current_quality', '')
    # Capitalize first letter and make rest lowercase
    formatted_quality = current_quality.capitalize() if current_quality else ''
    last_change_date = quality_info.get('last_change_date')

    if not last_change_date:
        return f"{formatted_quality} last 30 days"

    try:
        # Calculate days since last change
        if isinstance(last_change_date, str):
            # Parse timestamp string
            change_date = datetime.fromisoformat(last_change_date.replace('Z', '+00:00'))
        else:
            # Assume it's already a datetime object
            change_date = last_change_date

        today = datetime.now(change_date.tzinfo) if change_date.tzinfo else datetime.now()
        days_diff = (today - change_date).days

        # If more than 30 days, show "Since more than 30 days"
        if days_diff > 30:
            return f"{formatted_quality} last 30 days"

        # Use proper grammar: "day" for 1, "days" for multiple
        day_word = "day" if days_diff == 1 else "days"
        return f"{formatted_quality} last {days_diff} {day_word}"

    except Exception as e:
        print(f"⚠️ Error formatting quality rating: {e}")
        return formatted_quality

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Get list of available databases"""
    try:
        databases = discover_all_databases()
        return jsonify({'databases': databases})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/info', methods=['GET'])
def get_databases_info():
    """Get detailed information about all databases"""
    try:
        databases = discover_all_databases()
        info = []
        for db in databases:
            try:
                test_result = test_database_connection(db['key'])
                info.append({
                    'key': db['key'],
                    'name': db['name'],
                    'database': db['database'],
                    'status': 'connected' if test_result else 'failed'
                })
            except:
                info.append({
                    'key': db['key'],
                    'name': db['name'],
                    'database': db['database'],
                    'status': 'failed'
                })
        return jsonify({'databases': info})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/test', methods=['GET'])
def test_database(database_key):
    """Test connection to a specific database"""
    try:
        result = test_database_connection(database_key)
        return jsonify({'connected': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/schemas', methods=['GET'])
def get_schemas(database_key):
    """Get available schemas for a database"""
    try:
        schemas = get_available_schemas(database_key)
        return jsonify({'schemas': schemas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/tables', methods=['GET'])
def get_tables(database_key):
    """Get available tables for a database (default schema)"""
    try:
        tables = get_available_tables(database_key)
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/schemas/<schema_name>/tables', methods=['GET'])
def get_schema_tables(database_key, schema_name):
    """Get available tables for a specific schema"""
    try:
        tables = get_available_tables(database_key, schema_name)
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/tables/<table_name>/info', methods=['GET'])
def get_table_info_endpoint(database_key, table_name):
    """Get table information for a table in default schema"""
    try:
        info = get_table_info(table_name, database_key)
        return jsonify(info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/schemas/<schema_name>/tables/<table_name>/info', methods=['GET'])
def get_table_info_in_schema(database_key, schema_name, table_name):
    """Get table information for a table in specific schema"""
    try:
        info = get_table_info(table_name, database_key, schema_name)
        return jsonify(info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/data/<table_name>', methods=['GET'])
def get_table_data(database_key, table_name):
    """Get table data for a table in default schema"""
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        search = request.args.get('search', '')
        
        print(f"🔍 API Request: {database_key}.{table_name}")
        columns, rows = fetch_table_data(table_name, database_key, limit=limit, offset=offset, search=search)
        print(f"✅ Success: Found {len(rows)} rows")
        
        return jsonify({
            'columns': columns,
            'rows': rows,
            'metadata': {
                'table_name': table_name,
                'database': database_key,
                'limit': limit,
                'offset': offset,
                'total_rows': len(rows),
                'has_more': len(rows) == limit
            }
        })
    except ValueError as ve:
        print(f"❌ Error: {str(ve)}")
        return jsonify({'error': str(ve)}), 404
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases/<database_key>/schemas/<schema_name>/data/<table_name>', methods=['GET'])
def get_table_in_schema(database_key, schema_name, table_name):
    """Get table data for a table in specific schema"""
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        search = request.args.get('search', '')
        
        print(f"🔍 API Request: {database_key}.{schema_name}.{table_name}")
        columns, rows = fetch_table_data(table_name, database_key, schema_name=schema_name, limit=limit, offset=offset, search=search)
        print(f"✅ Success: Found {len(rows)} rows")
        
        return jsonify({
            'columns': columns,
            'rows': rows,
            'metadata': {
                'table_name': table_name,
                'database': database_key,
                'schema': schema_name,
                'limit': limit,
                'offset': offset,
                'total_rows': len(rows),
                'has_more': len(rows) == limit
            }
        })
    except ValueError as ve:
        print(f"❌ Error: {str(ve)}")
        return jsonify({'error': str(ve)}), 404
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['GET'])
def get_all_tables():
    """Get all available tables across all databases"""
    try:
        databases = discover_all_databases()
        all_tables = []
        
        for db in databases:
            db_key = db['key']
            schemas = get_available_schemas(db_key)
            
            for schema in schemas:
                tables = get_available_tables(db_key, schema['name'])
                for table in tables:
                    all_tables.append({
                        'database': db['name'],
                        'schema': schema['name'],
                        'table': table['name']
                    })
        
        return jsonify({'tables': all_tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables/<table_name>/info', methods=['GET'])
def get_table_info_route(table_name):
    """Get detailed information about a table"""
    try:
        database_key = request.args.get('database', 'llm_eval')
        schema_name = request.args.get('schema', 'PUBLIC')
        
        table_info = get_table_info(table_name, database_key, schema_name)
        return jsonify(table_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables/<table_name>/data', methods=['GET'])
def get_table_data_legacy(table_name):
    """Get data from a table with pagination and search"""
    try:
        database_key = request.args.get('database', 'llm_eval')
        schema_name = request.args.get('schema', 'PUBLIC')
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        search = request.args.get('search', '')
        
        columns, rows = fetch_table_data(table_name, database_key, schema_name, limit, offset, search)
        
        return jsonify({
            'columns': columns,
            'rows': rows,
            'total_rows': len(rows),
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bot-evals/yesterday', methods=['GET'])
def get_bot_evals_yesterday():
    """Get EVALS_SUMMARY_2 data for yesterday and optionally write to Google Sheets"""
    try:
        # Calculate yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')

        # Get query parameters
        write_to_sheets = request.args.get('write_to_sheets', 'false').lower() == 'true'

        print(f"🔍 API Request: EVALS_SUMMARY for yesterday: {date}")

        # Fetch data from Snowflake
        columns, rows = fetch_bot_evals_by_date(date)

        # Fetch shadowing agent breakdown data
        from snowflake_client import fetch_shadowing_agent_breakdown_by_date, fetch_quality_rating_data, fetch_agent_intervention_percentage_by_date
        shadowing_breakdowns = fetch_shadowing_agent_breakdown_by_date(date)

        # Fetch quality rating data
        quality_data = fetch_quality_rating_data()

        # Fetch agent intervention percentage data
        agent_intervention_data = fetch_agent_intervention_percentage_by_date(date)

        # Fetch sales conversion metrics for CC_Sales department
        sales_conversion_metrics = fetch_sales_conversion_metrics_for_cc_sales(date)

        # Department to phone number mapping
        department_phone_mapping = {
            'Doctors': ['971501159784'],
            'Delighters': ['971506715843'],
            'AT_Filipina': ['971507497417'],
            'AT_African': ['971502100827'],
            'AT_Ethiopian': ['97145810617'],
            'CC_Resolvers': ['971505741759'],
            'MV_Resolvers': ['971505741759'],
            'CC_Sales': ['97145810691', '97145810641'],
            'MV_Sales': ['97145810691', '97145810641']
        }

        # Add shadowing breakdown, quality data, and agent intervention percentage to each row
        for row in rows:
            department = row.get('Department', '')
            row['Shadowing_Breakdown'] = shadowing_breakdowns.get(department, '')

            # Add agent intervention percentage data
            intervention_percentage = agent_intervention_data.get(department)
            if intervention_percentage is not None:
                # Format as percentage with 2 decimal places (e.g., 1.35%)
                row['Agent_Intervention_Percentage'] = f"{intervention_percentage:.2f}%"
            else:
                row['Agent_Intervention_Percentage'] = ''

            # Add quality rating data based on department phone numbers
            phone_numbers = department_phone_mapping.get(department, [])
            if len(phone_numbers) == 1:
                # Single phone number departments
                phone = phone_numbers[0]
                quality_info = quality_data.get(phone, {})
                row['META_Quality'] = format_quality_rating(quality_info)
            elif len(phone_numbers) == 2:
                # Dual phone number departments (CC_Sales, MV_Sales)
                for phone in phone_numbers:
                    quality_info = quality_data.get(phone, {})
                    row[f'META_Quality_{phone}'] = format_quality_rating(quality_info)

        response_data = {
            'columns': columns + ['Shadowing_Breakdown', 'Agent_Intervention_Percentage'],  # Add the new columns
            'rows': rows,
            'metadata': {
                'table_name': 'EVALS_SUMMARY_2',
                'database': 'llm_eval',
                'schema': 'PUBLIC',
                'date_filter': date,
                'total_rows': len(rows),
                'departments': list(set(row.get('Department', 'Unknown') for row in rows)),
                'sales_conversion_metrics': sales_conversion_metrics
            }
        }

        # If write_to_sheets is requested, write data to Google Sheets
        if write_to_sheets and rows:
            try:
                from google_sheets_client import write_bot_evals_to_sheets, write_sales_conversion_metrics_to_sheet
                sheets_result = write_bot_evals_to_sheets(rows, date)
                response_data['sheets_result'] = sheets_result
                print(f"✅ Data written to Google Sheets: {sheets_result}")
                
                # Also write sales conversion metrics to CC_Sales sheet
                if sales_conversion_metrics:
                    cc_sales_sheet_id = "1te1fbAXhURIUO0EzQ2Mrorv3a6GDtEVM_5np9TO775o"  # CC_Sales sheet ID from config
                    print(f"🔍 Writing sales conversion metrics to CC_Sales sheet: {sales_conversion_metrics}")
                    sales_metrics_result = write_sales_conversion_metrics_to_sheet(cc_sales_sheet_id, sales_conversion_metrics)
                    response_data['sales_metrics_sheets_result'] = sales_metrics_result
                    print(f"✅ Sales conversion metrics written to CC_Sales sheet: {sales_metrics_result}")
                else:
                    print("⚠️ No sales conversion metrics to write")
                    
            except ImportError:
                response_data['sheets_error'] = 'Google Sheets integration not available. Install required dependencies.'
            except Exception as sheets_error:
                response_data['sheets_error'] = str(sheets_error)
                print(f"❌ Google Sheets error: {sheets_error}")

        print(f"✅ Success: Found {len(rows)} rows for date {date}")
        return jsonify(response_data)

    except ValueError as ve:
        print(f"❌ Validation Error: {str(ve)}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bot-evals/date/<date>', methods=['GET'])
def get_bot_evals_by_date(date):
    """Get EVALS_SUMMARY data for a specific date and optionally write to Google Sheets"""
    try:
        # Validate date format
        if not date:
            return jsonify({'error': 'Date parameter is required'}), 400

        # Get query parameters
        write_to_sheets = request.args.get('write_to_sheets', 'false').lower() == 'true'

        print(f"🔍 API Request: EVALS_SUMMARY for date {date}")

        # Fetch data from Snowflake
        columns, rows = fetch_bot_evals_by_date(date)

        # Fetch shadowing agent breakdown data
        from snowflake_client import fetch_shadowing_agent_breakdown_by_date, fetch_quality_rating_data, fetch_agent_intervention_percentage_by_date
        shadowing_breakdowns = fetch_shadowing_agent_breakdown_by_date(date)

        # Fetch quality rating data
        quality_data = fetch_quality_rating_data()

        # Fetch agent intervention percentage data
        agent_intervention_data = fetch_agent_intervention_percentage_by_date(date)

        # Fetch sales conversion metrics for CC_Sales department
        sales_conversion_metrics = fetch_sales_conversion_metrics_for_cc_sales(date)

        # Department to phone number mapping
        department_phone_mapping = {
            'Doctors': ['971501159784'],
            'Delighters': ['971506715843'],
            'AT_Filipina': ['971507497417'],
            'AT_African': ['971502100827'],
            'AT_Ethiopian': ['97145810617'],
            'CC_Resolvers': ['971505741759'],
            'MV_Resolvers': ['971505741759'],
            'CC_Sales': ['97145810691', '97145810641'],
            'MV_Sales': ['97145810691', '97145810641']
        }

        # Add shadowing breakdown, quality data, and agent intervention percentage to each row
        for row in rows:
            department = row.get('Department', '')
            row['Shadowing_Breakdown'] = shadowing_breakdowns.get(department, '')

            # Add agent intervention percentage data
            intervention_percentage = agent_intervention_data.get(department)
            if intervention_percentage is not None:
                # Format as percentage with 2 decimal places (e.g., 1.35%)
                row['Agent_Intervention_Percentage'] = f"{intervention_percentage:.2f}%"
            else:
                row['Agent_Intervention_Percentage'] = ''

            # Add quality rating data based on department phone numbers
            phone_numbers = department_phone_mapping.get(department, [])
            if len(phone_numbers) == 1:
                # Single phone number departments
                phone = phone_numbers[0]
                quality_info = quality_data.get(phone, {})
                row['META_Quality'] = format_quality_rating(quality_info)
            elif len(phone_numbers) == 2:
                # Dual phone number departments (CC_Sales, MV_Sales)
                for phone in phone_numbers:
                    quality_info = quality_data.get(phone, {})
                    row[f'META_Quality_{phone}'] = format_quality_rating(quality_info)

        response_data = {
            'columns': columns + ['Shadowing_Breakdown', 'Agent_Intervention_Percentage'],  # Add the new columns
            'rows': rows,
            'metadata': {
                'table_name': 'EVALS_SUMMARY_2',
                'database': 'llm_eval',
                'schema': 'PUBLIC',
                'date_filter': date,
                'total_rows': len(rows),
                'departments': list(set(row.get('Department', 'Unknown') for row in rows)),
                'sales_conversion_metrics': sales_conversion_metrics
            }
        }

        # If write_to_sheets is requested, write data to Google Sheets
        if write_to_sheets and rows:
            try:
                from google_sheets_client import write_bot_evals_to_sheets, write_sales_conversion_metrics_to_sheet
                sheets_result = write_bot_evals_to_sheets(rows, date)
                response_data['sheets_result'] = sheets_result
                print(f"✅ Data written to Google Sheets: {sheets_result}")
                
                # Also write sales conversion metrics to CC_Sales sheet
                if sales_conversion_metrics:
                    cc_sales_sheet_id = "1te1fbAXhURIUO0EzQ2Mrorv3a6GDtEVM_5np9TO775o"  # CC_Sales sheet ID from config
                    print(f"🔍 Writing sales conversion metrics to CC_Sales sheet: {sales_conversion_metrics}")
                    sales_metrics_result = write_sales_conversion_metrics_to_sheet(cc_sales_sheet_id, sales_conversion_metrics)
                    response_data['sales_metrics_sheets_result'] = sales_metrics_result
                    print(f"✅ Sales conversion metrics written to CC_Sales sheet: {sales_metrics_result}")
                else:
                    print("⚠️ No sales conversion metrics to write")
                    
            except ImportError:
                response_data['sheets_error'] = 'Google Sheets integration not available. Install required dependencies.'
            except Exception as sheets_error:
                response_data['sheets_error'] = str(sheets_error)
                print(f"❌ Google Sheets error: {sheets_error}")

        print(f"✅ Success: Found {len(rows)} rows for date {date}")
        return jsonify(response_data)

    except ValueError as ve:
        print(f"❌ Validation Error: {str(ve)}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bot-evals/info', methods=['GET'])
def get_bot_evals_info():
    """Get EVALS_SUMMARY table structure information"""
    try:
        info = get_table_info('EVALS_SUMMARY_2', 'llm_eval', 'PUBLIC')
        return jsonify(info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/raw-data/yesterday', methods=['POST'])
def process_yesterday_raw_data():
    """Process raw data for yesterday and write to Google Sheets"""
    try:
        # Calculate yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')

        print(f"🔍 Processing raw data for yesterday: {date_str}")

        # Fetch all raw data for yesterday
        all_raw_data = fetch_all_raw_data_by_date(date_str)

        # Write to Google Sheets
        try:
            from google_sheets_client import write_all_raw_data_to_sheets
            sheets_result = write_all_raw_data_to_sheets(all_raw_data, date_str)

            response_data = {
                'date': date_str,
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', [])),
                        'error': data.get('error')
                    } for table, data in all_raw_data.items()
                },
                'sheets_result': sheets_result
            }

            print(f"✅ Raw data processing complete for {date_str}")
            return jsonify(response_data)

        except ImportError:
            return jsonify({
                'error': 'Google Sheets integration not available. Install required dependencies.',
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', []))
                    } for table, data in all_raw_data.items()
                }
            }), 500
        except Exception as sheets_error:
            return jsonify({
                'error': f'Google Sheets error: {str(sheets_error)}',
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', []))
                    } for table, data in all_raw_data.items()
                }
            }), 500

    except Exception as e:
        print(f"❌ Error processing raw data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/raw-data/date/<date>', methods=['POST'])
def process_raw_data_by_date(date):
    """Process raw data for a specific date and write to Google Sheets"""
    try:
        print(f"🔍 Processing raw data for date: {date}")

        # Fetch all raw data for the specified date
        all_raw_data = fetch_all_raw_data_by_date(date)

        # Write to Google Sheets
        try:
            from google_sheets_client import write_all_raw_data_to_sheets
            sheets_result = write_all_raw_data_to_sheets(all_raw_data, date)

            response_data = {
                'date': date,
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', [])),
                        'error': data.get('error')
                    } for table, data in all_raw_data.items()
                },
                'sheets_result': sheets_result
            }

            print(f"✅ Raw data processing complete for {date}")
            return jsonify(response_data)

        except ImportError:
            return jsonify({
                'error': 'Google Sheets integration not available. Install required dependencies.',
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', []))
                    } for table, data in all_raw_data.items()
                }
            }), 500
        except Exception as sheets_error:
            return jsonify({
                'error': f'Google Sheets error: {str(sheets_error)}',
                'raw_data_summary': {
                    table: {
                        'total_rows': len(data.get('rows', [])),
                        'columns': len(data.get('columns', []))
                    } for table, data in all_raw_data.items()
                }
            }), 500

    except Exception as e:
        print(f"❌ Error processing raw data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-sheets', methods=['GET'])
def test_sheets_connection():
    """Test Google Sheets connection"""
    try:
        from google_sheets_client import test_sheets_connection
        result = test_sheets_connection()
        return jsonify(result)
    except ImportError:
        return jsonify({
            'status': 'error',
            'message': 'Google Sheets integration not available. Install required dependencies.'
        }), 500
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ============================================================================
# TIME-BASED SHEET MANAGEMENT ENDPOINTS
# ============================================================================

@app.route('/api/sheets/resolve-url', methods=['GET'])
def resolve_sheet_url():
    """
    Resolve the correct Google Sheet URL for a department and date

    Query Parameters:
        department (str): Department name (e.g., 'CC_Sales', 'Doctors')
        date (str): Date in YYYY-MM-DD format (e.g., '2025-01-15')

    Returns:
        JSON with sheet URL and metadata
    """
    try:
        department = request.args.get('department')
        date = request.args.get('date')

        if not department or not date:
            return jsonify({
                'error': 'Missing required parameters: department and date'
            }), 400

        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Expected YYYY-MM-DD'
            }), 400

        from time_based_sheets import sheet_manager

        # Get sheet URL
        sheet_url = sheet_manager.get_sheet_url(department, date)

        if sheet_url:
            # Get additional sheet info
            sheet_info = sheet_manager.find_sheet_for_date(department, date)

            response_data = {
                'status': 'success',
                'department': department,
                'date': date,
                'sheet_url': sheet_url,
                'sheet_info': {
                    'sheet_id': sheet_info.sheet_id if sheet_info else None,
                    'sheet_name': sheet_info.sheet_name if sheet_info else None,
                    'organization_type': sheet_info.week is not None and 'weekly' or 'monthly',
                    'date_range': {
                        'start': sheet_info.start_date if sheet_info else None,
                        'end': sheet_info.end_date if sheet_info else None
                    },
                    'row_count': sheet_info.row_count if sheet_info else 0,
                    'last_updated': sheet_info.last_updated if sheet_info else None
                } if sheet_info else None
            }

            return jsonify(response_data)
        else:
            return jsonify({
                'status': 'not_found',
                'department': department,
                'date': date,
                'error': f'No sheet found for {department} on {date}',
                'suggestion': 'Sheet may need to be created first by running raw data processing'
            }), 404

    except Exception as e:
        print(f"❌ Sheet URL resolution failed: {str(e)}")
        return jsonify({
            'error': f'Sheet URL resolution failed: {str(e)}'
        }), 500

@app.route('/api/sheets/list/<department>', methods=['GET'])
def list_department_sheets(department):
    """
    List all sheets for a specific department

    Path Parameters:
        department (str): Department name

    Returns:
        JSON with list of sheets and their metadata
    """
    try:
        from time_based_sheets import sheet_manager

        sheets = sheet_manager.list_sheets_for_department(department)

        sheets_data = []
        for sheet in sheets:
            sheets_data.append({
                'sheet_id': sheet.sheet_id,
                'sheet_name': sheet.sheet_name,
                'sheet_url': f"https://docs.google.com/spreadsheets/d/{sheet.sheet_id}/edit",
                'year': sheet.year,
                'month': sheet.month,
                'week': sheet.week,
                'date_range': {
                    'start': sheet.start_date,
                    'end': sheet.end_date
                },
                'row_count': sheet.row_count,
                'created_date': sheet.created_date,
                'last_updated': sheet.last_updated
            })

        return jsonify({
            'status': 'success',
            'department': department,
            'total_sheets': len(sheets_data),
            'sheets': sheets_data
        })

    except Exception as e:
        print(f"❌ Failed to list sheets for {department}: {str(e)}")
        return jsonify({
            'error': f'Failed to list sheets: {str(e)}'
        }), 500

@app.route('/api/sheets/register', methods=['POST'])
def register_sheet_manually():
    """
    Manually register a time-based sheet that was created outside the system

    Request Body:
        {
            "department": "CC_Sales",
            "date": "2025-01-15",
            "sheet_id": "1abc123...",
            "sheet_name": "CC_Sales_RawData_2025_01_Week3" (optional)
        }

    Returns:
        JSON with registration result
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                'error': 'Request body must be JSON'
            }), 400

        department = data.get('department')
        date = data.get('date')
        sheet_id = data.get('sheet_id')
        sheet_name = data.get('sheet_name')

        if not all([department, date, sheet_id]):
            return jsonify({
                'error': 'Missing required fields: department, date, sheet_id'
            }), 400

        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Expected YYYY-MM-DD'
            }), 400

        # Validate sheet exists and is accessible
        try:
            from google_sheets_client import get_sheets_service
            service = get_sheets_service()

            # Try to access the sheet
            sheet_info = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            actual_sheet_name = sheet_info.get('properties', {}).get('title', 'Unknown')

            print(f"✅ Verified sheet access: {actual_sheet_name}")

        except Exception as access_error:
            return jsonify({
                'error': f'Cannot access sheet {sheet_id}. Make sure it exists and is shared with promptseval@promptseval.iam.gserviceaccount.com',
                'details': str(access_error)
            }), 400

        # Register the sheet
        from time_based_sheets import sheet_manager

        # Use provided sheet name or generate one
        if not sheet_name:
            year, month, week = sheet_manager.get_date_info(date)
            sheet_name = sheet_manager.generate_sheet_name(department, year, month, week)

        registered_sheet = sheet_manager.register_sheet(department, date, sheet_id)

        return jsonify({
            'status': 'success',
            'message': f'Sheet registered successfully for {department} on {date}',
            'sheet_info': {
                'sheet_id': registered_sheet.sheet_id,
                'sheet_name': registered_sheet.sheet_name,
                'sheet_url': f"https://docs.google.com/spreadsheets/d/{registered_sheet.sheet_id}/edit",
                'department': registered_sheet.department,
                'date_range': {
                    'start': registered_sheet.start_date,
                    'end': registered_sheet.end_date
                },
                'organization_type': 'weekly' if registered_sheet.week else 'monthly'
            }
        })

    except Exception as e:
        print(f"❌ Sheet registration failed: {str(e)}")
        return jsonify({
            'error': f'Sheet registration failed: {str(e)}'
        }), 500

@app.route('/api/sheets/test-create', methods=['POST'])
def test_sheet_creation():
    """
    Test sheet creation for debugging purposes

    Request Body:
        {
            "department": "CC_Sales",
            "date": "2025-08-04"
        }

    Returns:
        JSON with creation result and detailed error information
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                'error': 'Request body must be JSON'
            }), 400

        department = data.get('department', 'CC_Sales')
        date = data.get('date', '2025-08-04')

        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Expected YYYY-MM-DD'
            }), 400

        print(f"🧪 Testing sheet creation for {department} on {date}")

        from time_based_sheets import sheets_creator

        # Try to create the sheet
        sheet_info = sheets_creator.create_time_based_sheet(department, date)

        if sheet_info:
            return jsonify({
                'status': 'success',
                'message': f'Sheet created successfully for {department}',
                'sheet_info': {
                    'sheet_id': sheet_info.sheet_id,
                    'sheet_name': sheet_info.sheet_name,
                    'sheet_url': f"https://docs.google.com/spreadsheets/d/{sheet_info.sheet_id}/edit",
                    'date_range': {
                        'start': sheet_info.start_date,
                        'end': sheet_info.end_date
                    }
                }
            })
        else:
            return jsonify({
                'status': 'failed',
                'message': f'Failed to create sheet for {department}',
                'suggestion': 'Check server logs for detailed error information'
            }), 500

    except Exception as e:
        print(f"❌ Test sheet creation failed: {str(e)}")
        return jsonify({
            'error': f'Test sheet creation failed: {str(e)}'
        }), 500

@app.route('/api/raw-data-v2/date/<date>', methods=['POST'])
def process_raw_data_time_based(date):
    """
    Process raw data for a specific date using time-based sheet organization

    This endpoint allows optional filtering by departments and metrics.

    Path Parameters:
        date (str): Date in YYYY-MM-DD format

    Query Parameters:
        organization_type (str): Override organization type ('weekly', 'monthly', 'yearly')

    Request Body (Optional JSON):
        {
            "departments": ["CC_Sales", "MV_Resolvers"],  // Optional: specific departments
            "metrics": ["DELAY_ANALYSIS_RAW_DATA", "SHADOWING_RAW_DATA"]  // Optional: specific tables
        }

    Available Metrics:
        - REPETITION_RAW_DATA
        - BOT_HANDLED_RAW_DATA
        - DELAY_ANALYSIS_RAW_DATA
        - UNRESPONSIVE_RAW_DATA
        - SIMILARITY_RAW_DATA
        - SHADOWING_RAW_DATA

    Examples:
        # Process all departments and all metrics (default)
        POST /api/raw-data-v2/date/2025-08-06

        # Process only specific departments
        POST /api/raw-data-v2/date/2025-08-06
        {"departments": ["CC_Sales", "MV_Resolvers"]}

        # Process only specific metrics
        POST /api/raw-data-v2/date/2025-08-06
        {"metrics": ["DELAY_ANALYSIS_RAW_DATA", "SHADOWING_RAW_DATA"]}

        # Process specific departments and metrics
        POST /api/raw-data-v2/date/2025-08-06
        {"departments": ["CC_Sales"], "metrics": ["SHADOWING_RAW_DATA"]}

    If no body provided: processes all departments and all metrics

    Returns:
        JSON with processing results and sheet information
    """
    try:
        print(f"🔍 Processing time-based raw data for date: {date}")

        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Expected YYYY-MM-DD'
            }), 400

        # Get query parameters
        org_type_override = request.args.get('organization_type')

        # Parse optional request body for filtering
        request_data = {}
        if request.is_json and request.get_json():
            request_data = request.get_json()

        # Extract filters from request body
        department_filter = request_data.get('departments', [])  # Empty list means all departments
        metrics_filter = request_data.get('metrics', [])  # Empty list means all metrics

        print(f"🔍 Processing filters:")
        print(f"   Departments: {department_filter if department_filter else 'ALL'}")
        print(f"   Metrics: {metrics_filter if metrics_filter else 'ALL'}")

        # Fetch all raw data for the specified date
        all_raw_data = fetch_all_raw_data_by_date(date)

        # Filter metrics if specified
        if metrics_filter:
            print(f"🔧 Filtering to specific metrics: {metrics_filter}")
            filtered_raw_data = {}
            for table_name in metrics_filter:
                if table_name in all_raw_data:
                    filtered_raw_data[table_name] = all_raw_data[table_name]
                else:
                    print(f"⚠️ Requested metric '{table_name}' not found in available data")
                    filtered_raw_data[table_name] = {
                        'columns': [],
                        'rows': [],
                        'error': f'Table {table_name} not found or has no data'
                    }
            all_raw_data = filtered_raw_data

        # Import time-based sheet management
        from time_based_sheets import sheet_manager
        from google_sheets_client import get_sheets_service
        from config import TIME_BASED_CONFIG

        # Debug: Print what data was fetched
        print(f"🔍 Debug: Raw data fetched for {date}:")
        for table_name, table_data in all_raw_data.items():
            if 'error' in table_data:
                print(f"   ❌ {table_name}: ERROR - {table_data['error']}")
            else:
                row_count = len(table_data.get('rows', []))
                print(f"   📊 {table_name}: {row_count} rows")
                if row_count > 0:
                    # Show sample of first row to debug
                    sample_row = table_data['rows'][0]
                    dept = sample_row.get('DEPARTMENT', 'NO_DEPT')
                    date_field = sample_row.get('DATE', 'NO_DATE')
                    print(f"      Sample: DEPARTMENT='{dept}', DATE='{date_field}'")

        # Process each department's data
        results = {
            'date': date,
            'organization_type': org_type_override or TIME_BASED_CONFIG.get('organization_type', 'weekly'),
            'departments': {},
            'summary': {
                'total_departments': 0,
                'sheets_created': 0,
                'sheets_updated': 0,
                'total_rows_written': 0,
                'errors': []
            }
        }

        # Get all departments from the raw data
        all_departments = set()
        for table_name, table_data in all_raw_data.items():
            if 'rows' in table_data:
                for row in table_data['rows']:
                    dept = row.get('DEPARTMENT', '').strip()
                    if dept:
                        all_departments.add(dept)

        print(f"📊 Found data for {len(all_departments)} departments: {list(all_departments)}")

        # Filter departments if specified
        if department_filter:
            print(f"🔧 Filtering to specific departments: {department_filter}")
            # Convert to set for case-insensitive matching
            filter_set = {dept.upper() for dept in department_filter}
            filtered_departments = {dept for dept in all_departments if dept.upper() in filter_set}

            # Check for requested departments that weren't found
            found_filter_set = {dept.upper() for dept in filtered_departments}
            for requested_dept in department_filter:
                if requested_dept.upper() not in found_filter_set:
                    print(f"⚠️ Requested department '{requested_dept}' not found in data")

            all_departments = filtered_departments
            print(f"📊 After filtering: {len(all_departments)} departments: {list(all_departments)}")

        total_departments = len(all_departments)
        for idx, department in enumerate(all_departments, start=1):
            print(f"\n🔄 Processing department {idx}/{total_departments}: {department}")
            results['departments'][department] = {
                'status': 'processing',
                'tables': {},
                'sheet_info': None,
                'rows_written': 0
            }
            results['summary']['total_departments'] += 1

            # Find existing sheet for this department and date
            sheet_info = sheet_manager.find_sheet_for_date(department, date)

            if not sheet_info:
                # Try to use existing sheet from config instead of creating new one
                print(f"🔍 Looking for existing sheet for {department}")
                sheet_id = sheet_manager.get_sheet_id_for_date(department, date)

                if sheet_id:
                    # Create sheet info object for existing sheet
                    from datetime import datetime as dt
                    date_obj = dt.strptime(date, '%Y-%m-%d')
                    year, month, week = sheet_manager.calculate_week_from_date(date)
                    expected_sheet_name = sheet_manager.get_sheet_name_for_date(department, date)

                    # Calculate week dates
                    week_start_day = (week - 1) * 7 + 1
                    week_end_day = min(week * 7, 31)  # Simplified for now

                    start_date = date_obj.replace(day=week_start_day)
                    end_date = date_obj.replace(day=min(week_end_day, 31))

                    # Create SheetInfo object
                    from time_based_sheets import SheetInfo
                    sheet_info = SheetInfo(
                        sheet_id=sheet_id,
                        sheet_name=expected_sheet_name,
                        department=department,
                        year=year,
                        month=month,
                        week=week,
                        start_date=start_date.strftime('%Y-%m-%d'),
                        end_date=end_date.strftime('%Y-%m-%d')
                    )

                    # Register it for future use
                    sheet_manager.register_sheet(
                        department=department,
                        date_str=date,
                        sheet_id=sheet_id
                    )

                    print(f"✅ Using existing sheet: {expected_sheet_name} ({sheet_id})")
                    results['summary']['sheets_updated'] += 1
                else:
                    error_msg = f"No existing sheet found for {department} on {date}"
                    results['summary']['errors'].append(error_msg)
                    results['departments'][department]['status'] = 'error'
                    results['departments'][department]['error'] = error_msg
                    print(f"❌ {error_msg}")
                    continue

            if not sheet_info:
                error_msg = f"No sheet available for {department} on {date}"
                results['summary']['errors'].append(error_msg)
                results['departments'][department]['status'] = 'error'
                results['departments'][department]['error'] = error_msg
                continue

            # Store sheet info in results
            results['departments'][department]['sheet_info'] = {
                'sheet_id': sheet_info.sheet_id,
                'sheet_name': sheet_info.sheet_name,
                'sheet_url': f"https://docs.google.com/spreadsheets/d/{sheet_info.sheet_id}/edit",
                'date_range': {
                    'start': sheet_info.start_date,
                    'end': sheet_info.end_date
                }
            }

            # Process each table's data for this department
            dept_total_rows = 0
            for table_name, table_data in all_raw_data.items():
                if table_data.get('error'):
                    results['departments'][department]['tables'][table_name] = {
                        'status': 'error',
                        'error': table_data['error']
                    }
                    continue

                # Filter rows for this department
                dept_rows = [
                    row for row in table_data.get('rows', [])
                    if row.get('DEPARTMENT', '').strip().upper() == department.upper()
                ]

                if not dept_rows:
                    results['departments'][department]['tables'][table_name] = {
                        'status': 'no_data',
                        'rows_written': 0
                    }
                    continue

                print(f"  📊 Writing {len(dept_rows)} rows from {table_name}")

                # Create worksheet name (consistent with google_sheets_client.py)
                table_suffix_map = {
                    'REPETITION_RAW_DATA': 'Repetition',
                    'BOT_HANDLED_RAW_DATA': 'bot handled',
                    'DELAY_ANALYSIS_RAW_DATA': '',  
                    'UNRESPONSIVE_RAW_DATA': 'unresponsive',
                    'SIMILARITY_RAW_DATA': '80%similarity',
                    'SHADOWING_RAW_DATA': 'Shadowing'
                }

                tab_suffix = table_suffix_map.get(table_name, table_name.lower())
                worksheet_name = f"{date} {tab_suffix}".strip()

                # Write data to the sheet
                try:
                    from google_sheets_client import write_raw_data_to_sheet
                    service = get_sheets_service()

                    write_result = write_raw_data_to_sheet(
                        service,
                        sheet_info.sheet_id,
                        worksheet_name,
                        table_data['columns'],
                        dept_rows
                    )

                    if write_result.get('status') == 'success':
                        rows_written = len(dept_rows)
                        dept_total_rows += rows_written
                        results['departments'][department]['tables'][table_name] = {
                            'status': 'success',
                            'worksheet_name': worksheet_name,
                            'rows_written': rows_written
                        }
                        print(f"  ✅ Successfully wrote {rows_written} rows to {worksheet_name}")
                    else:
                        error_msg = write_result.get('error', 'Unknown error')
                        results['departments'][department]['tables'][table_name] = {
                            'status': 'error',
                            'error': error_msg
                        }
                        results['summary']['errors'].append(f"{department}/{table_name}: {error_msg}")
                        print(f"  ❌ Failed to write {table_name}: {error_msg}")

                except Exception as write_error:
                    error_msg = str(write_error)
                    results['departments'][department]['tables'][table_name] = {
                        'status': 'error',
                        'error': error_msg
                    }
                    results['summary']['errors'].append(f"{department}/{table_name}: {error_msg}")
                    print(f"  ❌ Exception writing {table_name}: {error_msg}")

            # Update department results
            results['departments'][department]['rows_written'] = dept_total_rows
            results['summary']['total_rows_written'] += dept_total_rows

            if dept_total_rows > 0:
                results['departments'][department]['status'] = 'success'
                results['summary']['sheets_updated'] += 1

                # Update sheet row count in registry
                sheet_manager.update_sheet_row_count(department, date, dept_total_rows)
            else:
                results['departments'][department]['status'] = 'no_data'

        # Final summary
        success_count = sum(1 for dept in results['departments'].values() if dept['status'] == 'success')
        
        # Log missing metrics for each department
        print(f"\n🔍 Checking for missing raw data metrics per department:")
        expected_metrics = [
            'REPETITION_RAW_DATA',
            'BOT_HANDLED_RAW_DATA', 
            'DELAY_ANALYSIS_RAW_DATA',
            'UNRESPONSIVE_RAW_DATA',
            'SIMILARITY_RAW_DATA',
            'SHADOWING_RAW_DATA'
        ]
        
        for department, dept_data in results['departments'].items():
            if 'tables' in dept_data:
                missing_metrics = []
                for metric in expected_metrics:
                    if metric not in dept_data['tables']:
                        missing_metrics.append(metric)
                    elif dept_data['tables'][metric].get('status') == 'no_data':
                        missing_metrics.append(metric)
                
                if missing_metrics:
                    print(f"   ⚠️ {department}: Missing data for metrics: {', '.join(missing_metrics)}")
                else:
                    print(f"   ✅ {department}: All metrics have data")
        
        print(f"\n✅ Time-based raw data processing complete for {date}")
        print(f"📊 Summary: {success_count}/{len(all_departments)} departments processed successfully")
        print(f"📝 Sheets created: {results['summary']['sheets_created']}")
        print(f"📝 Sheets updated: {results['summary']['sheets_updated']}")
        print(f"📊 Total rows written: {results['summary']['total_rows_written']}")

        return jsonify(results)

    except Exception as e:
        print(f"❌ Time-based raw data processing failed for {date}: {str(e)}")
        return jsonify({
            'error': f'Time-based raw data processing failed: {str(e)}',
            'date': date
        }), 500

@app.route('/api/raw-data-v2/yesterday', methods=['POST'])
def process_raw_data_yesterday():
    """
    Process raw data for yesterday's date using time-based sheet organization

    This endpoint automatically calculates yesterday's date and processes the data.
    It supports the same filtering options as the date-specific endpoint.

    Query Parameters:
        organization_type (str): Override organization type ('weekly', 'monthly', 'yearly')

    Request Body (Optional JSON):
        {
            "departments": ["CC_Sales", "MV_Resolvers"],  // Optional: specific departments
            "metrics": ["DELAY_ANALYSIS_RAW_DATA", "SHADOWING_RAW_DATA"]  // Optional: specific tables
        }

    Available Metrics:
        - REPETITION_RAW_DATA
        - BOT_HANDLED_RAW_DATA
        - DELAY_ANALYSIS_RAW_DATA
        - UNRESPONSIVE_RAW_DATA
        - SIMILARITY_RAW_DATA
        - SHADOWING_RAW_DATA

    Examples:
        # Process all departments and all metrics for yesterday (default)
        POST /api/raw-data-v2/yesterday

        # Process only specific departments for yesterday
        POST /api/raw-data-v2/yesterday
        {"departments": ["CC_Sales", "MV_Resolvers"]}

        # Process only specific metrics for yesterday
        POST /api/raw-data-v2/yesterday
        {"metrics": ["DELAY_ANALYSIS_RAW_DATA", "SHADOWING_RAW_DATA"]}

    If no body provided: processes all departments and all metrics for yesterday

    Returns:
        JSON with processing results and sheet information for yesterday's date
    """
    try:
        # Calculate yesterday's date
        from datetime import datetime, timedelta
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        
        print(f"📅 Yesterday's date calculated: {yesterday_str}")
        
        # Call the existing function with yesterday's date
        return process_raw_data_time_based(yesterday_str)
        
    except Exception as e:
        print(f"❌ Failed to process yesterday's data: {str(e)}")
        return jsonify({
            'error': f'Failed to process yesterday\'s data: {str(e)}'
        }), 500

@app.route('/api/repetition-raw-data/download')
def download_repetition_raw_data():
    """Download repetition raw data as CSV file"""
    try:
        # Get required parameters
        department = request.args.get('department')
        date_str = request.args.get('date')

        if not department:
            return jsonify({'error': 'Department parameter is required'}), 400

        if not date_str:
            return jsonify({'error': 'Date parameter is required'}), 400

        # Validate date format
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

        print(f"📥 Fetching repetition raw data for {department} on {date_str}")

        # Fetch repetition raw data from Snowflake
        repetition_data = fetch_repetition_raw_data(department, date_str)

        if 'error' in repetition_data:
            return jsonify({'error': repetition_data['error']}), 500

        if not repetition_data.get('rows'):
            return jsonify({'error': f'No repetition data found for {department} on {date_str}'}), 404

        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header row
        columns = repetition_data.get('columns', [])
        writer.writerow(columns)

        # Write data rows
        for row in repetition_data['rows']:
            # Convert row dict to list in column order
            row_values = []
            for col in columns:
                value = row.get(col, '')
                # Convert date objects to strings
                if hasattr(value, 'strftime'):
                    value = value.strftime('%Y-%m-%d %H:%M:%S') if hasattr(value, 'hour') else value.strftime('%Y-%m-%d')
                elif value is None:
                    value = ''
                row_values.append(str(value))
            writer.writerow(row_values)

        # Get CSV content
        csv_content = output.getvalue()
        output.close()

        # Create filename
        filename = f"repetition_raw_data_{department}_{date_str}.csv"

        print(f"📤 Generated CSV file: {filename} ({len(repetition_data['rows'])} rows)")

        # Return CSV as downloadable file
        return Response(
            csv_content,
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
                'Content-Type': 'text/csv; charset=utf-8'
            }
        )

    except Exception as e:
        print(f"❌ Error generating CSV: {str(e)}")
        return jsonify({'error': f'Failed to generate CSV: {str(e)}'}), 500

@app.route('/api/clear-cache', methods=['POST'])
def clear_cache():
    """Clear all cached data and connections"""
    try:
        close_connections()
        return jsonify({'message': 'Cache cleared successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/get-sheet/<department>/<date>', methods=['GET'])
@app.route('/api/get-sheet/<department>/<date>/<metric>', methods=['GET'])
def get_sheet_for_department_and_date(department, date, metric=None):
    """
    Get the Google Sheet for a specific department and date, optionally with direct tab link

    This endpoint returns the sheet information and URL for the time-based sheet
    that corresponds to the given department and date. If metric is provided,
    returns direct link to the specific tab.

    Path Parameters:
        department (str): Department name (e.g., 'MV_Resolvers', 'CC_Sales')
        date (str): Date in YYYY-MM-DD format
        metric (str, optional): Metric name for direct tab link

    Available Metrics:
        - repetition (REPETITION_RAW_DATA)
        - bot-handled (BOT_HANDLED_RAW_DATA)
        - delays (DELAY_ANALYSIS_RAW_DATA)
        - unresponsive (UNRESPONSIVE_RAW_DATA)
        - similarity (SIMILARITY_RAW_DATA)
        - shadowing (SHADOWING_RAW_DATA)

    Returns:
        JSON with sheet information including:
        - sheet_id: Google Sheet ID
        - sheet_url: Direct URL to the sheet (or specific tab if metric provided)
        - sheet_name: Expected sheet name
        - tab_name: Specific tab name (if metric provided)
        - week_info: Year, month, week information
        - folder_structure: Expected folder organization

    Examples:
        GET /api/get-sheet/MV_Resolvers/2025-08-06
        GET /api/get-sheet/MV_Sales/2025-08-06/repetition
        GET /api/get-sheet/CC_Sales/2025-08-06/shadowing
    """
    try:
        print(f"🔍 Getting sheet for {department} on {date}" + (f" (metric: {metric})" if metric else ""))

        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Use YYYY-MM-DD format.',
                'example': '2025-08-06'
            }), 400

        # Define metric mapping for URL-friendly names to table names and tab suffixes
        metric_mapping = {
            'repetition': {'table': 'REPETITION_RAW_DATA', 'tab_suffix': 'Repetition'},
            'bot-handled': {'table': 'BOT_HANDLED_RAW_DATA', 'tab_suffix': 'bot handled'},
            'delays': {'table': 'DELAY_ANALYSIS_RAW_DATA', 'tab_suffix': ''},
            'unresponsive': {'table': 'UNRESPONSIVE_RAW_DATA', 'tab_suffix': 'unresponsive'},
            'similarity': {'table': 'SIMILARITY_RAW_DATA', 'tab_suffix': '80%similarity'},
            'shadowing': {'table': 'SHADOWING_RAW_DATA', 'tab_suffix': 'Shadowing'},
        }

        # Validate metric if provided
        if metric and metric not in metric_mapping:
            return jsonify({
                'error': f'Invalid metric: {metric}',
                'available_metrics': list(metric_mapping.keys()),
                'example': f'/api/get-sheet/{department}/{date}/repetition'
            }), 400

        # Get sheet information using time-based manager
        from time_based_sheets import sheet_manager

        # Calculate week information
        year, month, week = sheet_manager.calculate_week_from_date(date)
        expected_sheet_name = sheet_manager.get_sheet_name_for_date(department, date)

        # Try to get sheet ID and URL
        sheet_id = sheet_manager.get_sheet_id_for_date(department, date)
        sheet_url = sheet_manager.get_sheet_url(department, date)

        # Get folder structure information
        folder_structure = {
            'department_folder': DRIVE_FOLDER_IDS.get('departments', {}).get(department),
            'monthly_folder_name': f"{year}-{month:02d} Raw Data",
            'expected_path': f"{department}/{year}-{month:02d} Raw Data/{expected_sheet_name}"
        }

        if sheet_id and sheet_url:
            response_data = {
                'success': True,
                'department': department,
                'date': date,
                'sheet_id': sheet_id,
                'sheet_url': sheet_url,
                'sheet_name': expected_sheet_name,
                'week_info': {
                    'year': year,
                    'month': month,
                    'week': week,
                    'week_name': f"Week{week}"
                },
                'folder_structure': folder_structure,
                'message': f'Sheet found for {department} on {date}'
            }

            # Add metric-specific information if metric is provided
            if metric:
                metric_info = metric_mapping[metric]
                tab_suffix = metric_info['tab_suffix']
                tab_name = f"{date} {tab_suffix}".strip()

                # Get the actual GID for the specific worksheet
                print(f"🔍 Looking for worksheet: '{tab_name}' in sheet {sheet_id}")
                worksheet_gid = get_worksheet_gid(sheet_id, tab_name)

                # Generate tab-specific URL with actual GID
                if worksheet_gid:
                    tab_url = f"{sheet_url}#gid={worksheet_gid}"
                    tab_found = True
                    print(f"✅ Found worksheet '{tab_name}' with GID: {worksheet_gid}")
                else:
                    tab_url = sheet_url  # Fallback to main sheet URL
                    tab_found = False
                    print(f"⚠️ Worksheet '{tab_name}' not found, using main sheet URL")

                response_data.update({
                    'metric': metric,
                    'metric_table': metric_info['table'],
                    'tab_name': tab_name,
                    'tab_url': tab_url,
                    'tab_suffix': tab_suffix,
                    'tab_found': tab_found,
                    'worksheet_gid': worksheet_gid,
                    'message': f'Sheet and tab found for {department} on {date} (metric: {metric})' if tab_found
                              else f'Sheet found for {department} on {date}, but tab "{tab_name}" not found'
                })

                print(f"✅ Generated tab URL: {tab_url}")

            return jsonify(response_data)
        else:
            error_response = {
                'success': False,
                'department': department,
                'date': date,
                'sheet_id': None,
                'sheet_url': None,
                'sheet_name': expected_sheet_name,
                'week_info': {
                    'year': year,
                    'month': month,
                    'week': week,
                    'week_name': f"Week{week}"
                },
                'folder_structure': folder_structure,
                'error': f'No sheet found for {department} on {date}',
                'suggestion': 'Create the sheet using Google Apps Script or the /api/raw-data-v2 endpoint'
            }

            # Add metric information to error response if provided
            if metric:
                metric_info = metric_mapping[metric]
                tab_suffix = metric_info['tab_suffix']
                tab_name = f"{date} {tab_suffix}".strip()

                error_response.update({
                    'metric': metric,
                    'metric_table': metric_info['table'],
                    'tab_name': tab_name,
                    'tab_url': None,
                    'error': f'No sheet found for {department} on {date} (metric: {metric})'
                })

            return jsonify(error_response), 404

    except Exception as e:
        print(f"❌ Error getting sheet for {department} on {date}: {str(e)}")
        return jsonify({
            'error': f'Failed to get sheet information: {str(e)}',
            'department': department,
            'date': date
        }), 500


@app.route('/api/list-sheets/<department>', methods=['GET'])
def list_sheets_for_department(department):
    """
    List all available sheets for a specific department

    Path Parameters:
        department (str): Department name

    Query Parameters:
        year (int): Filter by year (optional)
        month (int): Filter by month (optional)

    Returns:
        JSON with list of available sheets for the department
    """
    try:
        print(f"📋 Listing sheets for department: {department}")

        year_filter = request.args.get('year', type=int)
        month_filter = request.args.get('month', type=int)

        from time_based_sheets import sheet_manager

        # Get all sheets for the department from registry
        department_sheets = []

        # Check registry first
        if department in sheet_manager.sheet_registry:
            for sheet_key, sheet_info in sheet_manager.sheet_registry[department].items():
                sheet_data = {
                    'sheet_id': sheet_info['sheet_id'],
                    'sheet_name': sheet_info['sheet_name'],
                    'year': sheet_info['year'],
                    'month': sheet_info['month'],
                    'week': sheet_info.get('week'),
                    'start_date': sheet_info.get('start_date', ''),
                    'end_date': sheet_info.get('end_date', ''),
                    'sheet_url': f"https://docs.google.com/spreadsheets/d/{sheet_info['sheet_id']}/edit"
                }

                # Apply filters
                if year_filter and sheet_data['year'] != year_filter:
                    continue
                if month_filter and sheet_data['month'] != month_filter:
                    continue

                department_sheets.append(sheet_data)

        # Also check DEPARTMENT_RAW_SHEET_IDS for pattern-based sheets
        pattern_sheets = []
        for sheet_key, sheet_id in DEPARTMENT_RAW_SHEET_IDS.items():
            if sheet_key.startswith(f"{department}_RawData_"):
                # Parse the pattern: Department_RawData_YYYY_MM_WeekN
                try:
                    parts = sheet_key.split('_')
                    if len(parts) >= 5:
                        year = int(parts[2])
                        month = int(parts[3])
                        week_part = parts[4]  # WeekN
                        week = int(week_part.replace('Week', ''))

                        # Apply filters
                        if year_filter and year != year_filter:
                            continue
                        if month_filter and month != month_filter:
                            continue

                        pattern_sheets.append({
                            'sheet_id': sheet_id,
                            'sheet_name': sheet_key,
                            'year': year,
                            'month': month,
                            'week': week,
                            'start_date': '',
                            'end_date': '',
                            'sheet_url': f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit",
                            'source': 'config_pattern'
                        })
                except (ValueError, IndexError):
                    continue

        # Combine and deduplicate
        all_sheets = department_sheets + pattern_sheets

        # Remove duplicates based on sheet_id
        seen_ids = set()
        unique_sheets = []
        for sheet in all_sheets:
            if sheet['sheet_id'] not in seen_ids:
                seen_ids.add(sheet['sheet_id'])
                unique_sheets.append(sheet)

        # Sort by year, month, week
        unique_sheets.sort(key=lambda x: (x['year'], x['month'], x.get('week', 0)))

        return jsonify({
            'success': True,
            'department': department,
            'filters': {
                'year': year_filter,
                'month': month_filter
            },
            'sheets_found': len(unique_sheets),
            'sheets': unique_sheets
        })

    except Exception as e:
        print(f"❌ Error listing sheets for {department}: {str(e)}")
        return jsonify({
            'error': f'Failed to list sheets: {str(e)}',
            'department': department
        }), 500

@app.route('/api/test/sales-conversion-structure', methods=['GET'])
def test_sales_conversion_structure():
    """Test endpoint to check SALES_CONVERSION_METRICS table structure"""
    try:
        from snowflake_client import check_sales_conversion_metrics_structure
        result = check_sales_conversion_metrics_structure()
        return jsonify({
            'status': 'success',
            'table_structure': result
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)