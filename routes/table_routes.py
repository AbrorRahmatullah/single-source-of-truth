import csv
import io
import re
from flask import Blueprint, current_app, make_response, request, jsonify, session
from datetime import date, datetime
from werkzeug.utils import secure_filename
import os
import logging

from utils.file_utils import allowed_file
from utils.excel_utils import get_excel_sheets
from models.audit import insert_audit_trail
from config.config import get_db_connection

table_bp = Blueprint('table', __name__)
logger = logging.getLogger(__name__)

@table_bp.route('/check-period', methods=['POST'])
def check_period():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    data = request.json
    table_name = data.get('table_name')
    periode_date = data.get('periode_date')

    if not table_name or not periode_date:
        return jsonify({'success': False, 'message': 'Nama tabel dan periode_date wajib diisi.'})

    try:
        # 🔹 Ubah dari YYYY-MM menjadi YYYY-MM-01
        try:
            periode_date = datetime.strptime(periode_date, '%Y-%m').date().replace(day=1)
        except ValueError:
            return jsonify({'success': False, 'message': 'Format tanggal periode tidak valid. Gunakan format YYYY-MM'})

        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name} WHERE period_date = ?"
        cursor.execute(query, (periode_date,))
        count = cursor.fetchone()[0]
        
        insert_audit_trail('check_period', f"User '{session.get('username')}' checked period '{periode_date}' in table '{table_name}'.")
        return jsonify({'success': True, 'exists': count > 0})

    except Exception as e:
        insert_audit_trail('check_period_failed', f"User '{session.get('username')}' failed to check period in table '{table_name}': {str(e)}")
        return jsonify({'success': False, 'message': f'Error checking period: {str(e)}'})

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# PERBAIKAN 2: Endpoint check table yang lebih reliable
@table_bp.route('/check-table-exists/<table_name>')
def check_table_exists(table_name):
    """Check if table already exists in database"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # PERBAIKAN: Gunakan multiple check untuk memastikan
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (table_name,))
        
        exists_count = cursor.fetchone()[0]
        exists = exists_count > 0
        
        # PERBAIKAN: Double check dengan INFORMATION_SCHEMA
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
        """, (table_name,))
        
        exists_info_schema = cursor.fetchone()[0] > 0
        
        # Jika ada perbedaan, log warning
        if exists != exists_info_schema:
            logger.warning(f"Table existence check mismatch for {table_name}: sys.tables={exists}, INFORMATION_SCHEMA={exists_info_schema}")
        
        # Gunakan hasil yang paling konservatif (jika salah satu mengatakan ada, maka ada)
        final_exists = exists or exists_info_schema
        insert_audit_trail('check_table_exists', f"User '{session.get('username')}' checked existence of table '{table_name}'.")
        return jsonify({
            'success': True,
            'exists': final_exists,
            'message': f'Tabel "{table_name}" {"sudah ada" if final_exists else "belum ada"} dalam database'
        })
        
    except Exception as e:
        insert_audit_trail('check_table_exists_failed', f"User '{session.get('username')}' failed to check existence of table '{table_name}': {str(e)}")
        logger.error(f"Error checking table existence: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@table_bp.route('/get-existing-tables')
def get_existing_tables():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not authenticated'})

    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user role and division for filtering
        role_access = session.get('role_access')
        user_division = session.get('division')

        # Build query based on role
        if role_access == 'admin':
            # Admin can see all templates
            query = """
                SELECT 
                    template_name,
                    division_name,
                    create_date,
                    create_by
                FROM MasterCreator
                ORDER BY create_date DESC
            """
            cursor.execute(query)
        else:
            # Non-admin users can only see their division's templates
            query = """
                SELECT 
                    template_name,
                    division_name,
                    create_date,
                    create_by
                FROM MasterCreator
                WHERE division_name = ?
                ORDER BY create_date DESC
            """
            cursor.execute(query, (user_division,))

        templates = cursor.fetchall()
        
        # Log for debugging
        logger.info(f"Found {len(templates)} templates for user {session.get('username')}")

        # Build response
        tables = []
        for template in templates:
            table_data = {
                'name': template[0],
                'division': template[1],
                'create_date': template[2].strftime('%Y-%m-%d %H:%M:%S') if template[2] else 'N/A',
                'create_by': template[3]
            }
            tables.append(table_data)
            logger.debug(f"Added template: {table_data}")

        insert_audit_trail('get_existing_tables', f"User '{session.get('username')}' accessed existing tables list.")
        return jsonify({
            'success': True, 
            'tables': tables,
            'count': len(tables)
        })

    except Exception as e:
        insert_audit_trail('get_existing_tables_failed', f"User '{session.get('username')}' failed to access existing tables list: {str(e)}")
        logger.error(f"Error getting existing tables: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Database error: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@table_bp.route('/get-table-details/<table_name>', methods=['GET'])
def get_table_details(table_name):
    """Get detailed information about a specific table"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get table columns information
        cursor.execute("""
            SELECT 
                c.name as column_name,
                t.name as data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tb ON c.object_id = tb.object_id
            WHERE tb.name = ? AND tb.type = 'U'
            ORDER BY c.column_id
        """, (table_name,))
        
        columns = []
        for row in cursor.fetchall():
            column_name = row[0]
            data_type = row[1]
            max_length = row[2]
            precision = row[3]
            scale = row[4]
            is_nullable = row[5]
            is_identity = row[6]
            
            # Skip identity columns (like ID)
            if is_identity:
                continue
                
            # Format length based on data type
            length = ''
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar'] and max_length > 0:
                length = str(max_length if max_length != -1 else 'MAX')
            elif data_type in ['decimal', 'numeric'] and precision > 0:
                length = f"{precision},{scale}" if scale > 0 else str(precision)
            
            columns.append({
                'name': column_name,
                'type': data_type.upper(),
                'length': length,
                'nullable': is_nullable
            })
        
        insert_audit_trail('get_table_details', f"User '{session.get('username')}' accessed table details for '{table_name}'.")
        return jsonify({
            'success': True,
            'table': {
                'name': table_name,
                'columns': columns
            }
        })
        
    except Exception as e:
        insert_audit_trail('get_table_details_failed', f"User '{session.get('username')}' failed to access table details for '{table_name}': {str(e)}")
        logger.error(f"Error fetching table details for {table_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error fetching table details: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@table_bp.route('/get-table-data/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """Get data from a specific table"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, check if table exists
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (table_name,))
        
        if cursor.fetchone()[0] == 0:
            return jsonify({'success': False, 'message': f'Table "{table_name}" not found'})
        
        # Get table data with pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        offset = (page - 1) * per_page
        
        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        total_count = cursor.fetchone()[0]
        
        # Get data with pagination
        cursor.execute(f"""
            SELECT * FROM [{table_name}]
            ORDER BY id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, (offset, per_page))
        
        columns = [column[0] for column in cursor.description]
        data = []
        
        for row in cursor.fetchall():
            row_data = {}
            for i, column in enumerate(columns):
                value = row[i]
                if isinstance(value, datetime):
                    row_data[column] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, date):
                    row_data[column] = value.strftime('%Y-%m-%d')
                else:
                    row_data[column] = value
            data.append(row_data)
        
        insert_audit_trail('get_table_data', f"User '{session.get('username')}' accessed data from table '{table_name}', page {page}.")
        return jsonify({
            'success': True,
            'data': data,
            'columns': columns,
            'total': total_count,
            'page': page,
            'per_page': per_page,
            'total_pages': (total_count + per_page - 1) // per_page
        })
        
    except Exception as e:
        insert_audit_trail('get_table_data_failed', f"User '{session.get('username')}' failed to access data from table '{table_name}': {str(e)}")
        logger.error(f"Error fetching table data for {table_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error fetching table data: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
                                    
@table_bp.route('/get-sql-data-types')
def get_sql_data_types():
    """Get available SQL Server data types"""
    data_types = [
        {'value': 'VARCHAR', 'label': 'VARCHAR - Variable-length character string', 'has_length': True},
        {'value': 'NVARCHAR', 'label': 'NVARCHAR - Variable-length Unicode string', 'has_length': True},
        {'value': 'CHAR', 'label': 'CHAR - Fixed-length character string', 'has_length': True},
        {'value': 'NCHAR', 'label': 'NCHAR - Fixed-length Unicode string', 'has_length': True},
        {'value': 'TEXT', 'label': 'TEXT - Variable-length text data', 'has_length': False},
        {'value': 'NTEXT', 'label': 'NTEXT - Variable-length Unicode text', 'has_length': False},
        {'value': 'INT', 'label': 'INT - Integer (32-bit)', 'has_length': False},
        {'value': 'BIGINT', 'label': 'BIGINT - Large integer (64-bit)', 'has_length': False},
        {'value': 'SMALLINT', 'label': 'SMALLINT - Small integer (16-bit)', 'has_length': False},
        {'value': 'TINYINT', 'label': 'TINYINT - Very small integer (8-bit)', 'has_length': False},
        {'value': 'DECIMAL', 'label': 'DECIMAL - Exact numeric with precision and scale', 'has_length': True},
        {'value': 'NUMERIC', 'label': 'NUMERIC - Exact numeric with precision and scale', 'has_length': True},
        {'value': 'FLOAT', 'label': 'FLOAT - Approximate numeric floating point', 'has_length': False},
        {'value': 'REAL', 'label': 'REAL - Approximate numeric floating point', 'has_length': False},
        {'value': 'MONEY', 'label': 'MONEY - Monetary data', 'has_length': False},
        {'value': 'SMALLMONEY', 'label': 'SMALLMONEY - Monetary data (smaller range)', 'has_length': False},
        {'value': 'DATE', 'label': 'DATE - Date only', 'has_length': False},
        {'value': 'TIME', 'label': 'TIME - Time only', 'has_length': False},
        {'value': 'DATETIME', 'label': 'DATETIME - Date and time', 'has_length': False},
        {'value': 'DATETIME2', 'label': 'DATETIME2 - Date and time with higher precision', 'has_length': False},
        {'value': 'SMALLDATETIME', 'label': 'SMALLDATETIME - Date and time (smaller range)', 'has_length': False},
        {'value': 'BIT', 'label': 'BIT - Boolean (0 or 1)', 'has_length': False},
        {'value': 'UNIQUEIDENTIFIER', 'label': 'UNIQUEIDENTIFIER - GUID', 'has_length': False},
        {'value': 'VARBINARY', 'label': 'VARBINARY - Variable-length binary data', 'has_length': True},
        {'value': 'BINARY', 'label': 'BINARY - Fixed-length binary data', 'has_length': True},
        {'value': 'IMAGE', 'label': 'IMAGE - Variable-length binary data (legacy)', 'has_length': False}
    ]
    
    return jsonify({
        'success': True,
        'data_types': data_types
    })

@table_bp.route('/get-excel-sheets', methods=['POST'])
def get_excel_sheets_endpoint():
    """Endpoint untuk mendapatkan daftar sheet dalam file Excel"""
    
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'message': 'File harus berformat Excel (.xlsx atau .xls)'})
        
        # Simpan file temporary
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"sheets_{timestamp}_{filename}"
        file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # Dapatkan daftar sheet
            sheets = get_excel_sheets(file_path)
            
            if sheets is None:
                return jsonify({'success': False, 'message': 'Gagal membaca daftar sheet dari file Excel'})
            
            insert_audit_trail('get_excel_sheets', f"User '{session.get('username')}' uploaded file '{filename}' and retrieved sheets.")
            return jsonify({
                'success': True,
                'sheets': sheets,
                'message': f'Ditemukan {len(sheets)} sheet dalam file Excel'
            })
            
        finally:
            # Hapus file temporary
            try:
                os.remove(file_path)
            except:
                pass
        
    except Exception as e:
        insert_audit_trail('get_excel_sheets_failed', f"User '{session.get('username')}' failed to retrieve sheets from uploaded file: {str(e)}")
        logger.error(f"Error in get_excel_sheets: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@table_bp.route('/export-table/<table_name>', methods=['GET'])
def export_table(table_name):
    """Export table data to CSV"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    insert_audit_trail('view_export_table', f"User '{session.get('username')}' viewed export page for table '{table_name}'.")
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (table_name,))
        
        if cursor.fetchone()[0] == 0:
            return jsonify({'success': False, 'message': f'Table "{table_name}" not found'})
        
        # Get all data
        cursor.execute(f"SELECT * FROM [{table_name}] ORDER BY id")
        
        # Create CSV response
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        columns = [column[0] for column in cursor.description]
        writer.writerow(columns)
        
        # Write data
        for row in cursor.fetchall():
            processed_row = []
            for value in row:
                if isinstance(value, datetime):
                    processed_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(value, date):
                    processed_row.append(value.strftime('%Y-%m-%d'))
                else:
                    processed_row.append(value)
            writer.writerow(processed_row)
        
        # Prepare response
        output.seek(0)
        csv_content = output.getvalue()
        
        response = make_response(csv_content)
        response.headers["Content-Disposition"] = f"attachment; filename={table_name}_export.csv"
        response.headers["Content-Type"] = "text/csv"
        
        insert_audit_trail('export_table', f"User '{session.get('username')}' exported data from table '{table_name}'.")
        return response
        
    except Exception as e:
        insert_audit_trail('export_table_failed', f"User '{session.get('username')}' failed to export data from table '{table_name}': {str(e)}")
        logger.error(f"Error exporting table {table_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error exporting table: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@table_bp.route('/duplicate-table/<table_name>', methods=['POST'])
def duplicate_table(table_name):
    """Duplicate an existing table structure"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
    conn = None
    cursor = None
    try:
        data = request.get_json()
        new_table_name = data.get('new_table_name', '').strip()
        
        if not new_table_name:
            return jsonify({'success': False, 'message': 'New table name is required'})
        
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', new_table_name):
            return jsonify({'success': False, 'message': 'Invalid table name format'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        # Check if source table exists
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (table_name,))
        
        if cursor.fetchone()[0] == 0:
            return jsonify({'success': False, 'message': f'Source table "{table_name}" not found'})
        
        # Check if new table name already exists
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (new_table_name,))
        
        if cursor.fetchone()[0] > 0:
            return jsonify({'success': False, 'message': f'Table "{new_table_name}" already exists'})
        
        # Get source table structure
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ? AND COLUMN_NAME NOT IN ('id', 'period_date', 'upload_date')
            ORDER BY ORDINAL_POSITION
        """, (table_name,))
        
        columns = cursor.fetchall()
        
        if not columns:
            return jsonify({'success': False, 'message': 'No columns found in source table'})
        
        # Create new table with same structure
        create_query = f"CREATE TABLE [{new_table_name}] (\n"
        create_query += "    [id] INT IDENTITY(1,1) PRIMARY KEY,\n"
        
        column_definitions = []
        for col in columns:
            col_name = col[0]
            col_type = col[1].upper()
            max_length = col[2]
            precision = col[3]
            scale = col[4]
            is_nullable = col[5]
            
            col_def = f"    [{col_name}] {col_type}"
            
            # Add length/precision
            if col_type in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR'] and max_length:
                col_def += f"({max_length})"
            elif col_type in ['DECIMAL', 'NUMERIC'] and precision:
                if scale and scale > 0:
                    col_def += f"({precision},{scale})"
                else:
                    col_def += f"({precision})"
            
            col_def += " NULL" if is_nullable == 'YES' else " NOT NULL"
            column_definitions.append(col_def)
        
        # Add standard columns
        column_definitions.append("    [period_date] DATE NULL")
        column_definitions.append("    [upload_date] DATETIME NOT NULL DEFAULT GETDATE()")
        
        create_query += ",\n".join(column_definitions)
        create_query += "\n)"
        
        cursor.execute(create_query)
        conn.commit()
        
        insert_audit_trail('duplicate_table', f"User '{session.get('username')}' duplicated table '{table_name}' to '{new_table_name}'.")
        return jsonify({
            'success': True,
            'message': f'Table "{new_table_name}" created successfully as duplicate of "{table_name}"',
            'new_table_name': new_table_name
        })
        
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        
        insert_audit_trail('duplicate_table_failed', f"User '{session.get('username')}' failed to duplicate table '{table_name}': {str(e)}")
        logger.error(f"Error duplicating table: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error duplicating table: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
