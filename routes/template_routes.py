import re
from flask import Blueprint, flash, render_template, request, jsonify, session, redirect, url_for
import logging

from utils.db_utils import get_master_divisions_tables
from models.audit import insert_audit_trail
from config.config import get_db_connection

template_bp = Blueprint('template', __name__)
logger = logging.getLogger(__name__)

@template_bp.route('/create-table', methods=['POST', 'GET'])
def create_table():
    if 'username' not in session:
        flash("Please log in first.")
        return redirect(url_for('auth.login'))

    role_access = session.get('role_access')
    fullname = session.get('fullname')
    username = session.get('username')
    division = session.get('division')
    divisions = get_master_divisions_tables()
    
    if request.method == 'GET':
        insert_audit_trail('view_create_table', f"User '{session.get('username')}' viewed create table page.")
        return render_template(
            'create_table.html',
            username=username,
            division=division,
            role_access=role_access,
            fullname=fullname,
            divisions=divisions
        )
    
    elif request.method == 'POST':
        conn = None
        cursor = None
        
        try:
            data = request.get_json()
            table_name = data.get('table_name', '').strip()
            columns = data.get('columns', [])
            divisions = data.get('divisions', '').strip()
            
            if not table_name:
                return jsonify({'success': False, 'message': 'Nama tabel harus diisi'})
            
            if not columns:
                return jsonify({'success': False, 'message': 'Minimal harus ada satu kolom'})
            
            if not divisions:
                return jsonify({'success': False, 'message': 'Divisi harus dipilih'})
            
            # Validate table name (alphanumeric and underscore only, no spaces)
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
                return jsonify({'success': False, 'message': 'Nama tabel hanya boleh mengandung huruf, angka, dan underscore. Harus dimulai dengan huruf dan tidak boleh ada spasi.'})
            
            # Get database connection
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Matikan autocommit dan gunakan explicit transaction
            conn.autocommit = False
            
            # Check table existence dengan lock
            cursor.execute("""
                SELECT COUNT(*) FROM sys.tables 
                WHERE name = ? AND type = 'U'
            """, (table_name,))
            
            if cursor.fetchone()[0] > 0:
                return jsonify({'success': False, 'message': f'Tabel "{table_name}" sudah ada dalam database'})
            
            # Build CREATE TABLE query with automatic columns
            create_query = f"CREATE TABLE [{table_name}] (\n"
            create_query += "    [id] INT IDENTITY(1,1) PRIMARY KEY,\n"
            
            column_definitions = []
            for col in columns:
                col_name = col.get('name', '').strip()
                reserved_columns = ['id', 'period_date', 'upload_date']
                
                if col_name.lower() in reserved_columns:
                    return jsonify({'success': False, 'message': f'Kolom "{col_name}" tidak boleh dibuat secara manual karena sudah ditambahkan otomatis'})
                
                col_type = col.get('type', 'VARCHAR')
                col_length = col.get('length', '')
                allow_nulls = col.get('allow_nulls', False)
                default_value = col.get('default_value', None)
                
                if not col_name:
                    return jsonify({'success': False, 'message': 'Semua kolom harus memiliki nama'})
                
                # Validate column name - DIPERBAIKI untuk menolak spasi
                if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', col_name):
                    return jsonify({'success': False, 'message': f'Nama kolom "{col_name}" tidak valid. Hanya boleh huruf, angka, dan underscore (tanpa spasi).'})
                
                # Build column definition
                col_def = f"    [{col_name}] {col_type}"
                
                # Add length for applicable types
                if col_type in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR'] and col_length:
                    try:
                        length_val = int(col_length)
                        if length_val <= 0 or length_val > 8000:
                            return jsonify({'success': False, 'message': f'Panjang kolom "{col_name}" harus antara 1-8000'})
                        col_def += f"({length_val})"
                    except ValueError:
                        return jsonify({'success': False, 'message': f'Panjang kolom "{col_name}" harus berupa angka'})
                elif col_type in ['DECIMAL', 'NUMERIC'] and col_length:
                    # For decimal types, allow precision,scale format
                    if ',' in col_length:
                        try:
                            precision, scale = col_length.split(',')
                            precision = int(precision.strip())
                            scale = int(scale.strip())
                            if precision < 1 or precision > 38 or scale < 0 or scale > precision:
                                return jsonify({'success': False, 'message': f'Precision/Scale tidak valid untuk kolom "{col_name}"'})
                            col_def += f"({precision},{scale})"
                        except ValueError:
                            return jsonify({'success': False, 'message': f'Format precision,scale tidak valid untuk kolom "{col_name}"'})
                    else:
                        try:
                            precision = int(col_length)
                            if precision < 1 or precision > 38:
                                return jsonify({'success': False, 'message': f'Precision tidak valid untuk kolom "{col_name}"'})
                            col_def += f"({precision})"
                        except ValueError:
                            return jsonify({'success': False, 'message': f'Precision harus berupa angka untuk kolom "{col_name}"'})
                
                # Add NULL/NOT NULL
                if not allow_nulls:
                    col_def += " NOT NULL"
                else:
                    col_def += " NULL"
                
                # BAGIAN PERBAIKAN: Handle default values dengan validasi yang lebih ketat
                if default_value not in [None, '']:
                    if col_type.upper() in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
                        # Properly escape single quotes and wrap with quotes
                        print(f"Default value for {col_name}: {default_value}")
                        escaped_value = default_value.replace("'", "''")
                        print(f"Escaped value for {col_name}: {escaped_value}")
                        col_def += f" DEFAULT '{escaped_value}'"
                    elif col_type.upper() in ['BIT']:
                        if default_value.lower() in ['0', '1', 'false', 'true']:
                            bit_value = '1' if default_value.lower() in ['1', 'true'] else '0'
                            col_def += f" DEFAULT {bit_value}"
                        else:
                            return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" bertipe BIT harus 0, 1, true, atau false'})
                    elif col_type.upper() in ['DATE', 'DATETIME', 'DATETIME2']:
                        # Handle special datetime functions
                        if default_value.upper() in ['GETDATE()', 'GETUTCDATE()', 'SYSDATETIME()', 'CURRENT_TIMESTAMP']:
                            col_def += f" DEFAULT {default_value.upper()}"
                        else:
                            # Validate date format
                            try:
                                from datetime import datetime
                                # Try multiple date formats
                                date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%d/%m/%Y']
                                parsed_date = None
                                for fmt in date_formats:
                                    try:
                                        parsed_date = datetime.strptime(default_value, fmt)
                                        break
                                    except ValueError:
                                        continue
                                
                                if parsed_date is None:
                                    raise ValueError("Invalid date format")
                                    
                                # Format untuk SQL Server
                                formatted_date = parsed_date.strftime('%Y-%m-%d')
                                col_def += f" DEFAULT '{formatted_date}'"
                            except ValueError:
                                return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" harus berformat YYYY-MM-DD atau fungsi seperti GETDATE()'})
                    elif col_type.upper() in ['TIME']:
                        # Handle time format
                        try:
                            from datetime import datetime
                            time_formats = ['%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p']
                            parsed_time = None
                            for fmt in time_formats:
                                try:
                                    parsed_time = datetime.strptime(default_value, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_time is None:
                                raise ValueError("Invalid time format")
                                
                            formatted_time = parsed_time.strftime('%H:%M:%S')
                            col_def += f" DEFAULT '{formatted_time}'"
                        except ValueError:
                            return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" harus berformat HH:MM:SS'})
                    elif col_type.upper() in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL']:
                        # Numeric types - validate that it's a number
                        try:
                            # Additional validation for integer ranges
                            if col_type.upper() == 'TINYINT':
                                val = int(float(default_value))
                                if val < 0 or val > 255:
                                    return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" bertipe TINYINT harus antara 0-255'})
                            elif col_type.upper() == 'SMALLINT':
                                val = int(float(default_value))
                                if val < -32768 or val > 32767:
                                    return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" bertipe SMALLINT harus antara -32768 hingga 32767'})
                            elif col_type.upper() == 'INT':
                                val = int(float(default_value))
                                if val < -2147483648 or val > 2147483647:
                                    return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" bertipe INT harus antara -2147483648 hingga 2147483647'})
                            
                            # Validate float for decimal types
                            float(default_value)
                            col_def += f" DEFAULT {default_value}"
                        except ValueError:
                            return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" harus berupa angka yang valid'})
                        except OverflowError:
                            return jsonify({'success': False, 'message': f'Default value untuk kolom "{col_name}" terlalu besar'})
                    else:
                        # For other types, wrap in quotes as string
                        escaped_value = default_value.replace("'", "''")
                        col_def += f" DEFAULT '{escaped_value}'"
                elif not allow_nulls:
                    # Jika tidak allow nulls dan tidak ada default value, berikan error
                    return jsonify({'success': False, 'message': f'Kolom "{col_name}" harus memiliki default value karena tidak mengizinkan NULL'})

                column_definitions.append(col_def)
            
            # Add automatic columns
            column_definitions.append("    [period_date] DATE NULL")
            column_definitions.append("    [upload_date] DATETIME NOT NULL DEFAULT GETDATE()")
            
            create_query += ",\n".join(column_definitions)
            create_query += "\n)"
            
            logger.info(f"Creating table with query: {create_query}")
            
            # Execute dan verify dengan handling yang lebih baik
            try:
                # Insert to MasterCreator first
                cursor.execute("""
                    INSERT INTO MasterCreator (template_name, division_name, create_date, create_by)
                    VALUES (?, ?, GETDATE(), ?)
                """, (table_name, divisions, username))
                
                # Create the table
                cursor.execute(create_query)
                
                # Wait and verify table creation
                import time
                time.sleep(0.5)
                
                # Verify table was created successfully
                cursor.execute("""
                    SELECT COUNT(*) FROM sys.tables 
                    WHERE name = ? AND type = 'U'
                """, (table_name,))
                
                table_count = cursor.fetchone()[0]
                
                if table_count == 0:
                    conn.rollback()
                    logger.error(f"Template {table_name} was not created successfully")
                    return jsonify({'success': False, 'message': 'Tabel gagal dibuat. Silakan periksa log database.'})
                
                # Verify columns are created correctly
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = ?
                """, (table_name,))
                
                column_count = cursor.fetchone()[0]
                expected_count = len(columns) + 3  # +3 for id, period_date, upload_date
                
                if column_count != expected_count:
                    logger.warning(f"Template {table_name} created but column count mismatch. Expected: {expected_count}, Actual: {column_count}")
                
                # Commit hanya jika verifikasi berhasil
                conn.commit()
                logger.info(f"Template {table_name} created and verified successfully with {column_count} columns")
                insert_audit_trail('create_table', f"User '{session.get('username')}' created table '{table_name}'.")
                
                return jsonify({
                    'success': True,
                    'message': f'Template "{table_name}" berhasil dibuat dengan {column_count} kolom.',
                    'table_name': table_name,
                    'columns_created': len(columns),
                    'total_columns': column_count,
                    'automatic_columns': ['id', 'period_date', 'upload_date'],
                    'query': create_query
                })
                
            except Exception as create_error:
                conn.rollback()
                logger.error(f"Error creating table {table_name}: {str(create_error)}")
                
                # Handle specific SQL Server errors
                error_msg = str(create_error)
                if "42S01" in error_msg or "2714" in error_msg:
                    return jsonify({'success': False, 'message': f'Template "{table_name}" sudah ada dalam database'})
                elif "2705" in error_msg:
                    return jsonify({'success': False, 'message': 'Nama kolom duplikat atau tidak valid'})
                elif "102" in error_msg:
                    return jsonify({'success': False, 'message': 'Syntax error dalam query SQL. Periksa tipe data dan default values.'})
                elif "245" in error_msg:
                    return jsonify({'success': False, 'message': 'Error konversi tipe data. Periksa default values yang dimasukkan.'})
                elif "2627" in error_msg:
                    return jsonify({'success': False, 'message': 'Terdapat duplikasi data atau constraint violation.'})
                else:
                    return jsonify({'success': False, 'message': f'Error database: {error_msg}'})
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            logger.error(f"Error creating table: {str(e)}")
            insert_audit_trail('create_table_failed', f"User '{session.get('username')}' failed to create table '{table_name}': {str(e)}")
            return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

@template_bp.route('/delete-table', methods=['POST'])
def delete_table():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not authenticated'})
    
    try:
        data = request.get_json()
        table_name = data.get('table_name', '').strip()
        
        if not table_name:
            return jsonify({'success': False, 'message': 'Table name is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        try:
            # Check if it's a template first
            cursor.execute("""
                SELECT COUNT(*) FROM MasterCreator 
                WHERE template_name = ?
            """, (table_name,))
            
            is_template = cursor.fetchone()[0] > 0
            
            if is_template:
                # Delete from template table
                cursor.execute("""
                    DELETE FROM MasterCreator 
                    WHERE template_name = ?
                """, (table_name,))
                
                # Also check and drop actual table if it exists
                cursor.execute("""
                    SELECT COUNT(*) FROM sys.tables 
                    WHERE name = ? AND type = 'U'
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0] > 0
                
                if table_exists:
                    cursor.execute(f"DROP TABLE [{table_name}]")
                    logger.info(f"Dropped table {table_name}")
                
                conn.commit()
                insert_audit_trail('delete_table', f"User '{session.get('username')}' deleted table '{table_name}'.")
                return jsonify({
                    'success': True, 
                    'message': f'Template "{table_name}" berhasil dihapus'
                })
            else:
                # Check if it's an actual table
                cursor.execute("""
                    SELECT COUNT(*) FROM sys.tables 
                    WHERE name = ? AND type = 'U'
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0] > 0
                
                if table_exists:
                    cursor.execute(f"DROP TABLE [{table_name}]")
                    conn.commit()
                    insert_audit_trail('delete_table', f"User '{session.get('username')}' deleted table '{table_name}'.")
                    logger.info(f"Dropped table {table_name}")
                    return jsonify({
                        'success': True, 
                        'message': f'Table "{table_name}" berhasil dihapus'
                    })
                else:
                    insert_audit_trail('delete_table_failed', f"User '{session.get('username')}' attempted to delete non-existent table '{table_name}'.")
                    return jsonify({
                        'success': False, 
                        'message': f'Table "{table_name}" not found'
                    })
                    
        except Exception as e:
            conn.rollback()
            logger.error(f"Error deleting table {table_name}: {str(e)}")
            return jsonify({
                'success': False, 
                'message': f'Error deleting table: {str(e)}'
            })
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in delete_table: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@template_bp.route('/get-template-details/<template_name>')
def get_template_details(template_name):
    """
    Get detailed information about a template including columns
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get template basic info from MasterCreator
        cursor.execute("""
            SELECT mc.template_name, mc.division_name, mc.create_date, mc.create_by
            FROM MasterCreator mc
            WHERE mc.template_name = ?
        """, (template_name,))
        
        template_info = cursor.fetchone()
        
        if not template_info:
            return jsonify({'success': False, 'message': 'Template not found in MasterCreator'})
        
        # Get column information from INFORMATION_SCHEMA
        cursor.execute("""
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.ORDINAL_POSITION,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY,
                CASE WHEN COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1 
                     THEN 1 ELSE 0 END as IS_IDENTITY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk ON 
                c.TABLE_NAME = pk.TABLE_NAME AND 
                c.COLUMN_NAME = pk.COLUMN_NAME AND 
                pk.CONSTRAINT_NAME LIKE 'PK_%'
            WHERE c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (template_name,))
        
        columns_raw = cursor.fetchall()
        
        if not columns_raw:
            return jsonify({'success': False, 'message': f'Template "{template_name}" not found in database'})
        
        # Format column information
        columns = []
        for col in columns_raw:
            # Format data type display
            data_type_display = col[1]
            if col[2] and col[2] != -1:  # CHARACTER_MAXIMUM_LENGTH
                data_type_display += f"({col[2]})"
            elif col[3] and col[4] is not None:  # NUMERIC_PRECISION and SCALE
                data_type_display += f"({col[3]},{col[4]})"
            elif col[3]:  # NUMERIC_PRECISION only
                data_type_display += f"({col[3]})"
            
            # Clean up default value display
            default_display = col[6]
            if default_display:
                # Remove extra parentheses from SQL Server default values
                if default_display.startswith('(') and default_display.endswith(')'):
                    default_display = default_display[1:-1]
                # Remove quotes from string defaults
                if default_display.startswith("'") and default_display.endswith("'"):
                    default_display = default_display[1:-1]
            
            column_info = {
                'name': col[0],
                'data_type': data_type_display,
                'raw_data_type': col[1],
                'max_length': col[2],
                'numeric_precision': col[3],
                'numeric_scale': col[4],
                'is_nullable': col[5] == 'YES',
                'default_value': default_display,
                'raw_default_value': col[6],
                'ordinal_position': col[7],
                'is_primary_key': bool(col[8]),
                'is_identity': bool(col[9])
            }
            columns.append(column_info)
        
        template_details = {
            'name': template_info[0],
            'division': template_info[1],
            'create_date': template_info[2].strftime('%Y-%m-%d %H:%M:%S') if template_info[2] else None,
            'created_by': template_info[3],
            'total_columns': len(columns),
            'user_columns': len([c for c in columns if c['name'] not in ['id', 'period_date', 'upload_date']]),
            'columns': columns
        }

        insert_audit_trail('get_template_details', f"User '{session.get('username')}' accessed template details for '{template_name}'.")

        return jsonify({
            'success': True,
            'template': template_details
        })
        
    except Exception as e:
        logger.error(f"Error getting template details for {template_name}: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@template_bp.route('/save-as-template', methods=['POST'])
def save_as_template():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not authenticated'})
    insert_audit_trail('view_save_as_template', f"User '{session.get('username')}' viewed save as template page.")
    
    try:
        data = request.get_json()
        table_name = data.get('table_name', '').strip()
        columns = data.get('columns', [])
        division = data.get('division', '').strip()
        username = session.get('username')
        
        if not table_name or not columns or not division:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        # Validate table name
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
            return jsonify({'success': False, 'message': 'Invalid table name format'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        try:
            # Check if template already exists
            cursor.execute("""
                SELECT COUNT(*) FROM MasterCreator 
                WHERE template_name = ?
            """, (table_name,))
            
            if cursor.fetchone()[0] > 0:
                return jsonify({'success': False, 'message': 'Template already exists'})
            
            # Save template metadata
            cursor.execute("""
                INSERT INTO MasterCreator (template_name, division_name, create_date, create_by)
                VALUES (?, ?, GETDATE(), ?)
            """, (table_name, division, username))
            
            # You might want to save column definitions in a separate table
            # For now, just save the template record
            
            conn.commit()
            logger.info(f"Template {table_name} saved successfully")
            
            insert_audit_trail('save_as_template', f"User '{username}' saved new template '{table_name}' for division '{division}'.")
            return jsonify({
                'success': True,
                'message': f'Template "{table_name}" saved successfully'
            })
            
        except Exception as e:
            insert_audit_trail('save_as_template_failed', f"User '{username}' failed to save template '{table_name}': {str(e)}")
            conn.rollback()
            logger.error(f"Error saving template {table_name}: {str(e)}")
            return jsonify({'success': False, 'message': f'Error saving template: {str(e)}'})
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in save_as_template: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

# @admin_bp.route('/get-data-count', methods=['GET'])
# def get_data_count():
#     if 'username' not in session:
#         return jsonify({'success': False, 'message': 'Session expired.'}), 401
#     table_name = request.args.get('table_name')
#     if not table_name:
#         return jsonify({'success': False, 'message': 'Parameter table_name wajib diisi.'})
#     counts = get_data_count_by_period(table_name)
#     return jsonify({'success': True, 'data': counts})

# @admin_bp.route('/get-divisions', methods=['GET'])
# def get_divisions():
#     if 'username' not in session:
#         return jsonify({'success': False, 'message': 'Session expired.'}), 401
#     divisions = get_master_divisions_tables()
#     return jsonify({'success': True, 'divisions': divisions})

# # ------------------------------------------------------------
# # Get Data Preview (Generic /data route)
# # ------------------------------------------------------------
# @admin_bp.route('/data', methods=['GET'])
# def get_data_preview():
#     """
#     Preview data from a specific table.
#     Example:
#       /data?table_name=MasterDebitur&limit=50
#     """
#     if 'username' not in session:
#         return redirect(url_for('auth.login'))

#     table_name = request.args.get('table_name')
#     limit = int(request.args.get('limit', 50))

#     if not table_name:
#         return jsonify({'success': False, 'message': 'Parameter table_name wajib diisi.'}), 400

#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Simple preview query
#         query = f"SELECT TOP {limit} * FROM [{table_name}] ORDER BY 1 DESC"
#         cursor.execute(query)

#         columns = [col[0] for col in cursor.description]
#         rows = cursor.fetchall()

#         data = [dict(zip(columns, row)) for row in rows]

#         return jsonify({
#             'success': True,
#             'table_name': table_name,
#             'total': len(data),
#             'data': data
#         })

#     except Exception as e:
#         return jsonify({'success': False, 'message': f'Gagal mengambil data: {e}'})
#     finally:
#         try:
#             cursor.close()
#             conn.close()
#         except Exception:
#             pass

