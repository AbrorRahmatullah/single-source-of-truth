from decimal import Decimal
import logging
from datetime import date, datetime

from config.config import get_db_connection
from utils.helpers import normalize_value

logger = logging.getLogger(__name__)

def get_automatic_columns():
    """
    Mendapatkan daftar kolom yang otomatis ditambahkan sistem
    """
    return ['id', 'period_date', 'upload_date']

def get_column_info(table_name, exclude_automatic=True):
    """
    Mendapatkan informasi kolom dari tabel database dengan informasi lengkap
    Args:
        table_name: Nama tabel
        exclude_automatic: True untuk mengecualikan kolom otomatis (period_date, upload_date)
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME = ? AND c.TABLE_SCHEMA = 'dbo'
        ORDER BY c.ORDINAL_POSITION
        """
        
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        
        if not columns:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan atau tidak memiliki kolom")
        
        columns_info = {}
        automatic_columns = ['period_date', 'upload_date', 'id']  # Kolom yang otomatis ditambahkan
        
        for col in columns:
            column_name = col[0]
            
            # Skip kolom otomatis jika diminta
            if exclude_automatic and column_name.lower() in automatic_columns:
                continue
                
            columns_info[column_name] = {
                'name': column_name,  # Tambahkan nama kolom untuk error messages
                'data_type': col[1],
                'max_length': col[2],
                'precision': col[3],
                'scale': col[4],
                'is_nullable': col[5] == 'YES',
                'default_value': col[6]
            }
        
        return columns_info
        
    except Exception as e:
        logger.error(f"Error getting column info: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def convert_value_for_sql_server(value):
    """
    Convert Python values specifically for SQL Server driver compatibility
    """
    if value is None:
        return None
    
    # Boolean conversion - critical for SQL Server driver
    elif isinstance(value, bool):
        return 1 if value else 0
    
    # String handling - ensure proper encoding
    elif isinstance(value, str):
        # Remove null bytes yang bisa menyebabkan error
        return value.replace('\x00', '')
    
    # Numeric types
    elif isinstance(value, (int, float)):
        return value
    
    # Decimal handling
    elif isinstance(value, Decimal):
        return float(value)
    
    # DateTime handling
    elif isinstance(value, (datetime, date)):
        return value
    
    # Complex types - convert to string
    elif isinstance(value, (list, dict, tuple)):
        return str(value)
    
    # Bytes handling
    elif isinstance(value, bytes):
        return value
    
    # Default case
    else:
        return str(value)

def safe_insert_single_record(table_name, columns, values):
    """
    Helper function untuk insert single record dengan error handling
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert values
        converted_values = [convert_value_for_sql_server(value) for value in values]
        
        # Build query
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Debug info
        print(f"Inserting to {table_name}:")
        for col, val in zip(columns, converted_values):
            print(f"  {col}: {type(val).__name__} = {repr(val)}")
        
        cursor.execute(query, tuple(converted_values))
        conn.commit()
        print(f"✅ Successfully inserted to {table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting to {table_name}: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_to_database(df, table_name, periode_date=None, replace_existing=True):
    """
    Insert dataframe ke SQL Server table - data sudah tervalidasi
    Includes period_date and upload_date automatic columns
    PERBAIKAN: Handle database default values
    """
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # PERBAIKAN: Filter kolom yang menggunakan database default
        # Check which columns have database defaults
        columns_with_defaults = {}
        for col in df.columns:
            # Check if any row in this column has the database default marker
            has_default_marker = df[col].astype(str).str.contains('__USE_DATABASE_DEFAULT__').any()
            if has_default_marker:
                columns_with_defaults[col] = True

        logger.info(f"Kolom dengan database default: {list(columns_with_defaults.keys())}")

        # PERBAIKAN: Build dynamic column list per row
        successful_inserts = 0
        current_datetime = datetime.now()
        
        # Delete existing data if replace_existing is True
        if replace_existing and periode_date:
            delete_query = f"DELETE FROM {table_name} WHERE period_date = ?"
            cursor.execute(delete_query, (periode_date,))
            logger.info(f"Data sebelumnya dengan periode {periode_date} telah dihapus dari {table_name}")

        # PERBAIKAN: Process each row individually to handle different column sets
        for idx, row in df.iterrows():
            try:
                # Build column list and values for this specific row
                insert_columns = []
                insert_values = []
                placeholders = []

                # Process data columns
                for col in df.columns:
                    raw_value = row[col]
                    
                    # PERBAIKAN: Skip kolom jika menggunakan database default
                    if str(raw_value) == '__USE_DATABASE_DEFAULT__':
                        logger.debug(f"Row {idx + 1}: Skipping column '{col}' - using database default")
                        continue
                    
                    # Include column in insert
                    insert_columns.append(f'[{col}]')
                    placeholders.append('?')
                    
                    # Normalize value
                    dtype = str(df[col].dtype)
                    value = normalize_value(raw_value, dtype)
                    insert_values.append(value)

                # Add automatic columns safely (avoid duplication)
                existing_columns = [c.strip('[]') for c in insert_columns]

                if 'period_date' not in existing_columns:
                    insert_columns.append('[period_date]')
                    placeholders.append('?')
                    insert_values.append(periode_date)

                if 'upload_date' not in existing_columns:
                    insert_columns.append('[upload_date]')
                    placeholders.append('GETDATE()')

                if not insert_columns:
                    logger.warning(f"Row {idx + 1}: No columns to insert, skipping")
                    continue

                # Build and execute query for this row
                insert_query = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({', '.join(placeholders)})"
                
                logger.debug(f"Row {idx + 1} Query: {insert_query}")
                logger.debug(f"Row {idx + 1} Values: {insert_values}")
                
                cursor.execute(insert_query, insert_values)
                successful_inserts += 1

            except Exception as row_error:
                logger.error(f"Error inserting row {idx + 1}: {str(row_error)}")
                # Continue with next row instead of failing completely
                continue

        conn.commit()
        logger.info(f"Berhasil insert {successful_inserts} dari {len(df)} baris")

        return {
            'success': True,
            'message': f'Berhasil insert {successful_inserts} baris data',
            'inserted_rows': successful_inserts,
            'skipped_rows': len(df) - successful_inserts,
            'error_rows': 0,
            'columns_used': [col for col in df.columns.tolist() if col not in columns_with_defaults],
            'columns_with_defaults': list(columns_with_defaults.keys()),
            'periode_date': periode_date,
            'upload_date': current_datetime.strftime('%Y-%m-%d %H:%M:%S')
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error inserting to database: {str(e)}")
        return {
            'success': False,
            'message': f'Error saat insert ke database: {str(e)}',
            'inserted_rows': 0,
            'skipped_rows': 0,
            'error_rows': len(df) if 'df' in locals() else 0
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_template_tables(role_access=None, division=None):
    """
    Mendapatkan daftar template dari MasterCreator.
    Jika role_access = 'user', hanya kembalikan template yang sesuai division user.
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if role_access and role_access.lower() == 'user':
            cursor.execute("""
                SELECT template_name
                FROM MasterCreator
                WHERE division_name = ?
                ORDER BY create_date DESC
            """, (division,))
        else:
            cursor.execute("""
                SELECT template_name
                FROM MasterCreator
                ORDER BY create_date DESC
            """)
        
        tables = cursor.fetchall()
        return [table[0] for table in tables]
        
    except Exception as e:
        logger.error(f"Error getting template tables: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_data_count_by_period(table_name):
    """
    Mendapatkan jumlah data berdasarkan period_date untuk tabel tertentu
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists and has period_date column
        check_query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ? AND COLUMN_NAME = 'period_date' AND TABLE_SCHEMA = 'dbo'
        """
        cursor.execute(check_query, (table_name,))
        has_period_date = cursor.fetchone()[0] > 0
        
        if not has_period_date:
            return []
        
        # Get data count grouped by period_date
        query = f"""
        SELECT TOP 5
            period_date,
            COUNT(*) as total_records
        FROM [{table_name}]
        GROUP BY period_date
        ORDER BY period_date DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        data_counts = []
        for row in results:
            data_counts.append({
                'period_date': row[0].strftime('%B %Y') if row[0] else 'NULL',
                'total_records': row[1]
            })
        
        return data_counts
        
    except Exception as e:
        logger.error(f"Error getting data count by period: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
def get_master_divisions_tables():
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT division_name
            FROM MasterDivisions
        """)

        tables = cursor.fetchall()
        return [table[0] for table in tables]

    except Exception as e:
        logger.error(f"Error getting template tables: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def check_master_uploader_by_date(filter_date):
    """
    Check MasterUploader table for templates with latest upload dates on a given filter date.
    This function retrieves template upload information for the specified period_date.

    Args:
        filter_date (str or date): The filter date in format 'YYYY-MM-DD' or date object

    Returns:
        list: List of dictionaries containing:
            - template: Template name
            - upload_date: Maximum upload date for that template
        Returns empty list if no data found or on error
    """
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Convert filter_date to string if it's a date object
        if isinstance(filter_date, (date, datetime)):
            filter_date_str = filter_date.strftime('%Y-%m-%d')
        else:
            filter_date_str = str(filter_date)

        query = """
            SELECT
                template,
                MAX(upload_date) AS upload_date
            FROM MasterUploader
            WHERE CAST(period_date AS DATE) = CAST(? AS DATE)
            GROUP BY template
        """

        cursor.execute(query, (filter_date_str,))
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                'template': row[0],
                'upload_date': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None
            })

        return results

    except Exception as e:
        logger.error(f"Error checking master uploader by date '{filter_date}': {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

