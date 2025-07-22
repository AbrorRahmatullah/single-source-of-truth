import csv
from decimal import Decimal, InvalidOperation
import io
import pandas as pd
import pyodbc
import os
import re
import logging

from flask import Flask, flash, make_response, request, render_template, jsonify, redirect, session, url_for
from functools import wraps
from werkzeug.utils import secure_filename
from datetime import datetime, time, date
from flask_bcrypt import Bcrypt
from app.config import get_db_connection

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.secret_key = 'rahasiayangsangatrahasia'  # Ganti dengan secret key yang kuat
bcrypt = Bcrypt(app)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Pastikan folder upload ada
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Decorator untuk proteksi admin
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash("Please log in first.")
            return redirect(url_for('login'))
        
        if session.get('role_access') != 'admin':
            flash("Access denied. Admin privileges required.")
            return redirect(url_for('upload_file'))
        
        return f(*args, **kwargs)
    return decorated_function

# Decorator untuk proteksi user login
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash("Please log in first.")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def render_alert(message, redirect_url, username, fullname, email, role_access=None, division=None):
    return '''
    <script>
        alert("{}");
        window.location.href = "{}";
    </script>
    '''.format(message, url_for(redirect_url))

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['xlsx', 'xls']

def find_primary_header_row(df, primary_header_pattern=None):
    """
    Mencari baris yang mengandung header utama (dinamis)
    Args:
        df: DataFrame Excel
        primary_header_pattern: Pattern header utama (bisa None untuk auto-detect)
    
    Returns: 
        tuple (header_row_index, detected_primary_header)
    """
    
    def normalize_text(text):
        """Normalisasi teks untuk perbandingan"""
        if pd.isna(text):
            return ""
        return str(text).lower().strip()
    
    def is_likely_header_row(row_values):
        """
        Menentukan apakah baris ini kemungkinan baris header
        Kriteria: mengandung teks yang tidak kosong dan tidak numeric
        """
        non_empty_count = 0
        text_count = 0
        
        for val in row_values:
            if pd.notna(val) and str(val).strip():
                non_empty_count += 1
                # Cek apakah nilai bukan angka murni
                try:
                    float(str(val))
                except ValueError:
                    text_count += 1
        
        # Header row harus punya minimal 3 kolom berisi dan mayoritas teks
        return non_empty_count >= 3 and text_count >= (non_empty_count * 0.6)
    
    # Jika primary_header_pattern diberikan, cari berdasarkan pattern tersebut
    if primary_header_pattern:
        pattern_normalized = normalize_text(primary_header_pattern)
        
        for idx, row in df.iterrows():
            row_values = [normalize_text(val) for val in row]
            
            # Cari pattern di baris ini
            for val in row_values:
                if pattern_normalized in val or val in pattern_normalized:
                    if is_likely_header_row(row_values):
                        logger.info(f"Header row ditemukan di baris {idx + 1} berdasarkan pattern '{primary_header_pattern}'")
                        return idx, primary_header_pattern
    
    # Auto-detect: Cari baris yang kemungkinan header
    common_header_keywords = [
        'number', 'name', 'id', 'code', 'facility', 'location', 'type', 
        'date', 'status', 'description', 'value', 'amount', 'quantity'
    ]
    
    best_header_row = None
    best_score = 0
    detected_primary = None
    
    for idx, row in df.iterrows():
        if idx > 20:  # Batasi pencarian di 20 baris pertama
            break
            
        row_values = [str(val) for val in row if pd.notna(val)]
        
        if not is_likely_header_row(row_values):
            continue
        
        # Scoring berdasarkan keyword yang ditemukan
        score = 0
        primary_candidate = None
        
        for val in row_values:
            val_normalized = normalize_text(val)
            for keyword in common_header_keywords:
                if keyword in val_normalized:
                    score += 1
                    if not primary_candidate and ('number' in val_normalized or 'id' in val_normalized):
                        primary_candidate = str(val).strip()
        
        if score > best_score:
            best_score = score
            best_header_row = idx
            detected_primary = primary_candidate or str(row_values[0]).strip()
    
    if best_header_row is not None:
        logger.info(f"Header row auto-detected di baris {best_header_row + 1}, primary header: '{detected_primary}'")
        return best_header_row, detected_primary
    
    raise ValueError("Tidak dapat menemukan baris header. Pastikan file Excel memiliki baris header yang jelas.")

def find_header_row_and_validate(df, required_headers, primary_header_pattern=None):
    """
    Mencari baris header dan memvalidasi keberadaan semua header yang diperlukan.
    Sekarang mendukung mapping kolom mirip seperti 'FACILITY NUMBER' dan 'NEW FACILITY NUMBER' tanpa tertimpa.
    """
    def normalize_header(header):
        """Normalisasi header untuk perbandingan"""
        if pd.isna(header):
            return ""
        normalized = re.sub(r'[^\w\s]', '', str(header))
        normalized = re.sub(r'\s+', ' ', normalized).lower().strip()
        return normalized

    def is_header_match(excel_header, db_header):
        """Cek apakah dua header cocok dengan berbagai variasi"""
        if pd.isna(excel_header) or pd.isna(db_header):
            return False
        excel_norm = normalize_header(excel_header)
        db_norm = normalize_header(db_header)

        if not excel_norm or not db_norm:
            return False

        # Prioritaskan exact match
        if excel_norm == db_norm:
            return True
        # Spasi ke underscore
        if excel_norm.replace(' ', '_') == db_norm.replace(' ', '_'):
            return True
        # Hapus separator
        if re.sub(r'[\s_-]+', '', excel_norm) == re.sub(r'[\s_-]+', '', db_norm):
            return True
        # Substring match minimal 4 karakter
        if len(excel_norm) >= 4 and (excel_norm in db_norm or db_norm in excel_norm):
            return True

        return False

    # Cari baris header
    header_row, detected_primary = find_primary_header_row(df, primary_header_pattern)

    # Ambil semua header dari baris tersebut
    excel_headers = []
    for val in df.iloc[header_row]:
        if pd.notna(val):
            excel_headers.append(str(val).strip())
        else:
            excel_headers.append("")

    # Validasi dan mapping
    valid_headers_mapping = []  # List of tuples: (excel_header, db_header)
    missing_headers = []
    found_headers = []
    match_details = []

    matched_excel_indices = set()  # Untuk menghindari mapping dua kali ke kolom Excel yang sama

    for db_header in required_headers:
        matching_excel_header = None
        match_type = ""

        for idx, excel_header in enumerate(excel_headers):
            if idx in matched_excel_indices:
                continue

            if excel_header and is_header_match(excel_header, db_header):
                matching_excel_header = excel_header
                matched_excel_indices.add(idx)

                # Tentukan tipe match
                excel_norm = normalize_header(excel_header)
                db_norm = normalize_header(db_header)
                if excel_norm == db_norm:
                    match_type = "exact"
                elif excel_norm.replace(' ', '_') == db_norm.replace(' ', '_'):
                    match_type = "separator_normalized"
                elif re.sub(r'[\s_-]+', '', excel_norm) == re.sub(r'[\s_-]+', '', db_norm):
                    match_type = "no_separator"
                else:
                    match_type = "substring"
                break

        if matching_excel_header:
            valid_headers_mapping.append((matching_excel_header, db_header))
            found_headers.append(db_header)
            match_details.append(f"'{matching_excel_header}' -> '{db_header}' ({match_type})")
        else:
            missing_headers.append(db_header)

    logger.info(f"Header row ditemukan di baris: {header_row + 1}")
    logger.info(f"Primary header detected: '{detected_primary}'")
    logger.info(f"Header yang ditemukan: {found_headers}")

    if match_details:
        logger.info("Detail pencocokan header:")
        for detail in match_details:
            logger.info(f"  {detail}")

    if missing_headers:
        logger.warning(f"Header yang tidak ditemukan: {missing_headers}")

    if not valid_headers_mapping:
        raise ValueError("Tidak ada header yang cocok antara Excel dan database")

    return header_row, valid_headers_mapping, missing_headers, detected_primary

def find_data_start_row(df, header_row, detected_primary_header):
    """
    Mencari baris mulai data berdasarkan kolom primary header yang terdeteksi
    Args:
        df: DataFrame Excel
        header_row: Index baris header
        detected_primary_header: Header utama yang terdeteksi
    
    Returns: data_start_row_index
    """
    # Cari index kolom primary header
    excel_headers = []
    for val in df.iloc[header_row]:
        excel_headers.append(str(val).strip() if pd.notna(val) else "")
    
    primary_col_index = None
    
    # Cari kolom yang cocok dengan detected primary header
    for idx, header in enumerate(excel_headers):
        if header and (
            header.lower().strip() == detected_primary_header.lower().strip() or
            detected_primary_header.lower().strip() in header.lower().strip() or
            header.lower().strip() in detected_primary_header.lower().strip()
        ):
            primary_col_index = idx
            break
    
    # Jika tidak ditemukan, gunakan kolom pertama yang tidak kosong
    if primary_col_index is None:
        for idx, header in enumerate(excel_headers):
            if header:
                primary_col_index = idx
                logger.warning(f"Primary header tidak ditemukan, menggunakan kolom pertama: '{header}'")
                break
    
    if primary_col_index is None:
        raise ValueError("Tidak dapat menentukan kolom primary untuk mencari data")
    
    # Cari baris pertama yang memiliki data pada kolom primary
    data_start_row = None
    for idx in range(header_row + 1, len(df)):
        if idx >= len(df):
            break
            
        cell_value = str(df.iloc[idx, primary_col_index]).strip()
        if cell_value and cell_value.lower() not in ['nan', 'none', '', 'null']:
            # Pastikan ini bukan baris kosong atau header tambahan
            row_data = [str(val).strip() for val in df.iloc[idx] if pd.notna(val)]
            if len(row_data) >= 2:  # Minimal 2 kolom berisi data
                data_start_row = idx
                break
    
    if data_start_row is None:
        raise ValueError(f"Tidak ditemukan data setelah header pada kolom '{detected_primary_header}'")
    
    logger.info(f"Data mulai dari baris: {data_start_row + 1} (Excel row: {data_start_row + 2})")
    return data_start_row

def get_column_info(table_name, exclude_automatic=True):
    """
    Mendapatkan informasi kolom dari tabel database
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
                'data_type': col[1],
                'max_length': col[2],
                'precision': col[3],
                'scale': col[4],
                'is_nullable': col[5] == 'YES',
                'default': col[6]
            }
        
        return columns_info
        
    except Exception as e:
        logger.error(f"Error getting column info: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()

def get_automatic_columns():
    """
    Mendapatkan daftar kolom yang otomatis ditambahkan sistem
    """
    return ['id', 'period_date', 'upload_date']

def validate_and_convert_value(value, column_info, column_name):
    """
    Validasi dan konversi nilai sesuai dengan tipe data kolom
    Returns: (converted_value, is_valid, error_message)
    """
    data_type = column_info['data_type']
    is_nullable = column_info['is_nullable']
    max_length = column_info['max_length']
    precision = column_info['precision']
    scale = column_info['scale']
    
    # Handle NULL values
    if pd.isna(value) or value is None or (isinstance(value, str) and value.strip() == ''):
        if is_nullable:
            return None, True, None
        else:
            return None, False, f"Column '{column_name}' cannot be NULL"
    
    # Convert to string for processing
    str_value = str(value).strip()
    
    # Handle different data types
    try:
        if data_type in ['int', 'integer', 'bigint', 'smallint', 'tinyint']:
            return validate_integer(str_value, column_name)
            
        elif data_type in ['decimal', 'numeric', 'money', 'smallmoney']:
            return validate_decimal(str_value, precision, scale, column_name)
            
        elif data_type in ['float', 'real']:
            return validate_float(str_value, column_name)
            
        elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
            return validate_string(str_value, max_length, column_name)
            
        elif data_type in ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset']:
            return validate_datetime(str_value, data_type, column_name)
            
        elif data_type in ['bit']:
            return validate_boolean(str_value, column_name)
            
        else:
            # For unknown types, return as string
            logger.warning(f"Unknown data type '{data_type}' for column '{column_name}', treating as string")
            return str_value, True, None
            
    except Exception as e:
        return None, False, f"Error validating column '{column_name}': {str(e)}"

def validate_integer(value, column_name):
    """Validate and convert to integer"""
    try:
        # Remove whitespace and check for empty string
        if not value or value.isspace():
            return None, False, f"Empty value for integer column '{column_name}'"
        
        # Try to convert to int
        if isinstance(value, (int, float)):
            if isinstance(value, float) and not value.is_integer():
                return None, False, f"Float value '{value}' cannot be converted to integer for column '{column_name}'"
            return int(value), True, None
        
        # Handle string representation
        clean_value = value.replace(',', '').replace(' ', '')
        
        # Check if it's a valid integer string
        if clean_value.lstrip('-+').isdigit():
            return int(clean_value), True, None
        
        # Try float conversion first, then check if it's a whole number
        try:
            float_val = float(clean_value)
            if float_val.is_integer():
                return int(float_val), True, None
            else:
                return None, False, f"Value '{value}' is not a whole number for integer column '{column_name}'"
        except ValueError:
            return None, False, f"Value '{value}' cannot be converted to integer for column '{column_name}'"
            
    except Exception as e:
        return None, False, f"Error converting '{value}' to integer for column '{column_name}': {str(e)}"

def validate_decimal(value, precision, scale, column_name):
    """Validate and convert to decimal"""
    try:
        if not value or (isinstance(value, str) and value.isspace()):
            return None, False, f"Empty value for decimal column '{column_name}'"
        
        # Remove whitespace and commas
        clean_value = str(value).replace(',', '').replace(' ', '')
        
        # Try to convert to Decimal
        decimal_val = Decimal(clean_value)
        
        # Check precision and scale if specified
        if precision is not None:
            # Get number of digits
            sign, digits, exponent = decimal_val.as_tuple()
            total_digits = len(digits)
            
            if total_digits > precision:
                return None, False, f"Value '{value}' exceeds precision {precision} for column '{column_name}'"
            
            if scale is not None and abs(exponent) > scale:
                return None, False, f"Value '{value}' exceeds scale {scale} for column '{column_name}'"
        
        return float(decimal_val), True, None
        
    except (InvalidOperation, ValueError) as e:
        return None, False, f"Value '{value}' cannot be converted to decimal for column '{column_name}'"

def validate_float(value, column_name):
    """Validate and convert to float"""
    try:
        if not value or (isinstance(value, str) and value.isspace()):
            return None, False, f"Empty value for float column '{column_name}'"
        
        clean_value = str(value).replace(',', '').replace(' ', '')
        return float(clean_value), True, None
        
    except ValueError:
        return None, False, f"Value '{value}' cannot be converted to float for column '{column_name}'"

def validate_string(value, max_length, column_name):
    """Validate string length"""
    str_value = str(value)
    
    if max_length is not None and len(str_value) > max_length:
        return None, False, f"String '{str_value[:20]}...' exceeds maximum length {max_length} for column '{column_name}'"
    
    return str_value, True, None

def validate_datetime(value, data_type, column_name):
    """Validate and convert datetime"""
    if not value or (isinstance(value, str) and value.isspace()):
        return None, False, f"Empty value for datetime column '{column_name}'"
    
    # Common datetime formats to try
    datetime_formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%d/%m/%Y %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%Y/%m/%d',
        '%Y/%m/%d %H:%M:%S'
    ]
    
    str_value = str(value).strip()
    
    # Try each format
    for fmt in datetime_formats:
        try:
            dt = datetime.strptime(str_value, fmt)
            if data_type == 'date':
                return dt.date(), True, None
            else:
                return dt, True, None
        except ValueError:
            continue
    
    return None, False, f"Value '{value}' is not a valid datetime format for column '{column_name}'"

def validate_boolean(value, column_name):
    """Validate and convert to boolean"""
    if isinstance(value, bool):
        return value, True, None
    
    str_value = str(value).strip().lower()
    
    if str_value in ['true', '1', 'yes', 'y', 'on']:
        return True, True, None
    elif str_value in ['false', '0', 'no', 'n', 'off']:
        return False, True, None
    else:
        return None, False, f"Value '{value}' cannot be converted to boolean for column '{column_name}'"

def validate_batch_data(df, columns_info):
    """
    Validate entire batch of data before insert
    Returns: (validated_df, is_valid, validation_errors)
    """
    validation_errors = []
    validated_data = {}
    
    # Initialize validated data structure
    for col in df.columns:
        validated_data[col] = []
    
    # Validate each row
    for idx, row in df.iterrows():
        row_errors = []
        row_data = {}
        
        # Validate each column in the row
        for col in df.columns:
            if col in columns_info:
                value = row[col]
                column_info = columns_info[col]
                
                converted_value, is_valid, error_msg = validate_and_convert_value(
                    value, column_info, col
                )
                
                if not is_valid:
                    row_errors.append(f"Row {idx + 1}, Column '{col}': {error_msg}")
                else:
                    row_data[col] = converted_value
            else:
                # Column not in database schema
                row_errors.append(f"Row {idx + 1}, Column '{col}': Column not found in database schema")
        
        # If any validation error in this row, record it
        if row_errors:
            validation_errors.extend(row_errors)
        else:
            # Add validated row data
            for col in df.columns:
                validated_data[col].append(row_data.get(col))
    
    # If any validation errors, return failure
    if validation_errors:
        return None, False, validation_errors
    
    # Create validated DataFrame
    validated_df = pd.DataFrame(validated_data)
    return validated_df, True, []

def process_excel_file(file_path, table_name, primary_header=None, sheet_name=None, periode_date=None):
    """
    Memproses file Excel dan insert data ke database dengan validasi header lengkap
    """
    try:
        if not sheet_name:
            return {'success': False, 'message': 'Nama sheet harus dipilih'}
        # Baca file Excel (kode yang sama seperti sebelumnya)
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        except ValueError as e:
            if "Worksheet named" in str(e):
                return {'success': False, 'message': f'Sheet "{sheet_name}" tidak ditemukan dalam file Excel'}
            else:
                return {'success': False, 'message': f'Error membaca sheet: {str(e)}'}
        
        # Validasi apakah sheet kosong
        if df.empty:
            return {'success': False, 'message': f'Sheet "{sheet_name}" kosong atau tidak memiliki data'}
        
        logger.info(f"File Excel berhasil dibaca dengan {len(df)} baris dan {len(df.columns)} kolom")
        
        # PERBAIKAN: Dapatkan kolom yang diperlukan TANPA kolom otomatis
        columns_info = get_column_info(table_name, exclude_automatic=True)
        required_headers = list(columns_info.keys())
        logger.info(f"Header yang diperlukan dari database (tanpa kolom otomatis): {required_headers}")
        
        # Cari baris header dan validasi keberadaan header
        header_row, valid_headers_mapping, missing_headers, detected_primary = find_header_row_and_validate(
            df, required_headers, primary_header
        )
        
        # Cari baris mulai data
        data_start_row = find_data_start_row(df, header_row, detected_primary)
        
        # Ambil header dari baris yang ditemukan
        excel_headers = []
        for val in df.iloc[header_row]:
            excel_headers.append(str(val).strip() if pd.notna(val) else "")
        
        # Ambil data mulai dari baris yang ditemukan
        data_df = df.iloc[data_start_row:].copy()
        
        # Set kolom names
        data_df.columns = range(len(data_df.columns))  # Reset ke numeric index
        
        # Buat mapping index kolom berdasarkan header yang valid
        col_index_mapping = {}
        for excel_header, db_header in valid_headers_mapping:
            for idx, header in enumerate(excel_headers):
                if header == excel_header:
                    col_index_mapping[idx] = db_header
                    break
        
        # Filter dan rename kolom
        filtered_data = {}
        for col_idx, db_header in col_index_mapping.items():
            if col_idx < len(data_df.columns):
                filtered_data[db_header] = data_df.iloc[:, col_idx]
        
        if not filtered_data:
            raise ValueError("Tidak ada data yang dapat diekstrak dari file Excel")
        
        # Buat DataFrame baru dengan kolom yang sudah difilter
        final_df = pd.DataFrame(filtered_data)
        
        # Bersihkan data - hapus baris yang kosong
        final_df = final_df.dropna(how='all')
        
        # Hapus baris yang semua kolomnya kosong atau hanya berisi string kosong
        final_df = final_df[~final_df.astype(str).apply(lambda x: x.str.strip().eq('').all(), axis=1)]
        
        # Reset index
        final_df = final_df.reset_index(drop=True)
        
        logger.info(f"Data yang akan divalidasi: {len(final_df)} baris")
        logger.info(f"Kolom yang akan divalidasi: {list(final_df.columns)}")
        
        if len(final_df) == 0:
            return {
                'success': False,
                'message': 'Tidak ada data valid ditemukan untuk diinsert',
                'header_info': {
                    'header_row': header_row + 1,
                    'data_start_row': data_start_row + 1,
                    'detected_primary': detected_primary,
                    'found_headers': [db_header for _, db_header in valid_headers_mapping],
                    'missing_headers': missing_headers,
                    'sheet_used': sheet_name if sheet_name else 'Sheet pertama'
                }
            }
        
        # **VALIDASI BATCH DATA SEBELUM INSERT**
        # Filter columns_info hanya untuk kolom yang ada di final_df
        relevant_columns_info = {col: columns_info[col] for col in final_df.columns if col in columns_info}
        
        validated_df, is_valid, validation_errors = validate_batch_data(final_df, relevant_columns_info)
        
        if not is_valid:
            logger.error(f"Validasi data gagal: {len(validation_errors)} error ditemukan")
            return {
                'success': False,
                'message': f'Validasi data gagal. Seluruh operasi insert dibatalkan karena ditemukan {len(validation_errors)} error validasi.',
                'validation_errors': validation_errors[:10],  # Tampilkan 10 error pertama
                'total_errors': len(validation_errors),
                'header_info': {
                    'header_row': header_row + 1,
                    'data_start_row': data_start_row + 1,
                    'detected_primary': detected_primary,
                    'found_headers': [db_header for _, db_header in valid_headers_mapping],
                    'missing_headers': missing_headers,
                    'sheet_used': sheet_name if sheet_name else 'Sheet pertama'
                }
            }
        
        logger.info(f"Validasi data berhasil. Data siap untuk insert: {len(validated_df)} baris")
        
        # Insert data ke database
        result = insert_to_database(validated_df, table_name, periode_date, replace_existing=True)
        
        # Tambahkan informasi header yang tidak ditemukan ke result
        result['header_info'] = {
            'periode_date': periode_date,
            'header_row': header_row + 1,
            'data_start_row': data_start_row + 1,
            'detected_primary': detected_primary,
            'found_headers': [db_header for _, db_header in valid_headers_mapping],
            'missing_headers': missing_headers,
            'sheet_used': sheet_name if sheet_name else 'Sheet pertama'
        }
        
        # PERBAIKAN: Hanya tampilkan warning untuk header yang SEHARUSNYA ada di Excel
        # Jangan tampilkan warning untuk kolom otomatis
        if missing_headers:
            result['missing_headers'] = missing_headers
            result['warning'] = f"Header tidak ditemukan di file Excel: {', '.join(missing_headers)}. Kolom ini dilewati saat insert."
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise

def insert_to_database(df, table_name, periode_date=None, replace_existing=True):

    """
    Insert dataframe ke SQL Server table - data sudah tervalidasi
    Includes period_date and upload_date automatic columns
    """
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Kolom yang akan diinsert (sudah difilter dan di-rename)
        insert_columns = list(df.columns)

        # Add automatic columns to the insert list
        insert_columns.extend(['period_date', 'upload_date'])

        if not insert_columns:
            raise ValueError("Tidak ada kolom valid untuk diinsert")

        logger.info(f"Kolom yang akan diinsert: {insert_columns}")

        # Siapkan query INSERT
        placeholders = ', '.join(['?' for _ in insert_columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(f'[{col}]' for col in insert_columns)}) VALUES ({placeholders})"
        
        logger.info(f"Query: {insert_query}")

        # Siapkan data untuk insert - data sudah tervalidasi dan terkonversi
        successful_inserts = 0
        batch_data = []
        current_datetime = datetime.now()

        for idx, row in df.iterrows():
            row_data = []

            # Ambil nilai yang sudah tervalidasi dan terkonversi untuk kolom data
            for col in df.columns:
                value = row[col]
                row_data.append(value)
            
            # Add automatic column values
            row_data.append(periode_date)  # period_date
            row_data.append(current_datetime)  # upload_date
            
            batch_data.append(row_data)

        # Insert semua data sekaligus menggunakan executemany untuk efisiensi
        try:
            if replace_existing and periode_date:
                delete_query = f"DELETE FROM {table_name} WHERE period_date = ?"
                cursor.execute(delete_query, (periode_date,))
                logger.info(f"Data sebelumnya dengan periode {periode_date} telah dihapus dari {table_name}")
                
            cursor.executemany(insert_query, batch_data)
            successful_inserts = len(batch_data)
            conn.commit()
            
            logger.info(f"Berhasil insert {successful_inserts} baris")

            return {
                'success': True,
                'message': f'Berhasil insert {successful_inserts} baris data',
                'inserted_rows': successful_inserts,
                'skipped_rows': 0,
                'error_rows': 0,
                'columns_used': df.columns.tolist(),
                'periode_date': periode_date,
                'upload_date': current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as insert_error:
            conn.rollback()
            logger.error(f"Error saat insert batch: {str(insert_error)}")
            return {
                'success': False,
                'message': f'Error saat insert ke database: {str(insert_error)}',
                'inserted_rows': 0,
                'skipped_rows': 0,
                'error_rows': len(batch_data)
            }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error inserting to database: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
                        
def get_template_tables():
    """
    Mendapatkan semua nama tabel yang mengandung kata 'template' dari database
    Returns: list of table names
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            AND LOWER(TABLE_NAME) LIKE '%template%'
            ORDER BY TABLE_NAME
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
            
def get_master_divisions_tables():
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT division_name 
            FROM ssot_divisions
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

def get_db_connection_with_retry(max_retries=3):
    """Get database connection with retry logic"""
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            # Test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return conn
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(0.1 * (attempt + 1))  # Progressive delay
    return None

def analyze_excel_structure(file_path, sheet_name):
    """
    Menganalisis struktur dan isi data dari sheet Excel tertentu
    
    Args:
        file_path: Path ke file Excel
        sheet_name: Nama sheet yang akan dianalisis
        
    Returns:
        Dictionary berisi informasi analisis data
    """
    try:
        # Baca sheet tertentu
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Informasi dasar
        total_rows = len(df)
        total_cols = len(df.columns)
        
        # Analisis kolom
        columns_info = []
        for col in df.columns:
            col_info = {
                'name': col,
                'type': str(df[col].dtype),
                'non_null_count': df[col].count(),
                'null_count': df[col].isnull().sum(),
                'unique_count': df[col].nunique(),
                'sample_values': df[col].dropna().head(3).tolist() if not df[col].empty else []
            }
            
            # Tambahan info untuk kolom numerik
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    'min_value': df[col].min(),
                    'max_value': df[col].max(),
                    'mean_value': round(df[col].mean(), 2) if not df[col].empty else None
                })
            
            columns_info.append(col_info)
        
        # Sample data (5 baris pertama)
        sample_data = df.head(5).to_dict('records')
        
        # Missing data summary
        missing_data = df.isnull().sum()
        missing_summary = {
            'total_missing': missing_data.sum(),
            'columns_with_missing': missing_data[missing_data > 0].to_dict()
        }
        
        # Duplicate rows
        duplicate_count = df.duplicated().sum()
        
        analysis_result = {
            'sheet_name': sheet_name,
            'basic_info': {
                'total_rows': total_rows,
                'total_columns': total_cols,
                'duplicate_rows': duplicate_count
            },
            'columns_info': columns_info,
            'sample_data': sample_data,
            'missing_data': missing_summary,
            'data_types': df.dtypes.value_counts().to_dict()
        }
        
        logger.info(f"Analisis berhasil untuk sheet '{sheet_name}': {total_rows} baris, {total_cols} kolom")
        return analysis_result
        
    except Exception as e:
        logger.error(f"Error analyzing Excel structure: {str(e)}")
        raise Exception(f"Gagal menganalisis struktur Excel: {str(e)}")

def get_excel_sheets(file_path):
    """
    Mendapatkan daftar nama sheet dalam file Excel
    Args:
        file_path: Path ke file Excel
    Returns:
        List nama sheet atau None jika error
    """
    try:
        # Baca file Excel untuk mendapatkan daftar sheet
        excel_file = pd.ExcelFile(file_path)
        sheets = excel_file.sheet_names
        excel_file.close()
        
        logger.info(f"Ditemukan {len(sheets)} sheet: {sheets}")
        return sheets
        
    except Exception as e:
        logger.error(f"Error reading Excel sheets: {str(e)}")
        return None

def insert_to_ssot_uploader(conn, username, division, template, sheets, file_upload, period_date, upload_date):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO ssot_uploader (username, division, template, sheets, file_upload, period_date, upload_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (username, division, template, sheets, file_upload, period_date, upload_date))
    conn.commit()


# Register route
@app.route('/register', methods=['GET', 'POST'])
def register():
    conn = get_db_connection()
    cur = conn.cursor()

    # Ambil daftar divisi dari tabel divisions
    cur.execute("SELECT division_name FROM ssot_divisions")
    division_rows = cur.fetchall()
    divisions = [row[0] for row in division_rows]

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_confirm = request.form['password_confirm']
        role_access = request.form['role_access']
        fullname = request.form['fullname']
        email = request.form['email']
        division = request.form['division']
        created_date = datetime.now()

        required_fields = ['username', 'password', 'password_confirm', 'role_access', 'fullname', 'email', 'division']
        data = {field: request.form[field] for field in required_fields}

        if not all(data.values()):
            return render_alert("Please fill the empty form!", 'register', username, fullname, email, division, divisions=divisions)

        if password != password_confirm:
            return render_alert("Passwords do not match.", 'register', username, fullname, email, division, divisions=divisions)

        password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

        cur.execute("SELECT * FROM ssot_users WHERE username = ?", (username,))
        existing_user = cur.fetchone()
        if existing_user:
            return render_alert("Username already exists.", 'register', username, fullname, email, role_access, divisions=divisions)

        cur.execute("SELECT * FROM ssot_users WHERE email = ?", (email,))
        existing_email = cur.fetchone()
        if existing_email:
            return render_alert("Email is already registered.", 'register', username, fullname, email, role_access, divisions=divisions)

        cur.execute("""
            INSERT INTO ssot_users (username, password_hash, role_access, fullname, email, division, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (username, password_hash, role_access, fullname, email, division, created_date))
        conn.commit()

        return '''
            <script>
                alert("User registered successfully.");
                window.location.href = "{}";
            </script>
        '''.format(url_for('login'))

    return render_template('register.html', divisions=divisions)
         
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT password_hash, role_access, division, fullname FROM ssot_users WHERE username = ?", (username,))
        user = cur.fetchone()

        if user and bcrypt.check_password_hash(user[0], password):
            session['username'] = username
            session['division'] = user[2]
            session['fullname'] = user[3]
            session['role_access'] = user[1]
            session['upload_done'] = True
            return redirect(url_for('upload_file'))
        
        flash("Invalid username or password.")

    return render_template('login.html')

# Logout Route
@app.route('/logout')
def logout():
    session.pop('username', None)
    flash("You have been logged out.")
    return redirect(url_for('login'))

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if 'username' not in session:
        flash("Please log in first.")
        return redirect(url_for('login'))

    role_access = session.get('role_access')
    fullname = session.get('fullname')
    username = session.get('username')
    division = session.get('division')
    
    if request.method == 'GET':
        # Mendapatkan daftar tabel template untuk dropdown
        template_tables = get_template_tables()
        
        # Tampilkan halaman upload dengan data tabel template
        return render_template(
            'upload.html',
            username=username,
            division=division,
            role_access=role_access,
            fullname=fullname,
            template_tables=template_tables
        )
    
    elif request.method == 'POST':
        # Kode POST method tetap sama seperti sebelumnya
        try:
            if 'file' not in request.files:
                return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
            
            file = request.files['file']
            table_name = request.form.get('table_name', '').strip()
            primary_header = request.form.get('primary_header', '').strip() or None
            sheet_name = request.form.get('sheet_name', '').strip() or None
            periode_date = request.form.get('periode_date', '').strip() or None
            
            if file.filename == '':
                return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
            
            if not table_name:
                return jsonify({'success': False, 'message': 'Nama tabel harus dipilih'})
            
            if not allowed_file(file.filename):
                return jsonify({'success': False, 'message': 'File harus berformat Excel (.xlsx atau .xls)'})
            
            # Validasi format tanggal periode
            if periode_date:
                try:
                    periode_date = datetime.strptime(periode_date, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({'success': False, 'message': 'Format tanggal periode tidak valid. Gunakan format YYYY-MM-DD'})
            
            # Buat folder uploads ada
            upload_folder = app.config['UPLOAD_FOLDER']
            os.makedirs(upload_folder, exist_ok=True)
            
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{filename}"
            file_path = os.path.join(upload_folder, filename)
            file.save(file_path)
            
            # Proses file
            result = process_excel_file(file_path, table_name, primary_header, sheet_name, periode_date)
            
            # Simpan ke tabel ssot_uploader
            try:
                conn = get_db_connection()  # pastikan Anda punya fungsi koneksi ini
                insert_to_ssot_uploader(
                    conn=conn,
                    username=session.get('username'),
                    division=session.get('division'),
                    template=table_name,
                    sheets=sheet_name,
                    file_upload=file_path,
                    period_date=periode_date,
                    upload_date=datetime.now()
                )
                conn.close()
            except Exception as e:
                logger.error(f"Gagal insert ke ssot_uploader: {str(e)}")
            
            return jsonify(result)
        
        except Exception as e:
            logger.error(f"Error in upload_file: {str(e)}")
            return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    
    return redirect(url_for('upload_file'))
                
@app.route('/preview-headers/<table_name>')
def preview_headers(table_name):
    """
    Preview header database dengan menandai kolom otomatis
    """
    try:
        # Dapatkan semua kolom termasuk otomatis
        all_columns = get_column_info(table_name, exclude_automatic=False)
        
        # Dapatkan kolom yang diharapkan dari Excel (tanpa otomatis)
        excel_columns = get_column_info(table_name, exclude_automatic=True)
        
        # Tandai kolom otomatis
        automatic_columns = get_automatic_columns()
        
        headers_with_info = {}
        for col_name, col_info in all_columns.items():
            headers_with_info[col_name] = {
                **col_info,
                'is_automatic': col_name.lower() in [ac.lower() for ac in automatic_columns],
                'required_in_excel': col_name in excel_columns
            }
        
        return jsonify({
            'success': True,
            'headers': headers_with_info,
            'table_name': table_name,
            'total_columns': len(all_columns),
            'excel_required_columns': len(excel_columns),
            'automatic_columns': len(automatic_columns)
        })
        
    except Exception as e:
        logger.error(f"Error getting table headers: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        })

@app.route('/analyze-excel', methods=['POST'])
def analyze_excel():
    """Analyze Excel file structure without inserting to database"""
    
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        file = request.files['file']
        sheet_name = request.form.get('sheet_name', '').strip() or None
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'message': 'File harus berformat Excel (.xlsx atau .xls)'})
        
        # Simpan file temporary
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"analyze_{timestamp}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # Dapatkan daftar sheet terlebih dahulu
            available_sheets = get_excel_sheets(file_path)
            
            # Baca file Excel dengan sheet yang spesifik atau default
            if sheet_name:
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    sheet_used = sheet_name
                except ValueError:
                    return jsonify({'success': False, 'message': f'Sheet "{sheet_name}" tidak ditemukan dalam file Excel'})
            else:
                df = pd.read_excel(file_path, header=None)
                sheet_used = available_sheets[0] if available_sheets else 'Sheet pertama'
            
            # Auto-detect header
            header_row, detected_primary = find_primary_header_row(df)
            
            # Get headers
            excel_headers = []
            for val in df.iloc[header_row]:
                if pd.notna(val):
                    excel_headers.append(str(val).strip())
                else:
                    excel_headers.append("")
            
            # Find data start row
            data_start_row = find_data_start_row(df, header_row, detected_primary)
            
            # Sample data
            sample_data = []
            for i in range(data_start_row, min(data_start_row + 3, len(df))):
                row_sample = []
                for j in range(min(len(excel_headers), len(df.columns))):
                    val = df.iloc[i, j]
                    row_sample.append(str(val) if pd.notna(val) else "")
                sample_data.append(row_sample)
            
            result = {
                'success': True,
                'analysis': {
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'header_row': header_row + 1,
                    'data_start_row': data_start_row + 1,
                    'detected_primary_header': detected_primary,
                    'headers': excel_headers,
                    'sample_data': sample_data,
                    'data_rows_available': len(df) - data_start_row,
                    'available_sheets': available_sheets,
                    'sheet_used': sheet_used
                }
            }
            
        finally:
            # Hapus file temporary
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in analyze_excel: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@app.route('/get-template-tables')
def get_template_tables_endpoint():
    """Endpoint untuk mendapatkan daftar tabel template"""
    try:
        tables = get_template_tables()
        return jsonify({
            'success': True, 
            'tables': tables,
            'message': f'Ditemukan {len(tables)} tabel template'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

# CRUD Template Tables
@app.route('/create-table', methods=['GET', 'POST'])
def create_table():
    if 'username' not in session:
        flash("Please log in first.")
        return redirect(url_for('login'))

    role_access = session.get('role_access')
    fullname = session.get('fullname')
    username = session.get('username')
    division = session.get('division')
    divisions = get_master_divisions_tables()
    
    if request.method == 'GET':
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
            
            
            # Validate table name (alphanumeric and underscore only)
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
                return jsonify({'success': False, 'message': 'Nama tabel hanya boleh mengandung huruf, angka, dan underscore. Harus dimulai dengan huruf.'})
            
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
                
                if not col_name:
                    return jsonify({'success': False, 'message': 'Semua kolom harus memiliki nama'})
                
                # Validate column name
                if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', col_name):
                    return jsonify({'success': False, 'message': f'Nama kolom "{col_name}" tidak valid. Hanya boleh huruf, angka, dan underscore.'})
                
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
                
                column_definitions.append(col_def)
            
            # Add automatic columns
            column_definitions.append("    [period_date] DATE NULL")
            column_definitions.append("    [upload_date] DATETIME NOT NULL DEFAULT GETDATE()")
            
            create_query += ",\n".join(column_definitions)
            create_query += "\n)"
            
            logger.info(f"Creating table with query: {create_query}")
            
            # Execute dan verify dengan handling yang lebih baik
            try:
                
                cursor.execute("""
                    INSERT INTO ssot_creator (template_name, division_name, created_date, created_by)
                    VALUES (?, ?, GETDATE(), ?)
                """, (table_name, divisions, username))
                
                cursor.execute(create_query)
                
                # Tunggu sebentar dan cek apakah tabel benar-benar dibuat
                import time
                time.sleep(0.5)  # Tunggu sebentar untuk memastikan operasi selesai
                
                # Verify table was created successfully
                cursor.execute("""
                    SELECT COUNT(*) FROM sys.tables 
                    WHERE name = ? AND type = 'U'
                """, (table_name,))
                
                table_count = cursor.fetchone()[0]
                
                if table_count == 0:
                    # Rollback jika tabel tidak terbuat
                    conn.rollback()
                    logger.error(f"Table {table_name} was not created successfully")
                    return jsonify({'success': False, 'message': 'Tabel gagal dibuat. Silakan periksa log database.'})
                
                # Commit hanya jika verifikasi berhasil
                conn.commit()
                logger.info(f"Table {table_name} created and verified successfully")
                
                return jsonify({
                    'success': True,
                    'message': f'Tabel "{table_name}" berhasil dibuat dengan {len(columns)} kolom + 2 kolom otomatis (period_date, upload_date)',
                    'table_name': table_name,
                    'columns_created': len(columns) + 2,  # +2 for automatic columns
                    'automatic_columns': ['period_date', 'upload_date'],
                    'query': create_query
                })
                
            except Exception as create_error:
                conn.rollback()
                logger.error(f"Error creating table {table_name}: {str(create_error)}")
                
                # Handle specific SQL Server errors
                error_msg = str(create_error)
                if "42S01" in error_msg or "2714" in error_msg:
                    return jsonify({'success': False, 'message': f'Tabel "{table_name}" sudah ada dalam database'})
                elif "2705" in error_msg:
                    return jsonify({'success': False, 'message': 'Nama kolom duplikat atau tidak valid'})
                elif "102" in error_msg:
                    return jsonify({'success': False, 'message': 'Syntax error dalam query SQL'})
                else:
                    return jsonify({'success': False, 'message': f'Error database: {error_msg}'})
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            logger.error(f"Error creating table: {str(e)}")
            return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
@app.route('/update-table', methods=['POST'])
def update_table():
    """Update an existing table structure"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
    conn = None
    cursor = None
    try:
        data = request.get_json()
        original_table_name = data.get('original_table_name', '').strip()
        new_table_name = data.get('table_name', '').strip()
        columns = data.get('columns', [])
        
        if not original_table_name or not new_table_name:
            return jsonify({'success': False, 'message': 'Table name is required'})
        
        if not columns:
            return jsonify({'success': False, 'message': 'At least one column is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        # Check if original table exists
        cursor.execute("""
            SELECT COUNT(*) FROM sys.tables 
            WHERE name = ? AND type = 'U'
        """, (original_table_name,))
        
        if cursor.fetchone()[0] == 0:
            return jsonify({'success': False, 'message': f'Table "{original_table_name}" not found'})
        
        # If table name changed, check if new name already exists
        if original_table_name != new_table_name:
            cursor.execute("""
                SELECT COUNT(*) FROM sys.tables 
                WHERE name = ? AND type = 'U'
            """, (new_table_name,))
            
            if cursor.fetchone()[0] > 0:
                return jsonify({'success': False, 'message': f'Table "{new_table_name}" already exists'})
        
        # For simplicity, we'll create a new table with the updated structure
        # and copy data from the old table (if structure allows)
        # This is a simplified approach - in production, you might want more sophisticated ALTER TABLE operations
        
        # Create new table with updated structure
        create_query = f"CREATE TABLE [{new_table_name}_new] (\n"
        create_query += "    [id] INT IDENTITY(1,1) PRIMARY KEY,\n"
        
        column_definitions = []
        for col in columns:
            col_name = col.get('name', '').strip()
            col_type = col.get('type', 'VARCHAR')
            col_length = col.get('length', '')
            allow_nulls = col.get('nullable', False)
            
            if not col_name:
                return jsonify({'success': False, 'message': 'All columns must have a name'})
            
            # Validate column name
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', col_name):
                return jsonify({'success': False, 'message': f'Column name "{col_name}" is invalid'})
            
            # Build column definition
            col_def = f"    [{col_name}] {col_type}"
            
            # Add length for applicable types
            if col_type in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR'] and col_length:
                try:
                    length_val = int(col_length)
                    if length_val <= 0 or length_val > 8000:
                        return jsonify({'success': False, 'message': f'Column "{col_name}" length must be between 1-8000'})
                    col_def += f"({length_val})"
                except ValueError:
                    return jsonify({'success': False, 'message': f'Column "{col_name}" length must be numeric'})
            elif col_type in ['DECIMAL', 'NUMERIC'] and col_length:
                if ',' in col_length:
                    try:
                        precision, scale = col_length.split(',')
                        precision = int(precision.strip())
                        scale = int(scale.strip())
                        if precision < 1 or precision > 38 or scale < 0 or scale > precision:
                            return jsonify({'success': False, 'message': f'Invalid precision/scale for column "{col_name}"'})
                        col_def += f"({precision},{scale})"
                    except ValueError:
                        return jsonify({'success': False, 'message': f'Invalid precision,scale format for column "{col_name}"'})
                else:
                    try:
                        precision = int(col_length)
                        if precision < 1 or precision > 38:
                            return jsonify({'success': False, 'message': f'Invalid precision for column "{col_name}"'})
                        col_def += f"({precision})"
                    except ValueError:
                        return jsonify({'success': False, 'message': f'Precision must be numeric for column "{col_name}"'})
            
            col_def += " NULL" if allow_nulls else " NOT NULL"
            column_definitions.append(col_def)
        
        # Tambahkan kembali kolom sistem meskipun tidak dikirim dari frontend
        existing_col_names = [col.get('name', '').lower() for col in columns]

        if 'period_date' not in existing_col_names:
            column_definitions.append("    [period_date] DATE NULL")
        if 'upload_date' not in existing_col_names:
            column_definitions.append("    [upload_date] DATETIME NOT NULL DEFAULT GETDATE()")

        
        create_query += ",\n".join(column_definitions)
        create_query += "\n)"
        
        # Execute create new table
        cursor.execute(create_query)
        
        # Ambil nama kolom yang sama antara old dan new table
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (original_table_name,))
        old_columns = set([row[0].lower() for row in cursor.fetchall()])
        new_columns = set([line.split()[0].strip("[]").lower() for line in column_definitions])

        common_columns = list(old_columns & new_columns)
        if common_columns:
            common_columns_str = ', '.join(f"[{col}]" for col in common_columns)
            insert_query = f"""
                INSERT INTO [{new_table_name}_new] ({common_columns_str})
                SELECT {common_columns_str} FROM [{original_table_name}]
            """
            cursor.execute(insert_query)
            logger.info(f"{cursor.rowcount} row(s) copied from {original_table_name} to {new_table_name}_new")
        else:
            logger.warning("No common columns between old and new table, data not copied.")

        
        # Drop old table and rename new one
        cursor.execute(f"DROP TABLE [{original_table_name}]")
        cursor.execute(f"EXEC sp_rename '[{new_table_name}_new]', '{new_table_name}'")
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': f'Table "{new_table_name}" updated successfully',
            'table_name': new_table_name
        })
        
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"Error updating table: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error updating table: {str(e)}'
        })
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/delete-table', methods=['POST'])
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
                SELECT COUNT(*) FROM ssot_creator 
                WHERE template_name = ?
            """, (table_name,))
            
            is_template = cursor.fetchone()[0] > 0
            
            if is_template:
                # Delete from template table
                cursor.execute("""
                    DELETE FROM ssot_creator 
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
                    logger.info(f"Dropped table {table_name}")
                    return jsonify({
                        'success': True, 
                        'message': f'Table "{table_name}" berhasil dihapus'
                    })
                else:
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
                
# CRUD Divisions Management - Simplified Backend Code (Create & Delete Only)
@app.route('/divisions-page')
def divisions_page():
    if 'username' not in session:
        flash('Please log in first.')
        return redirect(url_for('login'))

    return render_template(
        'divisions_management.html',
        username=session.get('username'),
        fullname=session.get('fullname'),
        division=session.get('division'),
        role_access=session.get('role_access')
    )

@app.route('/divisions', methods=['GET'])
def get_divisions():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, division_name, created_by, 
                   CONVERT(VARCHAR(19), created_date, 120) as created_date
            FROM ssot_divisions
            ORDER BY created_date DESC
        """)
        
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        divisions = [dict(zip(columns, row)) for row in rows]

        return jsonify({'success': True, 'divisions': divisions})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/divisions', methods=['POST'])
def create_division():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})

    try:
        data = request.get_json()
        division_name = data.get('division_name', '').strip()
        created_by = session.get('username')

        if not division_name:
            return jsonify({'success': False, 'message': 'Division name is required'})

        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if division name already exists
        cursor.execute("""
            SELECT COUNT(*) FROM ssot_divisions 
            WHERE LOWER(division_name) = LOWER(?)
        """, (division_name,))
        
        if cursor.fetchone()[0] > 0:
            return jsonify({'success': False, 'message': 'Division name already exists'})

        # Insert new division
        cursor.execute("""
            INSERT INTO ssot_divisions (division_name, created_by, created_date)
            VALUES (?, ?, GETDATE())
        """, (division_name, created_by))
        
        conn.commit()
        return jsonify({'success': True, 'message': f'Division "{division_name}" created successfully'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/divisions/<int:division_id>', methods=['DELETE'])
def delete_division(division_id):
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get division name before deletion
        cursor.execute("SELECT division_name FROM ssot_divisions WHERE id = ?", (division_id,))
        result = cursor.fetchone()
        
        if not result:
            return jsonify({'success': False, 'message': 'Division not found'})
        
        division_name = result[0]
        
        # Delete the division
        cursor.execute("DELETE FROM ssot_divisions WHERE id = ?", (division_id,))
        conn.commit()
        
        return jsonify({'success': True, 'message': f'Division "{division_name}" deleted successfully'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
   
@app.route('/check-period', methods=['POST'])
def check_period():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    data = request.json
    table_name = data.get('table_name')
    periode_date = data.get('periode_date')

    if not table_name or not periode_date:
        return jsonify({'success': False, 'message': 'Nama tabel dan periode_date wajib diisi.'})

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name} WHERE period_date = ?"
        cursor.execute(query, (periode_date,))
        count = cursor.fetchone()[0]
        return jsonify({'success': True, 'exists': count > 0})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error checking period: {str(e)}'})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# PERBAIKAN 2: Endpoint check table yang lebih reliable
@app.route('/check-table-exists/<table_name>')
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
        
        return jsonify({
            'success': True,
            'exists': final_exists,
            'message': f'Tabel "{table_name}" {"sudah ada" if final_exists else "belum ada"} dalam database'
        })
        
    except Exception as e:
        logger.error(f"Error checking table existence: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get-existing-tables')
def get_existing_tables():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not authenticated'})

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Ambil hanya data template dari ssot_creator
        cursor.execute("""
            SELECT 
                template_name,
                division_name,
                create_date,
                create_by
            FROM ssot_creator
            ORDER BY create_date DESC
        """)
        templates = cursor.fetchall()

        # Bangun response
        tables = []
        for template in templates:
            tables.append({
                'name': template[0],
                'division': template[1],
                'create_date': template[2].strftime('%Y-%m-%d %H:%M:%S') if template[2] else 'N/A',
                'create_by': template[3]
            })

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'tables': tables})

    except Exception as e:
        logger.error(f"Error getting existing tables: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@app.route('/get-table-details/<table_name>', methods=['GET'])
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
        
        return jsonify({
            'success': True,
            'table': {
                'name': table_name,
                'columns': columns
            }
        })
        
    except Exception as e:
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

@app.route('/get-table-data/<table_name>', methods=['GET'])
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
                                    
@app.route('/get-sql-data-types')
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

@app.route('/get-excel-sheets', methods=['POST'])
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
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # Dapatkan daftar sheet
            sheets = get_excel_sheets(file_path)
            
            if sheets is None:
                return jsonify({'success': False, 'message': 'Gagal membaca daftar sheet dari file Excel'})
            
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
        logger.error(f"Error in get_excel_sheets: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@app.route('/export-table/<table_name>', methods=['GET'])
def export_table(table_name):
    """Export table data to CSV"""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    
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
        
        return response
        
    except Exception as e:
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

@app.route('/duplicate-table/<table_name>', methods=['POST'])
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

@app.route('/save-as-template', methods=['POST'])
def save_as_template():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not authenticated'})
    
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
                SELECT COUNT(*) FROM ssot_creator 
                WHERE template_name = ?
            """, (table_name,))
            
            if cursor.fetchone()[0] > 0:
                return jsonify({'success': False, 'message': 'Template already exists'})
            
            # Save template metadata
            cursor.execute("""
                INSERT INTO ssot_creator (template_name, division_name, created_date, created_by)
                VALUES (?, ?, GETDATE(), ?)
            """, (table_name, division, username))
            
            # You might want to save column definitions in a separate table
            # For now, just save the template record
            
            conn.commit()
            logger.info(f"Template {table_name} saved successfully")
            
            return jsonify({
                'success': True,
                'message': f'Template "{table_name}" saved successfully'
            })
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving template {table_name}: {str(e)}")
            return jsonify({'success': False, 'message': f'Error saving template: {str(e)}'})
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in save_as_template: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
                        
if __name__ == '__main__':
    app.run(debug=True)