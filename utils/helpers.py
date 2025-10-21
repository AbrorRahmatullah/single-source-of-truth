from asyncio.log import logger
from datetime import datetime

import pandas as pd

def normalize_value(value, dtype=None):
    """
    Normalisasi nilai untuk insert ke SQL Server:
    - 'N/A', '', 'None', 'null', NaN => None (untuk kolom non-numeric)
    - '-' => 0 jika numeric, else None
    - Membersihkan tanda kurung & kutip jika ada
    PERBAIKAN: Handle database default marker
    """
    # PERBAIKAN: Handle database default marker - return as-is tanpa processing
    if str(value) == '__USE_DATABASE_DEFAULT__':
        return value
    
    if pd.isna(value):
        return 0 if dtype and ('int' in dtype or 'float' in dtype) else None

    cleaned = str(value).strip()
    cleaned_simple = cleaned.strip("'\"() ").upper()

    null_equivalents = {'N/A', '', 'NONE', 'NULL'}

    if cleaned_simple in null_equivalents:
        return 0 if dtype and ('int' in dtype or 'float' in dtype) else None

    if cleaned == '-':
        return 0 if dtype and ('int' in dtype or 'float' in dtype) else None

    return value

def parse_period_date(period_str):
    return datetime.strptime(period_str, '%Y-%m').date().replace(day=1)

def process_default_value(default_value, column_info=None):
    """
    Hybrid version of process_default_value:
    - Cepat seperti versi 'new'
    - Lengkap dan tipe-data-aware seperti versi 'old'
    - Aman terhadap SQL Server default expression ((1)), (GETDATE()), dll.
    """

    if default_value is None:
        return None

    col_type = (column_info.get('data_type', '') if column_info else '').upper()
    col_name = (column_info.get('name', '') if column_info else '')

    ds = str(default_value).strip()

    # --- 1️⃣ Tangani pola constraint SQL Server ((...)) ---
    if ds.startswith('((') and ds.endswith('))'):
        inner = ds[2:-2].strip()
        if inner.upper() in ['GETDATE', 'GETUTCDATE', 'SYSDATETIME', 'CURRENT_TIMESTAMP']:
            return '__USE_DATABASE_DEFAULT__'
        try:
            return float(inner) if '.' in inner else int(inner)
        except Exception:
            return inner

    # --- 2️⃣ Tangani pola umum (...) ---
    if ds.startswith('(') and ds.endswith(')'):
        inner = ds[1:-1].strip()
        inner_upper = inner.upper()
        # Jika function waktu SQL Server → gunakan database default
        if inner_upper in ['GETDATE', 'GETUTCDATE', 'SYSDATETIME', 'CURRENT_TIMESTAMP']:
            return '__USE_DATABASE_DEFAULT__'
        # Jika nilai diapit kutip → hapus kutip
        if inner.startswith("'") and inner.endswith("'"):
            inner = inner[1:-1]
        ds = inner  # Update string utama

    # --- 3️⃣ Jika default adalah SQL function ---
    if ds.upper() in ['GETDATE()', 'GETUTCDATE()', 'SYSDATETIME()', 'CURRENT_TIMESTAMP']:
        return '__USE_DATABASE_DEFAULT__'

    # --- 4️⃣ Konversi berdasarkan tipe kolom ---
    try:
        if col_type in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT']:
            if ds.startswith("'") and ds.endswith("'"):
                ds = ds[1:-1]
            return str(ds)

        elif col_type == 'BIT':
            if ds.lower() in ['1', 'true']:
                return True
            elif ds.lower() in ['0', 'false']:
                return False
            else:
                return bool(int(ds))

        elif col_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT']:
            return int(float(ds))

        elif col_type in ['DECIMAL', 'NUMERIC', 'FLOAT', 'REAL']:
            return float(ds)

        elif col_type in ['DATE', 'DATETIME', 'DATETIME2']:
            # Jika SQL function → gunakan default DB
            if ds.upper() in ['GETDATE()', 'GETUTCDATE()', 'SYSDATETIME()', 'CURRENT_TIMESTAMP']:
                return '__USE_DATABASE_DEFAULT__'
            # Coba parse tanggal
            date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%d/%m/%Y']
            for fmt in date_formats:
                try:
                    return datetime.strptime(ds, fmt)
                except ValueError:
                    continue
            return '__USE_DATABASE_DEFAULT__'

        elif col_type == 'TIME':
            time_formats = ['%H:%M:%S', '%H:%M']
            for fmt in time_formats:
                try:
                    return datetime.strptime(ds, fmt).time()
                except ValueError:
                    continue
            return '__USE_DATABASE_DEFAULT__'

        else:
            # Jika tidak diketahui tipe kolom, deteksi otomatis seperti versi 'new'
            if ds.startswith("'") and ds.endswith("'"):
                return ds[1:-1]
            if ds.isdigit():
                return int(ds)
            try:
                return float(ds)
            except Exception:
                return ds

    except Exception as e:
        logger.warning(f"Error processing default value '{default_value}' for column '{col_name}': {e}")
        return '__USE_DATABASE_DEFAULT__'
    
def handle_null_values_for_column(value, column_info):
    """
    Handle NULL values based on column configuration
    Improved version with better type handling and default value processing
    """
    # Check if value is considered "null" in various formats
    null_indicators = [None, '', 'NULL', 'null', 'Null', 'N/A', 'n/a', 'NA', 'na', '#N/A']
    
    # Handle pandas NaN and empty strings more thoroughly
    is_null = (value in null_indicators or 
               (isinstance(value, float) and pd.isna(value)) or
               (isinstance(value, str) and value.strip() in [''] + null_indicators) or
               value is None)
    
    if is_null:
        if column_info.get('is_nullable', False):
            return None  # Return None untuk NULL database value
        else:
            default_value = column_info.get('default_value')
            if default_value is not None and str(default_value).strip() != '':
                # INI SATU-SATUNYA TEMPAT PEMANGGILAN process_default_value
                return process_default_value(default_value, column_info)
            else:
                col_name = column_info.get('name', 'Unknown')
                raise ValueError(f"Column '{col_name}' cannot be NULL and has no default value")
    
    # If not null, return the value as is (will be processed later based on column type)
    return value
