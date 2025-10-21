import os
import re
import math
import logging
from datetime import datetime, date
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd

from utils.db_utils import get_column_info, insert_to_database
from utils.helpers import handle_null_values_for_column, process_default_value

logger = logging.getLogger(__name__)

def normalize_column_name(col_name: Any) -> str:
    if pd.isna(col_name):
        return ''
    return str(col_name).strip()
    
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

def find_primary_header_row(df, primary_header_pattern=None):
    def normalize_text(text):
        if pd.isna(text):
            return ""
        return str(text).lower().strip()

    def is_likely_header_row(row_values):
        non_empty_count = 0
        text_count = 0
        numeric_count = 0

        for val in row_values:
            if pd.notna(val) and str(val).strip():
                non_empty_count += 1
                try:
                    float(str(val))
                    numeric_count += 1
                except ValueError:
                    text_count += 1

        if non_empty_count < 2:
            return False

        if numeric_count == non_empty_count:
            return False

        return True

    def check_column_pattern(row_values):
        text_values = [normalize_text(val) for val in row_values if pd.notna(val) and str(val).strip()]
        column_pattern_count = sum(1 for val in text_values if val.startswith('column_'))
        if column_pattern_count >= 2:
            return 10

        header_indicators = ['col', 'field', 'data', 'info', 'value', 'item']
        score = 0
        for val in text_values:
            for indicator in header_indicators:
                if indicator in val:
                    score += 1
        return score

    # Step 1: jika diberikan primary_header_pattern
    if primary_header_pattern:
        pattern_normalized = normalize_text(primary_header_pattern)
        for idx, row in df.iterrows():
            row_values = [normalize_text(val) for val in row]
            for val in row_values:
                if pattern_normalized in val or val in pattern_normalized:
                    if is_likely_header_row(row.values):
                        logger.info(f"Header row ditemukan di baris {idx + 1} berdasarkan pattern '{primary_header_pattern}'")
                        return idx, primary_header_pattern

    # Step 2: auto-detection
    best_header_row = None
    best_score = -1
    detected_primary = None

    common_header_keywords = [
        'number', 'name', 'id', 'code', 'facility', 'location', 'type', 
        'date', 'status', 'description', 'value', 'amount', 'quantity',
        'column', 'field', 'data', 'info'
    ]

    for idx, row in df.iterrows():
        if idx > 20:
            break

        if not is_likely_header_row(row.values):
            continue

        score = 0
        primary_candidate = None
        row_values = [str(val) for val in row if pd.notna(val)]

        # Penambahan: beri preferensi lebih tinggi jika ini baris pertama
        if idx == 0:
            score += 5  # penalti negatif untuk baris bukan pertama

        # Skor berdasarkan keyword
        for val in row_values:
            val_norm = normalize_text(val)
            for keyword in common_header_keywords:
                if keyword in val_norm:
                    score += 1
                    if not primary_candidate and (
                        'number' in val_norm or 'id' in val_norm or 'column' in val_norm
                    ):
                        primary_candidate = str(val).strip()

        # Skor berdasarkan pola column_
        score += check_column_pattern(row.values)

        # Tambahan bonus untuk jumlah kolom
        non_empty_count = sum(1 for val in row_values if str(val).strip())
        if non_empty_count >= 3:
            score += 2

        if score > best_score:
            best_score = score
            best_header_row = idx
            detected_primary = primary_candidate or str(row_values[0]).strip()

    if best_header_row is not None:
        logger.info(f"Header row auto-detected di baris {best_header_row + 1}, primary header: '{detected_primary}', score: {best_score}")
        return best_header_row, detected_primary

    # Fallback
    logger.warning("Menggunakan fallback detection...")
    for idx, row in df.iterrows():
        if idx > 10:
            break
        if is_likely_header_row(row.values):
            row_values = [str(val) for val in row if pd.notna(val)]
            if row_values:
                detected_primary = str(row_values[0]).strip()
                logger.info(f"Header row fallback detected di baris {idx + 1}, primary header: '{detected_primary}'")
                return idx, detected_primary

    raise ValueError("Tidak dapat menemukan baris header. Pastikan file Excel memiliki baris header yang jelas.")

def _convert_spaces_to_underscore(s: str) -> str:
    return s.replace(' ', '_')

def strict_column_match(excel_headers: List[str], required_headers: List[str]) -> Tuple[List[Tuple[str, str]], List[str]]:
    non_empty_excel = [h for h in excel_headers if h and h.strip()]
    if len(non_empty_excel) != len(required_headers):
        logger.info('Excel column count (%s) != DB required (%s)', len(non_empty_excel), len(required_headers))

    found_db = set()
    mapping = []
    req_lower_map = {rh.lower(): rh for rh in required_headers}

    for ex in non_empty_excel:
        nl = ex.strip().lower()
        matched = req_lower_map.get(nl)
        if matched and matched not in found_db:
            mapping.append((ex, matched))
            found_db.add(matched)
        else:
            mapping.append((ex, None))

    remaining_db = [h for h in required_headers if h not in found_db]
    if any(m[1] is None for m in mapping):
        new_mapping = []
        for ex, dbm in mapping:
            if dbm is None:
                conv = _convert_spaces_to_underscore(ex).lower()
                matched = None
                for db in remaining_db:
                    if db.lower() == conv:
                        matched = db
                        break
                if matched:
                    new_mapping.append((ex, matched))
                    found_db.add(matched)
                    remaining_db.remove(matched)
                else:
                    new_mapping.append((ex, None))
            else:
                new_mapping.append((ex, dbm))
        mapping = new_mapping

    if any(m[1] is None for m in mapping):
        remaining_db = [h for h in required_headers if h not in found_db]
        for i, (ex, dbm) in enumerate(mapping):
            if dbm is None:
                ex_clean = re.sub(r'[\s_\-]+', '', ex).lower()
                if len(ex_clean) >= 3:
                    for db in remaining_db:
                        db_clean = re.sub(r'[\s_\-]+', '', db).lower()
                        if ex_clean == db_clean or ex_clean in db_clean or db_clean in ex_clean:
                            mapping[i] = (ex, db)
                            found_db.add(db)
                            remaining_db.remove(db)
                            break

    final_unmatched_excel = [ex for ex, dbm in mapping if dbm is None]
    final_unmatched_db = [db for db in required_headers if db not in found_db]

    if final_unmatched_db:
        msg = 'Nama kolom tidak sesuai dengan template database. '
        msg += 'Kolom database yang tidak ditemukan: ' + ', '.join(final_unmatched_db) + '. '
        msg += 'Spasi akan otomatis dikonversi menjadi underscore. Pastikan header case-insensitive cocok.'
        raise ValueError(msg)

    return [(ex, dbm) for ex, dbm in mapping], []

def find_header_row_and_validate(df: pd.DataFrame,
                                 required_headers: List[str],
                                 primary_header_pattern: Optional[str] = None
                                 ) -> Tuple[int, List[Tuple[str, str]], List[str], str]:
    header_row, detected_primary = None, ''
    if primary_header_pattern:
        try:
            hr, prim = find_primary_header_row(df, primary_header_pattern)
            header_row, detected_primary = hr, prim
        except Exception:
            header_row = None

    if header_row is None:
        header_row, detected_primary = find_primary_header_row(df)

    excel_headers = [normalize_column_name(v) for v in df.iloc[header_row]]
    while excel_headers and excel_headers[-1] == '':
        excel_headers.pop()

    try:
        valid_mapping, missing = strict_column_match(excel_headers, required_headers)
        logger.info('Header validated at row %s. Matched %s columns.', header_row + 1, len(valid_mapping))
        return header_row, valid_mapping, missing, detected_primary
    except ValueError as e:
        logger.error('Header validation failed: %s', e)
        raise

def find_data_start_row(df: pd.DataFrame, header_row: int, detected_primary_header: str) -> int:
    excel_headers = [str(v).strip() if pd.notna(v) else '' for v in df.iloc[header_row]]
    primary_col_index = None
    primary_norm = detected_primary_header.strip().lower() if detected_primary_header else ''

    for idx, header in enumerate(excel_headers):
        if not header:
            continue
        hnorm = header.lower().strip()
        if primary_norm and (hnorm == primary_norm or primary_norm in hnorm or hnorm in primary_norm):
            primary_col_index = idx
            break

    if primary_col_index is None:
        for idx, header in enumerate(excel_headers):
            if header:
                primary_col_index = idx
                logger.warning("Primary header not found, using first non-empty header '%s'", header)
                break

    if primary_col_index is None:
        raise ValueError('Tidak dapat menentukan kolom primary untuk mencari data')

    data_start_row = None
    for ridx in range(header_row + 1, len(df)):
        try:
            cell_val = df.iloc[ridx, primary_col_index]
        except Exception:
            cell_val = None
        if pd.notna(cell_val) and str(cell_val).strip().lower() not in ['nan', 'none', '', 'null']:
            row_data = [str(v).strip() for v in df.iloc[ridx] if pd.notna(v) and str(v).strip() != '']
            if len(row_data) >= 2:
                data_start_row = ridx
                break

    if data_start_row is None:
        raise ValueError(f"Tidak ditemukan data setelah header pada kolom '{detected_primary_header}'")

    logger.info('Data start row determined at %s', data_start_row + 1)
    return data_start_row

def validate_and_convert_value(value: Any, column_info: Dict[str, Any], column_name: str) -> Tuple[Any, bool, str]:
    try:
        processed = handle_null_values_for_column(value, column_info)
        if processed == '__USE_DATABASE_DEFAULT__':
            return processed, True, ''
        if processed is None:
            return None, True, ''
        col_type = column_info.get('data_type', '').upper()
        if col_type in ('VARCHAR','NVARCHAR','CHAR','NCHAR','TEXT'):
            s = str(processed).strip()
            max_len = column_info.get('max_length')
            if max_len and isinstance(max_len, int) and len(s) > max_len:
                return None, False, f"String length ({len(s)}) exceeds max {max_len}"
            return s, True, ''
        if col_type == 'BIT':
            if isinstance(processed, bool):
                return processed, True, ''
            s = str(processed).strip().lower()
            if s in ('1','true','yes','y','on'):
                return True, True, ''
            if s in ('0','false','no','n','off'):
                return False, True, ''
            return None, False, f"Invalid boolean: '{processed}'"
        if col_type in ('INT','BIGINT','SMALLINT','TINYINT'):
            if isinstance(processed, str):
                cleaned = re.sub(r'[,\s]', '', processed)
            else:
                cleaned = processed
            try:
                intval = int(float(cleaned))
                return intval, True, ''
            except Exception:
                return None, False, f"Invalid integer: '{processed}'"
        if col_type in ('DECIMAL','NUMERIC','FLOAT','REAL','MONEY'):
            if isinstance(processed, str):
                cleaned = re.sub(r'[,\s]', '', processed)
            else:
                cleaned = processed
            try:
                f = float(cleaned)
                if not math.isfinite(f):
                    return None, False, f"Invalid numeric (inf/NaN): '{processed}'"
                return f, True, ''
            except Exception:
                return None, False, f"Invalid numeric: '{processed}'"
        if col_type in ('DATE','DATETIME','DATETIME2','SMALLDATETIME'):
            if isinstance(processed, (datetime, date)):
                return processed, True, ''
            s = str(processed).strip()
            date_formats = [
                '%Y-%m-%d','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M:%S.%f',
                '%d/%m/%Y','%m/%d/%Y','%Y/%m/%d',
                '%d-%m-%Y','%m-%d-%Y','%Y%m%d',
                '%d.%m.%Y','%m.%d.%Y',
                '%Y-%m-%d %H:%M','%d/%m/%Y %H:%M:%S',
                '%m/%d/%Y %H:%M:%S','%Y/%m/%d %H:%M:%S'
            ]
            for fmt in date_formats:
                try:
                    parsed = datetime.strptime(s, fmt)
                    if col_type == 'DATE':
                        return parsed.date(), True, ''
                    return parsed, True, ''
                except Exception:
                    continue
            return None, False, f"Invalid date format: '{processed}'"
        return str(processed).strip(), True, ''
    except ValueError as ve:
        return None, False, str(ve)
    except Exception as exc:
        logger.exception("Conversion error for column %s: %s", column_name, exc)
        return None, False, f"Type conversion error: {exc}"

def process_excel_file(
    file_path,
    table_name,
    primary_header=None,
    sheet_name=None,
    periode_date=None,
    strict_mode=True
):
    """
    Hybrid Excel file processor:
    - strict_mode=True: perform full validation (header detection, type checking, DB insert)
    - strict_mode=False: lightweight validation (direct header usage, no DB insert)
    """
    result = {"success": False, "message": "", "data": [], "errors": []}

    if not os.path.exists(file_path):
        return {"success": False, "message": f"File tidak ditemukan: {file_path}"}

    try:
        # --- Load Excel file ---
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name or 0, header=None if strict_mode else 0)
        except ValueError as e:
            if "Worksheet named" in str(e):
                return {'success': False, 'message': f'Sheet "{sheet_name}" tidak ditemukan dalam file Excel'}
            else:
                return {'success': False, 'message': f'Error membaca sheet: {str(e)}'}

        if df.empty:
            return {'success': False, 'message': f'Sheet "{sheet_name}" kosong atau tidak memiliki data'}

        # --- Ambil metadata kolom dari database ---
        columns_info = get_column_info(table_name)
        if not columns_info:
            return {'success': False, 'message': f"Tabel '{table_name}' tidak ditemukan di database."}

        required_headers = list(columns_info.keys())

        # ========== STRICT MODE ==========
        if strict_mode:
            # --- Deteksi header otomatis dan validasi struktur ---
            try:
                header_row, valid_headers_mapping, missing_headers, detected_primary = find_header_row_and_validate(
                    df, required_headers, primary_header
                )
            except ValueError as validation_error:
                excel_headers_for_error = []
                for idx in range(min(10, len(df))):
                    row_data = [str(val).strip() if pd.notna(val) else "" for val in df.iloc[idx]]
                    non_empty = [v for v in row_data if v]
                    if len(non_empty) >= 2:
                        excel_headers_for_error = row_data
                        break
                if not excel_headers_for_error:
                    excel_headers_for_error = [str(val).strip() for val in df.iloc[0]]

                return {
                    'success': False,
                    'message': str(validation_error),
                    'validation_type': 'column_structure',
                    'header_info': {
                        'required_headers': required_headers,
                        'excel_headers': excel_headers_for_error
                    }
                }

            # --- Tentukan data mulai dari baris header ---
            data_start_row = find_data_start_row(df, header_row, detected_primary)

            excel_headers = [normalize_column_name(val) for val in df.iloc[header_row]]
            data_df = df.iloc[data_start_row:].copy()
            data_df.columns = range(len(data_df.columns))

            # Buat mapping index kolom
            col_index_mapping = {}
            for excel_header, db_header in valid_headers_mapping:
                for idx, header in enumerate(excel_headers):
                    if header == excel_header:
                        col_index_mapping[idx] = db_header
                        break

            # Ekstraksi data terfilter
            filtered_data = {}
            for col_idx, db_header in col_index_mapping.items():
                if col_idx < len(data_df.columns):
                    filtered_data[db_header] = data_df.iloc[:, col_idx]

            # Tambahkan default untuk kolom hilang
            for missing_col in missing_headers:
                col_info = columns_info.get(missing_col, {})
                if col_info.get('default_value') is not None:
                    filtered_data[missing_col] = [
                        process_default_value(col_info['default_value'], col_info)
                    ] * len(data_df)
                elif col_info.get('is_nullable', False):
                    filtered_data[missing_col] = [None] * len(data_df)

            final_df = pd.DataFrame(filtered_data)
            final_df = final_df.dropna(how='all')
            final_df = final_df[
                ~final_df.astype(str).apply(lambda x: x.str.strip().eq('').all(), axis=1)
            ].reset_index(drop=True)

            if len(final_df) == 0:
                return {
                    'success': False,
                    'message': 'Tidak ada data valid ditemukan untuk diinsert',
                    'header_info': {
                        'header_row': header_row + 1,
                        'data_start_row': data_start_row + 1,
                        'detected_primary': detected_primary,
                        'missing_headers': missing_headers,
                        'sheet_used': sheet_name
                    }
                }

            # --- Validasi isi data ---
            validation_errors = []
            validated_rows = []

            for idx, row in final_df.iterrows():
                record = {}
                for col in final_df.columns:
                    col_info = columns_info.get(col)
                    value = row[col]
                    converted, is_valid, error_msg = validate_and_convert_value(value, col_info, col)
                    if not is_valid:
                        validation_errors.append(f"Row {idx + 1}, Column '{col}': {error_msg}")
                        record[col] = None
                    else:
                        record[col] = converted

                # Tambahkan kolom tambahan
                record["period_date"] = periode_date or process_default_value("date")
                record["upload_date"] = "__USE_DATABASE_DEFAULT__"
                validated_rows.append(record)

            # Hitung error rate
            if validation_errors:
                error_rate = len(validation_errors) / (len(final_df) * len(final_df.columns))
                if error_rate > 0.1:
                    return {
                        'success': False,
                        'message': f'Validasi data gagal. Tingkat kesalahan: {error_rate:.1%}',
                        'validation_errors': validation_errors[:20],
                        'total_errors': len(validation_errors),
                        'rows_processed': len(final_df)
                    }

            validated_df = pd.DataFrame(validated_rows)

            # Insert ke database
            insert_result = insert_to_database(validated_df, table_name, periode_date, replace_existing=True)
            insert_result["rows_processed"] = len(validated_df)
            insert_result["validation_warnings"] = len(validation_errors)
            insert_result["mode"] = "strict"

            return insert_result

        # ========== LIGHT MODE ==========
        else:
            # --- Gunakan nama kolom langsung ---
            if primary_header and isinstance(primary_header, int):
                df.columns = df.iloc[primary_header].tolist()
                df = df.drop(range(primary_header + 1))
            else:
                df.columns = df.columns.astype(str)

            db_columns = columns_info
            valid_data, error_rows = [], []

            for idx, row in df.iterrows():
                record, row_error = {}, []
                for col_name, value in row.items():
                    col_name_db = col_name.strip()
                    col_meta = db_columns.get(col_name_db)
                    if not col_meta:
                        continue
                    if pd.isna(value):
                        record[col_name_db] = handle_null_values_for_column(col_name_db, None, col_meta['data_type'])
                        continue

                    try:
                        converted_value = validate_and_convert_value(value, col_meta['data_type'])
                        record[col_name_db] = converted_value
                    except Exception as e:
                        row_error.append(f"{col_name_db}: {e}")
                        record[col_name_db] = None

                record["period_date"] = periode_date or process_default_value("date")
                record["upload_date"] = "__USE_DATABASE_DEFAULT__"

                if row_error:
                    error_rows.append({"row": int(idx + 2), "errors": row_error})
                valid_data.append(record)

            result["data"] = valid_data
            if error_rows:
                result["message"] = f"Ada {len(error_rows)} baris error."
                result["errors"] = error_rows
            else:
                result["message"] = f"{len(valid_data)} baris berhasil divalidasi."
                result["success"] = True

            result["mode"] = "light"
            return result

    except Exception as e:
        logger.exception("process_excel_file error: %s", e)
        return {'success': False, 'message': f'Gagal memproses file Excel: {e}'}
    