import re
import math
from datetime import datetime, date
from typing import Any, Dict, Tuple

import logging

from utils.helpers import handle_null_values_for_column

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_password_strength(password):
    if len(password) < 9:
        return False, "Password minimal 9 karakter"
    if not re.search(r'[A-Z]', password):
        return False, "Password harus mengandung huruf kapital"
    if not re.search(r'\d', password):
        return False, "Password harus mengandung angka"
    if not re.search(r'[@$!%*?&]', password):
        return False, "Password harus mengandung karakter spesial"
    return True, "Password valid"

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
