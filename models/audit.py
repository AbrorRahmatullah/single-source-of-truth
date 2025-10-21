import logging
from config.config import get_db_connection
from flask import session, request

logger = logging.getLogger(__name__)

def insert_audit_trail(action, deskripsi=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        username = session.get('username', 'anonymous')
        ip_address = request.remote_addr if request else None
        query = '''
            INSERT INTO SSOT_AUDIT_TRAILS (changed_by, action, deskripsi, ip_address)
            VALUES (?, ?, ?, ?)
        '''
        cursor.execute(query, (username, action, deskripsi, ip_address))
        conn.commit()
    except Exception as e:
        logger.warning(f"Gagal insert audit trail: {e}")
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
