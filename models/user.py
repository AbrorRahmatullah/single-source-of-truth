from config.config import get_db_connection

def get_user_by_username(username):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT password_hash, role_access, division, fullname FROM MasterUsers WHERE username = ?", (username,))
    user = cur.fetchone()
    conn.close()
    return user
