from io import BytesIO
from flask import Blueprint, render_template, request, jsonify, send_file, session, redirect, url_for
import logging

import openpyxl

from models.audit import insert_audit_trail
from config.config import get_db_connection

audit_trails_bp = Blueprint('audit_trails', __name__)
logger = logging.getLogger(__name__)

@audit_trails_bp.route('/audit-trails', methods=['GET'])
def audit_trails_page():
    if 'username' not in session:
        return redirect(url_for('auth.login'))
    insert_audit_trail('view_audit_trails', f"User '{session.get('username')}' accessed audit trails page.")
    return render_template(
        'audit_trails.html',
        username=session.get('username'),
        fullname=session.get('fullname'),
        division=session.get('division'),
        role_access=session.get('role_access')
    )

@audit_trails_bp.route('/api/audit-trails', methods=['GET'])
def api_audit_trails():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    # insert_audit_trail('view_audit_trails_api', f"User '{session.get('username')}' accessed audit trails API.")
    changed_at = request.args.get('changed_at')
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 50))
    offset = (page - 1) * page_size
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        params = []
        where = ''
        if changed_at:
            where = 'WHERE CAST(changed_at AS DATE) = ?'
            params.append(changed_at)
        count_query = f"SELECT COUNT(*) FROM SSOT_AUDIT_TRAILS {where}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        query = f"""
            SELECT id, changed_at, changed_by, action, deskripsi, ip_address
            FROM SSOT_AUDIT_TRAILS
            {where}
            ORDER BY changed_at DESC, id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        params_page = params + [offset, page_size]
        cursor.execute(query, params_page)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'changed_at': row[1].isoformat() if row[1] else '',
                'changed_by': row[2],
                'action': row[3],
                'deskripsi': row[4]
            })
        return jsonify({'success': True, 'data': data, 'total': total})
    except Exception as e:
        logger.error(f"Error fetching audit trails: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@audit_trails_bp.route('/api/download-audit-trails', methods=['POST'])
def api_download_audit_trails():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    # insert_audit_trail('download_audit_trails', f"User '{session.get('username')}' downloaded audit trails Excel.")
    data = request.get_json() or {}
    changed_at = data.get('changed_at')
    changed_by = data.get('changed_by')
    action = data.get('action')
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        params = []
        where_clauses = []
        if changed_at:
            where_clauses.append('CAST(changed_at AS DATE) = ?')
            params.append(changed_at)
        if changed_by:
            where_clauses.append('changed_by = ?')
            params.append(changed_by)
        if action:
            where_clauses.append('action = ?')
            params.append(action)
        where = ''
        if where_clauses:
            where = 'WHERE ' + ' AND '.join(where_clauses)
        query = f"""
            SELECT changed_at, changed_by, action, deskripsi, ip_address
            FROM SSOT_AUDIT_TRAILS
            {where}
            ORDER BY changed_at DESC, id DESC
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()
        # Create Excel
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'Audit Trails'
        ws.append(['No', 'Waktu', 'User', 'Aksi', 'Deskripsi'])
        for idx, row in enumerate(rows, 1):
            ws.append([
                idx,
                row[0].strftime('%d-%m-%Y %H:%M:%S') if row[0] else '',
                row[1],
                row[2],
                row[3]
            ])
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        filename = 'audit_trails.xlsx'
        return send_file(output, as_attachment=True, download_name=filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        logger.error(f"Error exporting audit trails: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass
