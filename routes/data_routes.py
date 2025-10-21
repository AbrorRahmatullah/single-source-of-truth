from flask import (
    Blueprint, render_template, request, jsonify,
    session, redirect, url_for, send_file
)
from io import BytesIO
import openpyxl


from config.config import get_db_connection
from models.audit import insert_audit_trail

data_bp = Blueprint('data', __name__)


@data_bp.route('/data', methods=['GET'])
def data_page():
    if 'username' not in session:
        return redirect(url_for('auth.login'))
    
    insert_audit_trail('view_data_page', f"User '{session.get('username')}' accessed data page.")
    
    return render_template(
        'data.html',
        username=session.get('username'),
        fullname=session.get('fullname'),
        division=session.get('division'),
        role_access=session.get('role_access')
    )

@data_bp.route('/api/data', methods=['GET'])
def api_data():
    """
    Endpoint untuk datatable monthly data dengan filter tanggal, pagination, dan limit
    Query params: tanggal_data, page, page_size
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    # insert_audit_trail('view_monthly_data', f"User '{session.get('username')}' accessed monthly data API.")
    
    try:
        tanggal_data = request.args.get('tanggal_data')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))

        conn = get_db_connection()
        cursor = conn.cursor()

        # Filter tanggal_data jika ada
        where_clause = ""
        params = []
        if tanggal_data:
            # Jika hanya bulan-tahun, filter dengan LIKE atau range
            if len(tanggal_data) == 7:  # format YYYY-MM
                where_clause = "WHERE CONVERT(VARCHAR(7), Tanggal_Data, 120) = ?"
                params.append(tanggal_data)
            else:
                where_clause = "WHERE Tanggal_Data = ?"
                params.append(tanggal_data)

        # Hitung total data
        count_query = f"SELECT COUNT(*) FROM [SMIDWHSSOT].[dbo].[SSOT_FINAL_MONTHLY] {where_clause}"
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()[0]

        # Ambil data dengan pagination
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT *
            FROM [SMIDWHSSOT].[dbo].[SSOT_FINAL_MONTHLY]
            {where_clause}
            ORDER BY Tanggal_Data DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        cursor.execute(data_query, params + [offset, page_size])
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        data = [dict(zip(columns, row)) for row in rows]
        
        insert_audit_trail('view_monthly_data', f"User '{session.get('username')}' viewed monthly data, page {page}.")
        return jsonify({
            'success': True,
            'data': data,
            'total': total_records,
            'page': page,
            'page_size': page_size
        })
    except Exception as e:
        insert_audit_trail('view_monthly_data_failed', f"User '{session.get('username')}' failed to access monthly data API: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        
@data_bp.route('/api/download-data', methods=['POST'])
def api_download_data():
    """
    Download data excel sesuai filter
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    # insert_audit_trail('view_download_monthly_data', f"User '{session.get('username')}' accessed download monthly data Excel page.")
    
    try:
        tanggal_data = request.json.get('tanggal_data')
        conn = get_db_connection()
        cursor = conn.cursor()

        where_clause = ""
        params = []
        if tanggal_data:
            if len(tanggal_data) == 7:  # format YYYY-MM
                where_clause = "WHERE CONVERT(VARCHAR(7), Tanggal_Data, 120) = ?"
                params.append(tanggal_data)
            else:
                where_clause = "WHERE Tanggal_Data = ?"
                params.append(tanggal_data)

        # --- Ambil data utama ---
        query = f"""
            SELECT * 
            FROM [SMIDWHSSOT].[dbo].[SSOT_FINAL_MONTHLY] 
            {where_clause} 
            ORDER BY Tanggal_Data DESC
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # --- Ambil daftar kolom numeric ---
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'SSOT_FINAL_MONTHLY'
            AND DATA_TYPE = 'numeric'
        """)
        numeric_columns = {row[0] for row in cursor.fetchall()}

        # --- Buat Workbook ---
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Monthly Data"

        # Header
        ws.append(columns)

        # Data rows
        for row in rows:
            ws.append(list(row))

        # Format kolom numeric agar tidak eksponen & bisa di-SUM
        from openpyxl.styles import numbers
        for col_idx, col_name in enumerate(columns, start=1):
            if col_name in numeric_columns:
                for row_idx in range(2, len(rows) + 2):  # skip header
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.number_format = numbers.FORMAT_NUMBER_COMMA_SEPARATED1

        # Auto adjust column width
        for col_idx, col_name in enumerate(columns, start=1):
            max_length = len(str(col_name))  # header length
            for row in rows:
                value = row[col_idx - 1]
                if value is not None:
                    max_length = max(max_length, len(str(value)))
            ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(max_length + 2, 50)

        # Output ke response
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        filename = f"monthly_data_{tanggal_data}.xlsx"
        
        insert_audit_trail('download_monthly_data', f"User '{session.get('username')}' downloaded monthly data Excel file.")
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        insert_audit_trail('download_monthly_data_failed', f"User '{session.get('username')}' failed to download monthly data Excel file: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
