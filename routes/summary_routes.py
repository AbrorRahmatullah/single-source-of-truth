from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from datetime import datetime
import logging


from models.audit import insert_audit_trail
from config.config import get_db_connection

summary_bp = Blueprint('summary', __name__)
logger = logging.getLogger(__name__)

@summary_bp.route('/summary', methods=['GET'])
def summary_page():
    if 'username' not in session:
        return redirect(url_for('auth.login'))
    
    insert_audit_trail('view_summary', f"User '{session.get('username')}' accessed summary page.")
    
    return render_template(
        'summary.html',
        username=session.get('username'),
        fullname=session.get('fullname'),
        division=session.get('division'),
        role_access=session.get('role_access')
    )
            
@summary_bp.route('/api/summary', methods=['GET'])
def api_summary():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    try:
        tanggal_data = request.args.get('tanggal_data')
        conn = get_db_connection()
        cursor = conn.cursor()

        now = datetime.now()

        if not tanggal_data or not tanggal_data.strip():
            # Ambil semua EOM tahun berjalan
            cursor.execute("""
                SELECT EOM_DATE
                FROM [10.10.4.12].SMIDWHARIUM.dbo.PBK_EOM
                WHERE YEAR(EOM_DATE) = YEAR(GETDATE())
                ORDER BY EOM_DATE ASC
            """)
            eom_dates = [row[0] for row in cursor.fetchall()]

            # Pilih EOM terakhir yang <= tanggal hari ini
            default_eom = None
            for eom in eom_dates:
                if eom <= now.date():
                    default_eom = eom
                else:
                    break

            # Jika belum ada yang lewat (misal awal tahun), pakai EOM pertama
            if default_eom:
                # Gunakan awal bulan agar konsisten dengan input manual (YYYY-MM-01)
                tanggal_data = f"{default_eom.year}-{default_eom.month:02d}-01"
            else:
                tanggal_data = f"{now.year}-{now.month:02d}-01"

        elif len(tanggal_data) == 7:
            # Jika input manual hanya YYYY-MM, tambahkan '-01'
            tanggal_data = tanggal_data + '-01'

        # Pagination
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))

        # Query utama
        query_get_template_name = f"""
            DECLARE @FilterDate DATE = '{tanggal_data}';
            DECLARE @SQL NVARCHAR(MAX);
            SELECT @SQL = STRING_AGG(
                'SELECT ' 
                + 'CAST(@FilterDate AS DATE) AS PERIOD_DATE, '
                + '''' + template_name + ''' AS template_name, '
                + '''' + division_name + ''' AS division_name, '
                + 'COUNT(t.PERIOD_DATE) AS JUMLAH_DATA, '
                + 'CASE WHEN COUNT(t.PERIOD_DATE) > 1 THEN ''TERSEDIA'' ELSE ''BELUM TERSEDIA'' END AS STATUS '
                + 'FROM ' + QUOTENAME(template_name) + ' t '
                + 'WHERE CAST(t.PERIOD_DATE AS DATE) = @FilterDate',
                ' UNION ALL '
            )
            FROM MasterCreator;
            EXEC sp_executesql @SQL, N'@FilterDate DATE', @FilterDate=@FilterDate;
        """

        cursor.execute(query_get_template_name)
        rows = cursor.fetchall()

        data = [{
            'period_date': row[0].strftime('%Y-%m-%d') if row[0] else None,
            'template_name': row[1],
            'division_name': row[2],
            'jumlah_data': row[3],
            'status': row[4]
        } for row in rows]

        total_records = len(data)
        start = (page - 1) * page_size
        end = start + page_size
        paged_data = data[start:end]

        return jsonify({
            'success': True,
            'data': paged_data,
            'total': total_records,
            'page': page,
            'page_size': page_size,
            'default_date_used': tanggal_data  # Kirim tanggal default ke frontend
        })

    except Exception as e:
        insert_audit_trail('view_monthly_data_failed',
            f"User '{session.get('username')}' failed to access monthly data API: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
