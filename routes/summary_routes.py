from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, send_file
from datetime import datetime, timedelta
import logging
import os
from dateutil.relativedelta import relativedelta

from models.audit import insert_audit_trail
from config.config import get_db_connection
from utils.db_utils import check_master_uploader_by_date

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

@summary_bp.route('/api/analytics-dashboard', methods=['GET'])
def api_analytics_dashboard():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    conn = None
    cursor = None

    try:
        # Get year parameter (default: current year)
        year = request.args.get('year', str(datetime.now().year), type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Total User Count (from MasterUsers)
        cursor.execute("""
            SELECT COUNT(*)
            FROM MasterUsers
        """)
        total_users = cursor.fetchone()[0]

        # 2. Total File Download Count (from SSOT_AUDIT_TRAILS where action = 'download_monthly_data')
        cursor.execute("""
            SELECT COUNT(*)
            FROM SSOT_AUDIT_TRAILS
            WHERE action = 'download_monthly_data'
        """)
        total_downloads = cursor.fetchone()[0]

        # 3. Total File Upload Count (from MasterUploader)
        cursor.execute("""
            SELECT COUNT(*)
            FROM MasterUploader
        """)
        total_uploads = cursor.fetchone()[0]

        # 4. User Login Per Month for the specified year (Line Chart data)
        cursor.execute("""
            SELECT
                MONTH(changed_at) as month,
                COUNT(*) as login_count
            FROM SSOT_AUDIT_TRAILS
            WHERE action = 'login'
            AND YEAR(changed_at) = ?
            GROUP BY MONTH(changed_at)
            ORDER BY MONTH(changed_at)
        """, (year,))

        login_data = cursor.fetchall()
        login_monthly = {}
        for row in login_data:
            month = row[0]
            count = row[1]
            month_name = datetime(year, month, 1).strftime('%B')
            login_monthly[month_name] = count

        # Fill missing months with 0
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
        login_monthly_data = [{'month': m, 'count': login_monthly.get(m, 0)} for m in month_names]

        # 5. Download and Upload Per Month for the specified year (Double Bar Chart data)
        # Download data from SSOT_AUDIT_TRAILS
        cursor.execute("""
            SELECT
                MONTH(changed_at) as month,
                COUNT(*) as download_count
            FROM SSOT_AUDIT_TRAILS
            WHERE action = 'download_monthly_data'
            AND YEAR(changed_at) = ?
            GROUP BY MONTH(changed_at)
            ORDER BY MONTH(changed_at)
        """, (year,))

        download_data = cursor.fetchall()
        download_monthly = {}
        for row in download_data:
            month = row[0]
            count = row[1]
            month_name = datetime(year, month, 1).strftime('%B')
            download_monthly[month_name] = count

        # Upload data from MasterUploader
        cursor.execute("""
            SELECT
                MONTH(upload_date) as month,
                COUNT(*) as upload_count
            FROM MasterUploader
            WHERE YEAR(upload_date) = ?
            GROUP BY MONTH(upload_date)
            ORDER BY MONTH(upload_date)
        """, (year,))

        upload_data = cursor.fetchall()
        upload_monthly = {}
        for row in upload_data:
            month = row[0]
            count = row[1]
            month_name = datetime(year, month, 1).strftime('%B')
            upload_monthly[month_name] = count

        # Combine download and upload data
        traffic_monthly = {}
        for m in month_names:
            traffic_monthly[m] = {
                'downloads': download_monthly.get(m, 0),
                'uploads': upload_monthly.get(m, 0)
            }

        traffic_monthly_data = [
            {
                'month': m,
                'downloads': traffic_monthly[m]['downloads'],
                'uploads': traffic_monthly[m]['uploads']
            }
            for m in month_names
        ]

        return jsonify({
            'success': True,
            'summary': {
                'total_users': total_users,
                'total_downloads': total_downloads,
                'total_uploads': total_uploads
            },
            'login_monthly': login_monthly_data,
            'traffic_monthly': traffic_monthly_data,
            'year': year
        })

    except Exception as e:
        logger.error(f"Error fetching analytics dashboard data: {str(e)}")
        insert_audit_trail('analytics_dashboard_failed',
            f"User '{session.get('username')}' failed to fetch analytics dashboard: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@summary_bp.route('/api/summary', methods=['GET'])
def api_summary():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    conn = None
    cursor = None

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

        # Check master uploader data for the given date
        master_uploader_data = check_master_uploader_by_date(tanggal_data)

        # Pagination
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))

        # Get all templates from MasterCreator
        cursor.execute("""
            SELECT template_name, division_name
            FROM MasterCreator
            ORDER BY template_name
        """)
        templates = cursor.fetchall()

        data = []

        # Process each template
        for template in templates:
            template_name = template[0]
            division_name = template[1]

            # Get upload date from master uploader data
            upload_date = None
            for uploader in master_uploader_data:
                if uploader['template'] == template_name:
                    upload_date = uploader['upload_date']
                    break

            # Get data count from template table
            jumlah_data = 0
            status = 'BELUM TERSEDIA'

            try:
                # Check if template table exists
                count_query = f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo'
                """
                cursor.execute(count_query, (template_name,))
                table_exists = cursor.fetchone()[0] > 0

                if table_exists:
                    # Get data count for the specified date
                    data_query = f"""
                        SELECT COUNT(*)
                        FROM [{template_name}]
                        WHERE CAST(PERIOD_DATE AS DATE) = CAST(? AS DATE)
                    """
                    cursor.execute(data_query, (tanggal_data,))
                    jumlah_data = cursor.fetchone()[0]

                    # Set status
                    if jumlah_data > 0:
                        status = 'TERSEDIA'
                    else:
                        status = 'BELUM TERSEDIA'

            except Exception as table_error:
                logger.warning(f"Error checking template {template_name}: {str(table_error)}")
                status = 'BELUM TERSEDIA'
                jumlah_data = 0

            data.append({
                'period_date': tanggal_data,
                'template_name': template_name,
                'division_name': division_name,
                'upload_date': upload_date,
                'jumlah_data': jumlah_data,
                'status': status
            })

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
            'default_date_used': tanggal_data,
            'master_uploader_info': master_uploader_data
        })

    except Exception as e:
        insert_audit_trail('view_monthly_data_failed',
            f"User '{session.get('username')}' failed to access monthly data API: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@summary_bp.route('/api/download-file', methods=['GET'])
def download_file():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'}), 401
    
    try:
        template_name = request.args.get('template')
        period_date = request.args.get('period_date')
        
        if not template_name or not period_date:
            return jsonify({'success': False, 'message': 'Template dan periode harus diisi.'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query untuk mendapatkan file_upload path
        query = """
            SELECT TOP 1 file_upload, upload_date
            FROM MasterUploader
            WHERE template = ? 
            AND CAST(period_date AS DATE) = CAST(? AS DATE)
            ORDER BY upload_date DESC
        """
        
        cursor.execute(query, (template_name, period_date))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({'success': False, 'message': 'File tidak ditemukan di database.'}), 404
        
        file_path = result[0]
        
        # Normalisasi path (ganti backslash jadi forward slash untuk compatibility)
        file_path = file_path.replace('\\', '/')
        
        # Cek apakah file ada di server
        if not os.path.exists(file_path):
            # Coba alternatif path jika ada
            alt_path = os.path.join(os.getcwd(), file_path)
            if os.path.exists(alt_path):
                file_path = alt_path
            else:
                logger.error(f"File not found: {file_path}")
                return jsonify({'success': False, 'message': f'File tidak ada di server.'}), 404
        
        # Log audit trail
        insert_audit_trail('download_file', 
            f"User '{session.get('username')}' downloaded file: {os.path.basename(file_path)}")
        
        # Dapatkan nama file original
        filename = os.path.basename(file_path)
        
        # Tentukan mimetype berdasarkan ekstensi
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        if file_path.lower().endswith('.xls'):
            mimetype = 'application/vnd.ms-excel'
        elif file_path.lower().endswith('.csv'):
            mimetype = 'text/csv'
        
        return send_file(
            file_path,
            mimetype=mimetype,
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        insert_audit_trail('download_file_failed',
            f"User '{session.get('username')}' failed to download file: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500