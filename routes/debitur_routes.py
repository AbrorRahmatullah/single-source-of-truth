from io import BytesIO
from flask import Blueprint, jsonify, send_file, session
import logging

import openpyxl

from models.audit import insert_audit_trail
from config.config import get_db_connection

debitur_bp = Blueprint('debitur', __name__)
logger = logging.getLogger(__name__)

# CRUD Divisions Management - Simplified Backend Code (Create & Delete Only)
@debitur_bp.route('/api/debitur-aktif', methods=['GET'])
def api_debitur_aktif():
    """
    GET /api/debitur-aktif
    Mengambil data debitur aktif untuk preview (max 1000 rows)
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Default: periode terakhir (EOD_DATE)
        cursor.execute("""
            SELECT MAX(PBK_EOD_DATE) FROM [10.10.4.12].SMIDWHARIUM.dbo.PBK_T_INF_COR_FACILITY_ACCOUNT
            WHERE FACILITY_STATUS = 'AC'
        """)
        last_eod_date = cursor.fetchone()[0]

        # Query data dari function
        cursor.execute("""
            SELECT TOP 1000
                PBK_EOD_DATE,
                NOMOR_CIF,
                CUST_NAME,
                FACILITY_NO
            FROM Func_GetCustomerData(?)
            ORDER BY NOMOR_CIF
        """, (last_eod_date,))
        rows = cursor.fetchall()

        # Format data
        data = []
        for idx, row in enumerate(rows):
            data.append({
                'pbk_eod_date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'kode_debitur': row[1],
                'nama_debitur': row[2],
                'facility_no': row[3],
                'status': 'AKTIF',
                'tanggal_dibuat': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'last_update': row[0].strftime('%Y-%m-%d') if row[0] else None,
            })

        # Stats
        cursor.execute("""
            SELECT COUNT(DISTINCT NOMOR_CIF)
            FROM Func_GetCustomerData(?)
        """, (last_eod_date,))
        total_records = cursor.fetchone()[0]

        stats = {
            'total': total_records,
            'active': total_records,
            'last_update': last_eod_date.strftime('%Y-%m-%d') if last_eod_date else None
        }
        
        insert_audit_trail('view_debitur_aktif', f"User '{session.get('username')}' viewed debitur aktif preview.")
        return jsonify({'success': True, 'data': data, 'stats': stats})

    except Exception as e:
        insert_audit_trail('view_debitur_aktif_failed', f"User '{session.get('username')}' failed to view debitur aktif preview: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@debitur_bp.route('/api/sync-debitur', methods=['POST'])
def api_sync_debitur():
    """
    POST /api/sync-debitur
    Refresh data debitur aktif dari database
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    # insert_audit_trail('view_sync_debitur', f"User '{session.get('username')}' viewed sync debitur aktif page.")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Ambil periode terbaru
        cursor.execute("""
            SELECT MAX(PBK_EOD_DATE) FROM [10.10.4.12].SMIDWHARIUM.dbo.PBK_T_INF_COR_FACILITY_ACCOUNT
            WHERE FACILITY_STATUS = 'AC'
        """)
        last_eod_date = cursor.fetchone()[0]

        # Query data dari function
        cursor.execute("""
            SELECT
                PBK_EOD_DATE,
                NOMOR_CIF,
                CUST_NAME,
                FACILITY_NO
            FROM Func_GetCustomerData(?)
            ORDER BY NOMOR_CIF
        """, (last_eod_date,))
        rows = cursor.fetchall()

        data = []
        for idx, row in enumerate(rows):
            data.append({
                'pbk_eod_date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'kode_debitur': row[1],
                'nama_debitur': row[2],
                'facility_no': row[3],
                'status': 'AKTIF',
                'tanggal_dibuat': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'last_update': row[0].strftime('%Y-%m-%d') if row[0] else None,
            })

        stats = {
            'total': len(data),
            'active': len(data),
            'last_update': last_eod_date.strftime('%Y-%m-%d') if last_eod_date else None
        }
        
        cursor.execute("""
            MERGE INTO SSOT_LAST_SYNC AS target
            USING (SELECT ? AS sync_type) AS source
            ON target.sync_type = source.sync_type
            WHEN MATCHED THEN
                UPDATE SET last_sync_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (sync_type, last_sync_time) VALUES (source.sync_type, GETDATE());
        """, ('debitur_aktif',))
        conn.commit()
        
        # insert_audit_trail('sync_debitur_aktif', f"User '{session.get('username')}' synchronized debitur aktif data.")
        return jsonify({'success': True, 'data': data, 'stats': stats})

    except Exception as e:
        # insert_audit_trail('sync_debitur_aktif_failed', f"User '{session.get('username')}' failed to synchronize debitur aktif data: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        
@debitur_bp.route('/api/last-sync', methods=['GET'])
def api_last_sync():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1 last_sync_time
            FROM SSOT_LAST_SYNC
            WHERE sync_type = 'debitur_aktif'
            ORDER BY last_sync_time DESC
        """)
        row = cursor.fetchone()
        last_sync = row[0].strftime('%Y-%m-%d %H:%M:%S') if row and row[0] else None
        
        # insert_audit_trail('view_last_sync', f"User '{session.get('username')}' viewed last sync time for debitur aktif.")
        return jsonify({'success': True, 'last_sync': last_sync})
    except Exception as e:
        # insert_audit_trail('view_last_sync_failed', f"User '{session.get('username')}' failed to view last sync time for debitur aktif: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@debitur_bp.route('/api/download-debitur-excel', methods=['POST'])
def api_download_debitur_excel():
    """
    POST /api/download-debitur-excel
    Generate dan download file Excel debitur aktif
    """
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    # insert_audit_trail('view_download_debitur_excel', f"User '{session.get('username')}' viewed download debitur aktif Excel page.")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Ambil periode terbaru
        cursor.execute("""
            SELECT MAX(PBK_EOD_DATE) FROM [10.10.4.12].SMIDWHARIUM.dbo.PBK_T_INF_COR_FACILITY_ACCOUNT
            WHERE FACILITY_STATUS = 'AC'
        """)
        last_eod_date = cursor.fetchone()[0]

        # Query semua data
        cursor.execute("""
            SELECT
                PBK_EOD_DATE,
                NOMOR_CIF,
                CUST_NAME,
                FACILITY_NO
            FROM Func_GetCustomerData(?)
            ORDER BY NOMOR_CIF
        """, (last_eod_date,))
        rows = cursor.fetchall()

        # Generate Excel file in memory
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Debitur Aktif"

        # Header
        ws.append(['Tanggal Data', 'Kode Debitur', 'Nama Debitur', 'Nomor Fasilitas'])

        # Data
        for row in rows:
            ws.append([
                row[0].strftime('%Y-%m-%d') if row[0] else '',
                row[1],
                row[2],
                row[3]
            ])

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"debitur_aktif_{last_eod_date.strftime('%Y%m%d')}.xlsx" if last_eod_date else "debitur_aktif.xlsx"
        # insert_audit_trail('download_debitur_excel', f"User '{session.get('username')}' downloaded debitur aktif Excel file.")
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        # insert_audit_trail('download_debitur_excel_failed', f"User '{session.get('username')}' failed to download debitur aktif Excel file: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
