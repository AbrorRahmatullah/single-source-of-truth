from flask import Blueprint, current_app, render_template, request, jsonify, session, redirect, url_for, flash
import pandas as pd
from config.config import get_db_connection
from utils.file_utils import allowed_file
from utils.excel_utils import find_data_start_row, find_primary_header_row, get_excel_sheets, process_excel_file
from utils.db_utils import get_automatic_columns, get_column_info, get_data_count_by_period, get_template_tables, safe_insert_single_record
from models.audit import insert_audit_trail
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import logging

upload_bp = Blueprint('upload', __name__)
logger = logging.getLogger(__name__)

@upload_bp.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if 'username' not in session:
        flash("Please log in first.")
        return redirect(url_for('auth.login'))

    role_access = session.get('role_access')
    fullname = session.get('fullname')
    username = session.get('username')
    division = session.get('division')
    
    if request.method == 'GET':
        # Mendapatkan daftar tabel template untuk dropdown
        template_tables = get_template_tables(role_access, division)
        
        if role_access.lower() == 'user':
            # Ambil hanya tabel yang sesuai division
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT table_name 
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN MasterDivisions d ON t.TABLE_SCHEMA = 'dbo'
                WHERE d.division_name = ?
            """, (division,))
            allowed_tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            # Hanya ambil table yang masuk allowed_tables
            template_tables = [tbl for tbl in template_tables if tbl in allowed_tables]
        
        insert_audit_trail('view_upload', f"User '{session.get('username')}' viewed upload page.")
        # Tampilkan halaman upload dengan data tabel template
        return render_template(
            'upload.html',
            username=username,
            division=division,
            role_access=role_access,
            fullname=fullname,
            template_tables=template_tables
        )
    
    elif request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
            
            file = request.files['file']
            table_name = request.form.get('table_name', '').strip()
            primary_header = request.form.get('primary_header', '').strip() or None
            sheet_name = request.form.get('sheet_name', '').strip() or None
            periode_date = request.form.get('periode_date', '').strip() or None
            
            if file.filename == '':
                return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
            
            if not table_name:
                return jsonify({'success': False, 'message': 'Nama tabel harus dipilih'})
            
            if not allowed_file(file.filename):
                return jsonify({'success': False, 'message': 'File harus berformat Excel (.xlsx atau .xls)'})
            
            # Validasi format tanggal periode
            if periode_date:
                try:
                    periode_date = datetime.strptime(periode_date, '%Y-%m').date().replace(day=1)
                except ValueError:
                    return jsonify({'success': False, 'message': 'Format tanggal periode tidak valid. Gunakan format YYYY-MM'})
            
            # Buat folder uploads ada
            upload_folder = current_app.config['UPLOAD_FOLDER']
            os.makedirs(upload_folder, exist_ok=True)
            
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{filename}"
            file_path = os.path.join(upload_folder, filename)
            file.save(file_path)
            
            print(f"Periode Date: {periode_date}")
            # Proses file
            result = process_excel_file(file_path, table_name, primary_header, sheet_name, periode_date)
            
            # Simpan ke tabel MasterUploader dengan error handling yang lebih baik
            try:
                print("Attempting to insert to MasterUploader...")
                
                # Gunakan fungsi safe_insert_single_record
                columns = ['username', 'division', 'template', 'sheets', 'file_upload', 'period_date', 'upload_date']
                values = [
                    session.get('username'),
                    session.get('division'),
                    table_name,
                    sheet_name,
                    file_path,
                    periode_date,
                    datetime.now()
                ]
                
                # Debug data sebelum insert
                print("Data to be inserted to MasterUploader:")
                for col, val in zip(columns, values):
                    print(f"  {col}: {type(val).__name__} = {repr(val)}")
                
                insert_success = safe_insert_single_record('MasterUploader', columns, values)
                insert_audit_trail('upload', f"User '{session.get('username')}' uploaded file '{filename}'.")
                
                if not insert_success:
                    logger.warning("Failed to insert to MasterUploader, but continuing with main process")
                    
            except Exception as e:
                insert_audit_trail('upload_failed', f"User '{session.get('username')}' failed to upload file '{filename}': {str(e)}")
                logger.error(f"Gagal insert ke MasterUploader: {str(e)}")
                # Don't fail the entire process if MasterUploader insert fails
                logger.warning("Continuing with main process despite MasterUploader insert failure")
            
            return jsonify(result)
        
        except Exception as e:
            logger.error(f"Error in upload_file: {str(e)}")
            return jsonify({'success': False, 'message': f'Error: {str(e)}'})

    return redirect(url_for('upload.upload_file'))

@upload_bp.route('/analyze-excel', methods=['POST'])
def analyze_excel():
    """Analyze Excel file structure without inserting to database"""
    
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        file = request.files['file']
        sheet_name = request.form.get('sheet_name', '').strip() or None
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'Tidak ada file yang dipilih'})
        
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'message': 'File harus berformat Excel (.xlsx atau .xls)'})
        
        # Simpan file temporary
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"analyze_{timestamp}_{filename}"
        file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # Dapatkan daftar sheet terlebih dahulu
            available_sheets = get_excel_sheets(file_path)
            
            # Baca file Excel dengan sheet yang spesifik atau default
            if sheet_name:
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    sheet_used = sheet_name
                except ValueError:
                    return jsonify({'success': False, 'message': f'Sheet "{sheet_name}" tidak ditemukan dalam file Excel'})
            else:
                df = pd.read_excel(file_path, header=None)
                sheet_used = available_sheets[0] if available_sheets else 'Sheet pertama'
            
            # Auto-detect header
            header_row, detected_primary = find_primary_header_row(df)
            
            # Get headers
            excel_headers = []
            for val in df.iloc[header_row]:
                if pd.notna(val):
                    excel_headers.append(str(val).strip())
                else:
                    excel_headers.append("")
            
            # Find data start row
            data_start_row = find_data_start_row(df, header_row, detected_primary)
            total_data_rows = len(df) - data_start_row
            
            # Sample data
            sample_data = []
            for i in range(data_start_row, min(data_start_row + 3, len(df))):
                row_sample = []
                for j in range(min(len(excel_headers), len(df.columns))):
                    val = df.iloc[i, j]
                    row_sample.append(str(val) if pd.notna(val) else "")
                sample_data.append(row_sample)
            
            result = {
                'success': True,
                'analysis': {
                    'total_rows': total_data_rows,
                    'total_columns': len(df.columns),
                    'header_row': header_row + 1,
                    'data_start_row': data_start_row + 1,
                    'detected_primary_header': detected_primary,
                    'headers': excel_headers,
                    'sample_data': sample_data,
                    'data_rows_available': len(df) - data_start_row,
                    'available_sheets': available_sheets,
                    'sheet_used': sheet_used
                }
            }
            insert_audit_trail('analyze_excel', f"User '{session.get('username')}' analyze excel.")
        finally:
            # Hapus file temporary
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in analyze_excel: {str(e)}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@upload_bp.route('/preview-headers/<table_name>')
def preview_headers(table_name):
    """
    Preview header database dengan menandai kolom otomatis
    """
    try:
        # Dapatkan semua kolom termasuk otomatis
        all_columns = get_column_info(table_name, exclude_automatic=False)
        
        # Dapatkan kolom yang diharapkan dari Excel (tanpa otomatis)
        excel_columns = get_column_info(table_name, exclude_automatic=True)
        
        # Tandai kolom otomatis
        automatic_columns = get_automatic_columns()
        
        headers_with_info = {}
        for col_name, col_info in all_columns.items():
            headers_with_info[col_name] = {
                **col_info,
                'is_automatic': col_name.lower() in [ac.lower() for ac in automatic_columns],
                'required_in_excel': col_name in excel_columns
            }
        
        data_counts = get_data_count_by_period(table_name)
        insert_audit_trail('preview_data', f"User '{session.get('username')}' preview data.")
        
        return jsonify({
            'success': True,
            'headers': headers_with_info,
            'table_name': table_name,
            'total_columns': len(all_columns),
            'excel_required_columns': len(excel_columns),
            'automatic_columns': len(automatic_columns),
            'data_counts_by_period': data_counts
        })
        
    except Exception as e:
        logger.error(f"Error getting table headers: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        })

@upload_bp.route('/get-template-tables')
def get_template_tables_endpoint():
    """Endpoint untuk mendapatkan daftar tabel template"""
    try:
        tables = get_template_tables()
        # insert_audit_trail('get_template_tables', f"User '{session.get('username')}' get template tables.")
        
        return jsonify({
            'success': True, 
            'tables': tables,
            'message': f'Ditemukan {len(tables)} tabel template'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

# @upload_bp.route('/insert-data', methods=['POST'])
# def insert_data():
#     if 'username' not in session:
#         return jsonify({'success': False, 'message': 'Session expired. Silakan login ulang.'}), 401

#     session_data = session.get('analyzed_data')
#     if not session_data:
#         return jsonify({'success': False, 'message': 'Tidak ada data yang dianalisis. Jalankan analisis terlebih dahulu.'})

#     table_name = session_data.get('table')
#     periode_str = session_data.get('periode')
#     data_rows = session_data.get('data')

#     if not data_rows:
#         return jsonify({'success': False, 'message': 'Tidak ada data yang valid untuk diinsert.'})

#     periode_date = None
#     if periode_str:
#         try:
#             periode_date = datetime.strptime(periode_str, "%Y-%m-%d").date()
#         except Exception:
#             try:
#                 periode_date = datetime.strptime(periode_str, "%Y-%m").date().replace(day=1)
#             except Exception:
#                 pass

#     logger.info("Inserting %s rows into %s (periode=%s)", len(data_rows), table_name, periode_date)

#     result = insert_to_database(
#         rows=data_rows,
#         table_name=table_name,
#         periode_date=periode_date,
#         replace_existing=True
#     )

#     if result.get('success'):
#         insert_audit_trail('insert_success', f"User '{session.get('username')}' berhasil insert {result.get('inserted_rows')} baris ke tabel '{table_name}'.")
#     else:
#         insert_audit_trail('insert_failed', f"User '{session.get('username')}' gagal insert ke tabel '{table_name}': {result.get('message')}")

#     session.pop('analyzed_data', None)
#     return jsonify(result)
