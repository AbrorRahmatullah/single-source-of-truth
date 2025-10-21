import re
from flask import Blueprint, flash, render_template, request, jsonify, session, redirect, url_for
import logging

from models.audit import insert_audit_trail
from config.config import get_db_connection

division_bp = Blueprint('division', __name__)
logger = logging.getLogger(__name__)

# CRUD Divisions Management - Simplified Backend Code (Create & Delete Only)
@division_bp.route('/divisions-page')
def divisions_page():
    if 'username' not in session:
        flash('Please log in first.')
        return redirect(url_for('auth.login'))
    insert_audit_trail('view_divisions', f"User '{session.get('username')}' viewed divisions management page.")
    
    return render_template(
        'divisions_management.html',
        username=session.get('username'),
        fullname=session.get('fullname'),
        division=session.get('division'),
        role_access=session.get('role_access')
    )

@division_bp.route('/divisions', methods=['GET'])
def get_divisions():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, division_name, created_by, 
                   CONVERT(VARCHAR(19), created_date, 120) as created_date
            FROM MasterDivisions
            ORDER BY created_date DESC
        """)
        
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        divisions = [dict(zip(columns, row)) for row in rows]
        
        insert_audit_trail('view_divisions', f"User '{session.get('username')}' viewed division list.")
        return jsonify({'success': True, 'divisions': divisions})
        
    except Exception as e:
        insert_audit_trail('view_divisions_failed', f"User '{session.get('username')}' failed to view division list: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Separate endpoint for dropdown options (simple format)
@division_bp.route('/divisions/dropdown', methods=['GET'])
def get_divisions_dropdown():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT division_name FROM MasterDivisions WHERE division_name IS NOT NULL ORDER BY division_name")
        rows = cursor.fetchall()
        divisions = [r[0] for r in rows]
        insert_audit_trail('get_divisions_dropdown', f"User '{session.get('username')}' accessed divisions dropdown.")
        return jsonify({'success': True, 'divisions': divisions})
        
    except Exception as e:
        insert_audit_trail('get_divisions_dropdown_failed', f"User '{session.get('username')}' failed to access divisions dropdown: {str(e)}")
        print(f"Error in get_divisions_dropdown: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to load divisions: {str(e)}'})
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@division_bp.route('/divisions', methods=['POST'])
def create_division():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    insert_audit_trail('view_create_division', f"User '{session.get('username')}' viewed create division page.")
    
    conn = None
    cursor = None
    
    try:
        data = request.get_json()
        division_name = data.get('division_name', '').strip()
        created_by = session.get('username')

        if not division_name:
            return jsonify({'success': False, 'message': 'Nama divisi harus diisi'})

        # Validate division name format
        if not re.match(r'^[a-zA-Z0-9\s_-]+$', division_name):
            return jsonify({'success': False, 'message': 'Nama divisi hanya boleh mengandung huruf, angka, spasi, underscore, dan dash'})

        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if division name already exists (case insensitive)
        cursor.execute("""
            SELECT COUNT(*) FROM MasterDivisions 
            WHERE LOWER(division_name) = LOWER(?)
        """, (division_name,))
        
        if cursor.fetchone()[0] > 0:
            return jsonify({'success': False, 'message': f'Nama divisi "{division_name}" sudah ada'})

        # Insert new division
        cursor.execute("""
            INSERT INTO MasterDivisions (division_name, created_by, created_date)
            VALUES (?, ?, GETDATE())
        """, (division_name, created_by))
        
        conn.commit()
        insert_audit_trail('create_division', f"User '{session.get('username')}' created division '{division_name}'.")
        # Log successful creation
        logger.info(f"Division '{division_name}' created successfully by {created_by}")
        
        return jsonify({
            'success': True, 
            'message': f'Divisi "{division_name}" berhasil dibuat',
            'division_name': division_name
        })
        
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"Error creating division: {str(e)}")
        insert_audit_trail('create_division_failed', f"User '{session.get('username')}' failed to create division '{division_name}': {str(e)}")
        return jsonify({'success': False, 'message': f'Terjadi kesalahan: {str(e)}'})
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@division_bp.route('/divisions/<int:division_id>', methods=['DELETE'])
def delete_division(division_id):
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first'})
    insert_audit_trail('view_delete_division', f"User '{session.get('username')}' viewed delete division page for ID {division_id}.")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get division name before deletion
        cursor.execute("SELECT division_name FROM MasterDivisions WHERE id = ?", (division_id,))
        result = cursor.fetchone()
        
        if not result:
            return jsonify({'success': False, 'message': 'Division not found'})
        
        division_name = result[0]
        insert_audit_trail('delete_division', f"User '{session.get('username')}' deleted division '{division_name}'.")
        
        # Delete the division
        cursor.execute("DELETE FROM MasterDivisions WHERE id = ?", (division_id,))
        conn.commit()
        return jsonify({'success': True, 'message': f'Division "{division_name}" deleted successfully'})
        
    except Exception as e:
        insert_audit_trail('delete_division_failed', f"User '{session.get('username')}' failed to delete division ID {division_id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
