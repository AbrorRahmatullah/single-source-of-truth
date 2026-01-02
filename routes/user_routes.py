from flask_bcrypt import Bcrypt
from flask import Blueprint, flash, render_template, request, jsonify, session, redirect, url_for
from datetime import datetime
import logging

from models.audit import insert_audit_trail
from config.config import get_db_connection

user_bp = Blueprint('user', __name__)
logger = logging.getLogger(__name__)
bcrypt = Bcrypt()

@user_bp.route('/users', methods=['GET', 'POST'])
def handle_users():
    if 'username' not in session:
        flash('Please log in first.')
        return redirect(url_for('auth.login'))

    if request.method == 'GET':
        # Jika browser meminta HTML (bukan fetch/ajax)
        if request.headers.get('Accept', '').startswith('text/html'):
            insert_audit_trail('view_users_page', f"User '{session.get('username')}' viewed users management page.")
            return render_template(
                'users_management.html',
                username=session.get('username'),
                fullname=session.get('fullname'),
                division=session.get('division'),
                role_access=session.get('role_access')
            )

        # Jika permintaan fetch() dari JavaScript, balas JSON
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, username, role_access, 
                    fullname, email, division
                FROM MasterUsers
                ORDER BY created_date DESC
            """)
            
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            users = [dict(zip(columns, row)) for row in rows]
            insert_audit_trail('view_users', f"User '{session.get('username')}' viewed user list.")

            return jsonify({'success': True, 'users': users})

        except Exception as e:
            return jsonify({'success': False, 'message': str(e)})

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    elif request.method == 'POST':
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        password_confirm = data.get('password_confirm')
        role_access = data.get('role_access')
        fullname = data.get('fullname')
        email = data.get('email')
        division = data.get('division')
        created_date = datetime.now()

        required_fields = [username, password, password_confirm, role_access, fullname, email, division]
        if not all(required_fields):
            return jsonify({'success': False, 'message': 'Please fill in all fields.'})

        if password != password_confirm:
            return jsonify({'success': False, 'message': 'Passwords do not match.'})

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute("SELECT id FROM MasterUsers WHERE username = ?", (username,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'Username already exists.'})

            cur.execute("SELECT id FROM MasterUsers WHERE email = ?", (email,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'Email is already registered.'})

            password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

            cur.execute("""
                INSERT INTO MasterUsers (username, password_hash, role_access, fullname, email, division, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (username, password_hash, role_access, fullname, email, division, created_date))

            conn.commit()
            insert_audit_trail('create_user', f"User '{session.get('username')}' created new user '{username}'.")
            return jsonify({
                'success': True, 
                'message': f'User "{username}" created successfully.'
                })

        except Exception as e:
            insert_audit_trail('create_user_failed', f"User '{session.get('username')}' failed to create new user '{username}': {str(e)}")
            return jsonify({'success': False, 'message': str(e)})

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

@user_bp.route('/users/<int:id>', methods=['GET'])
def get_user_by_id(id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, username, fullname, email, division, role_access
            FROM MasterUsers WHERE id = ?
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'success': False, 'message': 'User not found'})
        user = dict(zip([desc[0] for desc in cur.description], row))
        insert_audit_trail('get_user', f"User '{session.get('username')}' accessed details for user ID {id}.")
        return jsonify({'success': True, 'user': user})
    except Exception as e:
        insert_audit_trail('get_user_failed', f"User '{session.get('username')}' failed to access details for user ID {id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()
        conn.close()

@user_bp.route('/users/<int:id>', methods=['PUT'])
def update_user(id):
    if 'username' not in session:
        flash('Please log in first.')
        return redirect(url_for('auth.login'))
    insert_audit_trail('view_edit_user', f"User '{session.get('username')}' viewed edit page for user ID {id}.")
    
    try:
        data = request.get_json()

        username = data.get('username')
        fullname = data.get('fullname')
        email = data.get('email')
        division = data.get('division')
        role_access = data.get('role_access')

        if not all([username, fullname, email, division, role_access]):
            return jsonify({'success': False, 'message': 'All fields are required'})

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if user exists
        cursor.execute("SELECT id FROM MasterUsers WHERE id = ?", (id,))
        if not cursor.fetchone():
            return jsonify({'success': False, 'message': 'User not found'})

        # Check if username is taken by another user
        cursor.execute("SELECT id FROM MasterUsers WHERE username = ? AND id != ?", (username, id))
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'Username already exists'})

        # Check if email is taken by another user
        cursor.execute("SELECT id FROM MasterUsers WHERE email = ? AND id != ?", (email, id))
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'Email is already registered'})

        cursor.execute("""
            UPDATE MasterUsers
            SET username = ?, fullname = ?, email = ?, division = ?, role_access = ?
            WHERE id = ?
        """, (username, fullname, email, division, role_access, id))

        conn.commit()
        insert_audit_trail('update_user', f"User '{session.get('username')}' updated user ID {id}.")
        return jsonify({
            'success': True, 
            'message': f'User "{username}" updated successfully'
        })

    except Exception as e:
        insert_audit_trail('update_user_failed', f"User '{session.get('username')}' failed to update user ID {id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@user_bp.route('/users/<int:id>', methods=['DELETE'])
def delete_user(id):
    if 'username' not in session:
        flash('Please log in first.')
        return redirect(url_for('auth.login'))
    insert_audit_trail('view_delete_user', f"User '{session.get('username')}' viewed delete page for user ID {id}.")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get division name before deletion
        cursor.execute("SELECT username FROM MasterUsers WHERE id = ?", (id,))
        result = cursor.fetchone()
        
        if not result:
            return jsonify({'success': False, 'message': 'User not found'})
        
        username = result[0]
        
        # Delete the division
        cursor.execute("DELETE FROM MasterUsers WHERE id = ?", (id,))
        conn.commit()
        
        insert_audit_trail('delete_user', f"User '{session.get('username')}' deleted user id {id}.")
        return jsonify({'success': True, 'message': f'User: "{username}" deleted successfully'})
        
    except Exception as e:
        insert_audit_trail('delete_user_failed', f"User '{session.get('username')}' failed to delete user id {id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Tambahkan route baru untuk validasi username
@user_bp.route('/users/check-username', methods=['POST'])
def check_username():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    data = request.get_json()
    username = data.get('username')
    user_id = data.get('user_id')  # Optional, untuk edit user
    
    if not username:
        return jsonify({'success': False, 'message': 'Username is required'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if user_id:
            # Untuk edit user - cek username selain user yang sedang diedit
            cursor.execute("SELECT id FROM MasterUsers WHERE username = ? AND id != ?", (username, user_id))
        else:
            # Untuk create user baru
            cursor.execute("SELECT id FROM MasterUsers WHERE username = ?", (username,))
        
        exists = cursor.fetchone() is not None
        insert_audit_trail('check_username', f"User '{session.get('username')}' checked username availability for '{username}'.")
        return jsonify({
            'success': True,
            'exists': exists,
            'message': 'Username already exists' if exists else 'Username available'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@user_bp.route('/users/check-email', methods=['POST'])
def check_email():
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})
    
    data = request.get_json()
    email = data.get('email')
    user_id = data.get('user_id')  # Optional, untuk edit user
    
    if not email:
        return jsonify({'success': False, 'message': 'Email is required'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if user_id:
            # Untuk edit user - cek email selain user yang sedang diedit
            cursor.execute("SELECT id FROM MasterUsers WHERE email = ? AND id != ?", (email, user_id))
        else:
            # Untuk create user baru
            cursor.execute("SELECT id FROM MasterUsers WHERE email = ?", (email,))
        
        exists = cursor.fetchone() is not None
        insert_audit_trail('check_email', f"User '{session.get('username')}' checked email availability for '{email}'.")
        return jsonify({
            'success': True,
            'exists': exists,
            'message': 'Email already registered' if exists else 'Email available'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@user_bp.route('/users/<int:id>/reset-password', methods=['POST'])
def reset_password(id):
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Please log in first.'})

    data = request.get_json()
    new_password = data.get('new_password')

    if not new_password:
        return jsonify({'success': False, 'message': 'Password is required'})

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if user exists
        cursor.execute("SELECT username FROM MasterUsers WHERE id = ?", (id,))
        result = cursor.fetchone()

        if not result:
            return jsonify({'success': False, 'message': 'User not found'})

        username = result[0]

        # Generate hashed password
        password_hash = bcrypt.generate_password_hash(new_password).decode('utf-8')

        # Update password
        cursor.execute("""
            UPDATE MasterUsers
            SET password_hash = ?
            WHERE id = ?
        """, (password_hash, id))

        conn.commit()
        insert_audit_trail('reset_password', f"User '{session.get('username')}' reset password for user '{username}'.")
        return jsonify({
            'success': True,
            'message': f'Password for user "{username}" has been reset successfully'
        })

    except Exception as e:
        insert_audit_trail('reset_password_failed', f"User '{session.get('username')}' failed to reset password for user ID {id}: {str(e)}")
        return jsonify({'success': False, 'message': 'Error resetting password'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
