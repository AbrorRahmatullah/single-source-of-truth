from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from flask_bcrypt import Bcrypt
from config.config import get_db_connection
from models.user import get_user_by_username
from models.audit import insert_audit_trail
from utils.validation import validate_password_strength

auth_bp = Blueprint('auth', __name__)
bcrypt = Bcrypt()

@auth_bp.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = get_user_by_username(username)
        if user and bcrypt.check_password_hash(user[0], password):
            session.update({
                'username': username,
                'division': user[2],
                'fullname': user[3],
                'role_access': user[1],
                'upload_done': True
            })
            insert_audit_trail('login', f"User '{username}' logged in.")
            if user[1].lower() == 'admin':
                return redirect(url_for('summary.summary_page'))
            return redirect(url_for('upload.upload_file'))
        flash('Invalid username or password.')
    return render_template('login.html')

@auth_bp.route('/change_password', methods=['GET', 'POST'])
def change_password():
    conn = get_db_connection()
    cur = conn.cursor()
        
    if 'username' not in session:
        flash("You need to log in first.")
        return redirect(url_for('auth.login'))
    
    role_access = session.get('role_access')
    fullname = session.get('fullname')
    username = session.get('username')
    division = session.get('division')
    
    if request.method == 'GET':
        insert_audit_trail('view_change_password', f"User '{session.get('username')}' viewed change password page.")
        return render_template(
            'change_password.html',
            username=username,
            division=division,
            role_access=role_access,
            fullname=fullname
        )

    elif request.method == 'POST':
        current_password = request.form['current_password']
        new_password = request.form['new_password']
        password_confirm = request.form['password_confirm']
        username = session['username']

        # Validasi password confirmation
        if new_password != password_confirm:
            return render_template(
                'change_password.html',
                username=username,
                division=division,
                role_access=role_access,
                fullname=fullname,
                error_confirm="Passwords do not match.",
                current_password=current_password,
                new_password=new_password,
                password_confirm=password_confirm
            )

        # Validasi password strength
        is_valid, error_message = validate_password_strength(new_password)
        if not is_valid:
            return render_template(
                'change_password.html',
                username=username,
                division=division,
                role_access=role_access,
                fullname=fullname,
                error_strength=error_message,
                current_password=current_password,
                new_password=new_password,
                password_confirm=password_confirm
            )

        # Fetch the current hashed password from the database
        cur.execute("SELECT password_hash FROM MasterUsers WHERE username = ?", (username,))
        user = cur.fetchone()

        if not user or not bcrypt.check_password_hash(user[0], current_password):
            return render_template(
                'change_password.html',
                username=username,
                division=division,
                role_access=role_access,
                fullname=fullname,
                error_current="Current password is incorrect.",
                current_password=current_password,
                new_password=new_password,
                password_confirm=password_confirm
            )
            
        else:
            # Hash the new password and update the database
            new_password_hash = bcrypt.generate_password_hash(new_password).decode('utf-8')
            cur.execute("UPDATE MasterUsers SET password_hash = ? WHERE username = ?", (new_password_hash, username))
            conn.commit()
            insert_audit_trail('change_password', f"User '{session.get('username')}' changed password.")
            return '''
                <script>
                    alert("Perubahan password berhasil dilakukan.");
                    window.location.href = "{}";
                </script>
            '''.format(url_for('upload.upload_file'))


@auth_bp.route('/logout')
def logout():
    insert_audit_trail('logout', f"User '{session.get('username')}' logged out.")
    session.pop('username', None)
    flash("You have been logged out.")
    return redirect(url_for('auth.login'))
