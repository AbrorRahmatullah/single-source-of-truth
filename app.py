from flask import Flask, session, redirect, url_for, flash, jsonify, request
# from flask_wtf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from datetime import datetime, timedelta
import logging
import os

from routes.auth_routes import auth_bp
from routes.upload_routes import upload_bp
from routes.template_routes import template_bp
from routes.data_routes import data_bp
from routes.table_routes import table_bp
from routes.summary_routes import summary_bp
from routes.audit_trails_routes import audit_trails_bp
from routes.debitur_routes import debitur_bp
from routes.division_routes import division_bp
from routes.user_routes import user_bp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Flask App Setup
app = Flask(__name__)
# csrf = CSRFProtect(app)

# Rate limiting
limiter = Limiter(get_remote_address, app=app, default_limits=["200 per day", "50 per hour"])

app.config.update(
    UPLOAD_FOLDER='uploads',
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,
)

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'unsafe-default-key')

# Logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'app.log'),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("Application starting...")

# Middleware: Session Timeout
@app.before_request
def session_timeout_middleware():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=30)

    now = datetime.now()
    last_activity = session.get('last_activity')
    if last_activity:
        try:
            last_activity_time = datetime.strptime(last_activity, "%Y-%m-%d %H:%M:%S")
            if now - last_activity_time > timedelta(minutes=30):
                session.clear()
                flash("Sesi Anda telah berakhir karena tidak ada aktivitas. Silakan login kembali.")
                logger.info("Session timeout for user.")
                return redirect(url_for('auth.login'))
        except Exception:
            session.clear()
            return redirect(url_for('auth.login'))

    session['last_activity'] = now.strftime("%Y-%m-%d %H:%M:%S")

# Global error handlers
@app.errorhandler(404)
def not_found_error(error):
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({'success': False, 'message': 'Endpoint tidak ditemukan (404).'}), 404
    return "<h3>404 - Halaman tidak ditemukan.</h3>", 404

@app.errorhandler(500)
def internal_error(error):
    logger.exception("Internal Server Error: %s", error)
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({'success': False, 'message': 'Terjadi kesalahan internal server.'}), 500
    return "<h3>500 - Kesalahan internal server. Silakan hubungi admin.</h3>", 500

@app.errorhandler(Exception)
def unhandled_exception(e):
    logger.exception("Unhandled exception: %s", e)
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({'success': False, 'message': str(e)}), 500
    return "<h3>Terjadi kesalahan tak terduga.</h3>", 500

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(template_bp)
app.register_blueprint(data_bp)
app.register_blueprint(table_bp)
app.register_blueprint(summary_bp)
app.register_blueprint(audit_trails_bp)
app.register_blueprint(debitur_bp)
app.register_blueprint(division_bp)
app.register_blueprint(user_bp)


if __name__ == "__main__":
    from waitress import serve
    # logger.info("Starting Waitress server on port 5000...")
    # serve(app, host='0.0.0.0', port=5000)
    app.run(debug=True)
