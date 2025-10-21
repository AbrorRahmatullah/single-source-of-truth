import os

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['xlsx', 'xls']

def ensure_upload_folder(path):
    os.makedirs(path, exist_ok=True)
