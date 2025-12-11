@echo off
REM Aktifkan virtual environment
call C:\Users\sstap01dwh\Documents\Aplikasi\single-source-of-truth\venv\Scripts\activate

REM Jalankan waitress-serve
waitress-serve --host=127.0.0.1 --port=8000 app:app > run.log 2>&1