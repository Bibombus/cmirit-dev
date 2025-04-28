@echo off
set VENV_DIR="./venv"

call %VENV_DIR%/Scripts/Activate.bat

start pythonw main.py --gui