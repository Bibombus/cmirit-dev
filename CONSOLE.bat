@echo off
set VENV_DIR="./venv"

call %VENV_DIR%/Scripts/Activate.bat

start python main.py --verbose