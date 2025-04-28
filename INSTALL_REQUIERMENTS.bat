set VENV_DIR="./venv"

py -3.10 -m venv %VENV_DIR%

call %VENV_DIR%/Scripts/Activate.bat

call pip install -r requierments.txt

call deactivate

pause