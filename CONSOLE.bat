@echo off
set VENV_DIR="./venv"

if not exist %VENV_DIR%\Scripts\Activate.bat (
    echo ОШИБКА: Виртуальное окружение не найдено
    echo Создайте виртуальное окружение командой: python -m venv venv
    pause
    exit /b 1
)

if not exist input.xlsx (
    echo ОШИБКА: Файл input.xlsx не найден
    echo Убедитесь, что файл находится в текущей директории
    pause
    exit /b 1
)

call %VENV_DIR%\Scripts\Activate.bat

echo Запуск обработки адресов...
python main.py -v -s -i "input.xlsx" -isn "Sheet 1" -icn "Address" -o "output.xlsx"

if errorlevel 1 (
    echo Произошла ошибка при выполнении программы
    pause
    exit /b 1
)

echo Обработка завершена успешно
pause