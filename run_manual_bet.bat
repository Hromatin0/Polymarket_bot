@echo off
chcp 65001 >nul
title Manual Polymarket Bet (via VPN)

echo.
echo ════════════════════════════════════════════════════════════════
echo   MANUAL BET  ·  Polymarket Dota 2
echo   Python 3.14.4  (via Split Tunnel VPN)
echo ════════════════════════════════════════════════════════════════
echo.

set PYTHON_PATH=C:\Users\dinis\AppData\Local\Python\pythoncore-3.14-64\python.exe

if not exist "%PYTHON_PATH%" (
    echo [ERROR] Python 3.14.4 not found!
    echo Path: %PYTHON_PATH%
    echo.
    pause
    exit /b 1
)

echo Python found: %PYTHON_PATH%
echo.
echo How to use:
echo   1. Paste token_id and press Enter
echo   2. (Optional) Enter USD amount after the token separated by space
echo.
echo Examples:
echo   0x1234567890abcdef1234567890abcdef12345678
echo   0x1234567890abcdef1234567890abcdef12345678 12.5
echo.

:loop
echo ────────────────────────────────────────────────────────────────
set /p "input=Token ID (or empty string to exit): "

if "%input%"=="" (
    echo.
    echo Exiting...
    goto :end
)

:: Run script with entered data
"%PYTHON_PATH%" manual_bet.py %input%

echo.
echo Press any key for a new bet...
pause >nul
goto :loop

:end
echo.
echo Program completed.
pause