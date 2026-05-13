@echo off
REM Stop the GENRRI Trading View server (whatever is listening on port 5001).
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5001 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
    echo Stopped GENRRI server PID %%a.
    goto :done
)
echo GENRRI server was not running.
:done
timeout /t 2 /nobreak >nul
exit