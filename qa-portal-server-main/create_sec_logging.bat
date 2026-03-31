@echo off
chcp 65001
echo.
echo ==========================================
echo   sec_logging.json 로컬 생성 도구
echo ==========================================
echo.

REM Flask 서버 실행 상태 확인
echo 🔄 서버 연결 확인 중...
curl -s http://localhost:5000/ >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Flask 서버가 실행되지 않았습니다.
    echo.
    echo 💡 해결 방법:
    echo    1. 새 터미널에서 'python app.py' 실행
    echo    2. 서버 시작 후 이 스크립트 다시 실행
    echo.
    pause
    exit /b 1
)

echo ✅ 서버 연결 성공
echo.

REM Python 스크립트 실행
echo 🚀 sec_logging.json 생성 중...
python create_sec_logging_local.py

echo.
pause
