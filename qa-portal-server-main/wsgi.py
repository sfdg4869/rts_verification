"""WSGI entry (Docker / gunicorn). 패키지 `app`과 루트 `app.py` 이름 충돌을 피하기 위해 별도 모듈 사용."""
from app import create_app

application = create_app()
