# app.py — 로컬 개발용 진입점 (라우트는 app.create_app 에 등록됨)
from app import create_app

app = create_app()

if __name__ == '__main__':
    print('RTS Check UI:  http://localhost:5000/rts-check')
    print('Swagger UI:    http://localhost:5000/apidocs')
    app.run(debug=True)
