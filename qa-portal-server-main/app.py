# app.py
from app import create_app
from flask import send_file
import os

app = create_app()

@app.route('/rts-check')
def rts_check_ui():
    return send_file(os.path.join(os.path.dirname(__file__), 'rts_check.html'))

if __name__ == '__main__':
    print('RTS Check UI:  http://localhost:5000/rts-check')
    print('Swagger UI:    http://localhost:5000/apidocs')
    app.run(debug=True)
