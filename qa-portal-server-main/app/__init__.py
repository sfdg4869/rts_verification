
# Flask app factory and route bootstrap
from flask import Flask
from flasgger import Swagger
from flask_cors import CORS
import os

# Blueprint registry
from app.routes import blueprints


SWAGGER_TEMPLATE = {
    "info": {
        "title": "QS1 Portal API",
        "version": "1.0.0",
        "description": "RTS Check API — MaxGauge RTS 데몬 상태 점검 + MongoDB DB 설정 관리",
    },
    "tags": [
        {"name": "MongoDB Configuration", "description": "Repo DB 접속 정보 CRUD (MongoDB)"},
        {"name": "DB Selection", "description": "MongoDB에서 Target/Repo DB 선택"},
        {"name": "DB Selector", "description": "DB 선택기 (UI + API)"},
        {"name": "RTS Check", "description": "RTS 프로세스 상태 SSH 점검"},
    ],
}


def _api_rule(rule):
    return "/api/" in rule.rule or "/rts/" in rule.rule


SWAGGER_CONFIG = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec_all",
            "route": "/apispec_all.json",
            "rule_filter": _api_rule,
            "model_filter": lambda tag: True,
        },
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/",
}


def _auto_load_db_setup():
	"""Check DB setup status at startup and print guidance."""
	from app.shared_db import is_target_db_configured, is_repo_db_configured

	if is_target_db_configured() and is_repo_db_configured():
		print("[OK] DB setup is already loaded.")
		return

	setup_file = "db_setup.json"
	if os.path.exists(setup_file):
		print("[INFO] db_setup.json exists.")
		print("[SETUP] Use /rts/db/setup API from Swagger UI to configure DB.")
		print("[URL] Swagger UI: http://localhost:5000/apidocs")
	else:
		print("[WARNING] db_setup.json not found.")
		print("[SETUP] Configure DB connection first.")
		print("   1. Create db_setup.json")
		print("   2. Or call /rts/db/setup API directly")


def _init_oracle_client_once():
	"""Oracle Thick 모드 초기화 (프로세스 당 1회)."""
	import oracledb
	try:
		oracledb.init_oracle_client()
	except Exception as e:
		if "DPY-3015" not in str(e):
			print(f"[WARNING] Oracle client init failed: {e}")


def create_app():
	"""
	Create Flask app and register CORS, Swagger and all blueprints.
	"""
	_init_oracle_client_once()
	app = Flask(__name__)
	CORS(app)
	Swagger(app, template=SWAGGER_TEMPLATE, config=SWAGGER_CONFIG)

	_auto_load_db_setup()

	# Register all blueprints
	for bp in blueprints:
		app.register_blueprint(bp)

	return app
