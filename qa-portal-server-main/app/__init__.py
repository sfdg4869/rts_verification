from flask import Flask, redirect, send_file
from flasgger import Swagger
from flask_cors import CORS
import os

from app.routes import blueprints


_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


SWAGGER_TEMPLATE = {
    "info": {
        "title": "QS1 Portal API",
        "version": "1.0.0",
        "description": "RTS Check API and MongoDB-backed DB configuration helpers.",
    },
    "tags": [
        {"name": "MongoDB Configuration", "description": "Repo DB connection CRUD (MongoDB)"},
        {"name": "DB Selection", "description": "Select Target/Repo DB from MongoDB"},
        {"name": "DB Selector", "description": "DB selector (UI + API)"},
        {"name": "RTS Check", "description": "RTS process status checks over SSH"},
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
    from app.shared_db import is_repo_db_configured, is_target_db_configured

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
    """Initialize python-oracledb once and log whether Thick mode was enabled."""
    import oracledb

    lib_dir = (os.environ.get("ORACLE_CLIENT_LIB_DIR") or "").strip()
    search_target = lib_dir or "(default search path)"

    try:
        if lib_dir and os.path.isdir(lib_dir):
            print(f"[INFO] Oracle client init requested. ORACLE_CLIENT_LIB_DIR={lib_dir}")
            oracledb.init_oracle_client(lib_dir=lib_dir)
        else:
            if lib_dir:
                print(
                    f"[WARNING] ORACLE_CLIENT_LIB_DIR does not exist: {lib_dir}. "
                    "Falling back to default Oracle client search path."
                )
            else:
                print("[INFO] ORACLE_CLIENT_LIB_DIR not set. Using default Oracle client search path.")
            oracledb.init_oracle_client()

        mode = "Thin" if oracledb.is_thin_mode() else "Thick"
        print(f"[OK] Oracle client initialized. mode={mode}, ORACLE_CLIENT_LIB_DIR={search_target}")
    except Exception as exc:
        if "DPY-3015" in str(exc):
            mode = "Thin" if oracledb.is_thin_mode() else "Thick"
            print(f"[INFO] Oracle client already initialized. mode={mode}, ORACLE_CLIENT_LIB_DIR={search_target}")
        else:
            print(
                f"[WARNING] Oracle client init failed; continuing in Thin mode. "
                f"ORACLE_CLIENT_LIB_DIR={search_target}, error={exc}"
            )


def create_app():
    """Create Flask app and register CORS, Swagger, and all blueprints."""
    _init_oracle_client_once()
    app = Flask(__name__)
    CORS(app)
    Swagger(app, template=SWAGGER_TEMPLATE, config=SWAGGER_CONFIG)

    _auto_load_db_setup()

    for bp in blueprints:
        app.register_blueprint(bp)

    @app.route("/")
    def _root():
        return redirect("/rts-check")

    @app.route("/rts-check")
    def _rts_check_ui():
        return send_file(os.path.join(_REPO_ROOT, "rts_check.html"))

    return app
