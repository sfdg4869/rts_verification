import unittest
from unittest.mock import patch
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.routes.rts_check_routes import _normalize_repo_queries_payload
from app.services import new_repo_check_service as service


class RepoQueryNormalizationTests(unittest.TestCase):
    def test_normalize_repo_queries_payload_legacy_single_repo(self):
        payload = {
            "repo_db_id_list": "255,567",
            "repo_mode": "pol",
            "schema_name": "public",
        }

        repo_queries = _normalize_repo_queries_payload(payload)

        self.assertEqual(
            repo_queries,
            [
                {
                    "slot_id": "repo_a",
                    "source_type": "active_repo",
                    "config_id": None,
                    "direct_config": {},
                    "mode": "pol",
                    "target_mapping": {
                        "db_id_list": "255,567",
                        "schema_name": "public",
                    },
                }
            ],
        )

    def test_normalize_repo_queries_payload_legacy_dual_repo(self):
        payload = {
            "repo_db_id_list": "255",
            "pol_repo_config_id": "cfg-b",
            "pol_repo_mode": "vsql",
            "pol_repo_schema_name": "repo_b_schema",
        }

        repo_queries = _normalize_repo_queries_payload(payload)

        self.assertEqual([item["slot_id"] for item in repo_queries], ["repo_a", "repo_b"])
        self.assertEqual(repo_queries[1]["source_type"], "config")
        self.assertEqual(repo_queries[1]["config_id"], "cfg-b")
        self.assertEqual(repo_queries[1]["mode"], "vsql")
        self.assertEqual(repo_queries[1]["target_mapping"]["schema_name"], "repo_b_schema")

    def test_normalize_repo_queries_payload_preserves_explicit_repo_queries(self):
        payload = {
            "repo_queries": [
                {
                    "slot_id": "repo_b",
                    "source_type": "direct",
                    "direct_config": {"host": "10.0.0.2"},
                    "mode": "pol",
                    "target_mapping": {"schema_name": "schema_b"},
                },
                {
                    "slot_id": "repo_a",
                    "source_type": "active_repo",
                    "mode": "vsql",
                    "target_mapping": {"db_id_list": "101"},
                },
            ]
        }

        repo_queries = _normalize_repo_queries_payload(payload)

        self.assertEqual([item["slot_id"] for item in repo_queries], ["repo_b", "repo_a"])
        self.assertEqual(repo_queries[0]["direct_config"]["host"], "10.0.0.2")
        self.assertEqual(repo_queries[0]["mode"], "pol")
        self.assertEqual(repo_queries[1]["target_mapping"]["db_id_list"], "101")

    def test_normalize_repo_queries_payload_legacy_direct_repo_b(self):
        payload = {
            "pol_repo_direct_config": {
                "host": "10.0.0.5",
                "port": 5432,
                "user": "repo_user",
                "password": "secret",
                "database": "maxgauge",
                "db_type": "postgresql",
            },
            "pol_repo_mode": "vsql",
            "pol_repo_schema_name": "repo_b_schema",
        }

        repo_queries = _normalize_repo_queries_payload(payload)

        self.assertEqual([item["slot_id"] for item in repo_queries], ["repo_a", "repo_b"])
        self.assertEqual(repo_queries[1]["source_type"], "direct")
        self.assertEqual(repo_queries[1]["direct_config"]["host"], "10.0.0.5")
        self.assertEqual(repo_queries[1]["mode"], "vsql")
        self.assertEqual(repo_queries[1]["target_mapping"]["schema_name"], "repo_b_schema")


class Step5RepoOnlyTests(unittest.TestCase):
    def test_run_step5_repo_only_single_repo(self):
        calls = []

        def fake_execute_repo_query_slot(**kwargs):
            calls.append(kwargs["repo_query"])
            return {
                "slot_id": kwargs["repo_query"]["slot_id"],
                "slot_label": "Repo A",
                "engine": "oracle",
                "mode": kwargs["repo_query"]["mode"],
                "schema_name": "",
                "db_id_list": "255",
                "elapse_rows": [{"sql_id": "fbf2t9pw12ynm", "execution_count": 1}],
                "stat_rows": [{"sql_id": "fbf2t9pw12ynm", "execution_count": 1}],
                "error": "",
            }

        with patch.object(service, "_execute_repo_query_slot", side_effect=fake_execute_repo_query_slot):
            result = service.run_step5_repo_only(
                db_id=255,
                repo_queries=[
                    {
                        "slot_id": "repo_a",
                        "source_type": "active_repo",
                        "mode": "pol",
                        "target_mapping": {"db_id_list": "255"},
                    }
                ],
            )

        self.assertEqual(result["overall_status"], "pass")
        self.assertEqual([item["slot_id"] for item in result["data"]["repo_results"]], ["repo_a"])
        self.assertEqual(result["data"]["repo_results"][0]["mode"], "pol")
        self.assertEqual(calls[0]["target_mapping"]["db_id_list"], "255")

    def test_run_step5_repo_only_dual_repo_keeps_slot_order(self):
        calls = []

        def fake_execute_repo_query_slot(**kwargs):
            repo_query = kwargs["repo_query"]
            calls.append(repo_query["slot_id"])
            return {
                "slot_id": repo_query["slot_id"],
                "slot_label": "Repo A" if repo_query["slot_id"] == "repo_a" else "Repo B",
                "engine": "oracle" if repo_query["slot_id"] == "repo_a" else "postgresql",
                "mode": repo_query["mode"],
                "schema_name": repo_query["target_mapping"].get("schema_name") or "",
                "db_id_list": repo_query["target_mapping"].get("db_id_list") or "",
                "elapse_rows": [],
                "stat_rows": [],
                "error": "",
            }

        with patch.object(service, "_execute_repo_query_slot", side_effect=fake_execute_repo_query_slot):
            result = service.run_step5_repo_only(
                db_id=255,
                repo_queries=[
                    {
                        "slot_id": "repo_a",
                        "source_type": "active_repo",
                        "mode": "vsql",
                        "target_mapping": {"db_id_list": "255"},
                    },
                    {
                        "slot_id": "repo_b",
                        "source_type": "config",
                        "config_id": "cfg-b",
                        "mode": "pol",
                        "target_mapping": {"schema_name": "schema_b"},
                    },
                ],
            )

        self.assertEqual(calls, ["repo_a", "repo_b"])
        self.assertEqual([item["slot_id"] for item in result["data"]["repo_results"]], ["repo_a", "repo_b"])
        self.assertEqual(result["data"]["repo_results"][1]["mode"], "pol")
        self.assertEqual(result["data"]["repo_results"][1]["schema_name"], "schema_b")


if __name__ == "__main__":
    unittest.main()
