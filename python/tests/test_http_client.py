from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from urllib import error, request


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_ROOT = REPO_ROOT / "python"
sys.path.insert(0, str(PYTHON_ROOT))

from aether_sdk import AetherClient  # noqa: E402


@unittest.skipUnless(shutil.which("cargo"), "cargo is required for HTTP client integration tests")
class AetherHttpClientIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._port = cls._free_port()
        cls._base_url = f"http://127.0.0.1:{cls._port}"
        cls._server = subprocess.Popen(
            [
                "cargo",
                "run",
                "-p",
                "aether_api",
                "--example",
                "http_kernel_service",
                "--",
                f"127.0.0.1:{cls._port}",
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ.copy(),
        )
        cls._wait_for_server(cls._base_url, cls._server)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._server.terminate()
        try:
            cls._server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls._server.kill()
            cls._server.wait(timeout=10)

    def test_client_runs_documents_and_sidecar_queries(self) -> None:
        client = AetherClient(self._base_url)

        self.assertEqual(client.health()["status"], "ok")

        document = """
schema {
}

predicates {
  task(Entity)
  ready(Entity)
}

rules {
  ready(x) <- task(x)
}

materialize {
  ready
}

facts {
  task(entity(1))
}

query current_cut {
  current
  goal ready(x)
  keep x
}
"""
        run_response = client.run_document(document)
        self.assertEqual(
            run_response["query"]["rows"][0]["values"],
            [{"Entity": 1}],
        )

        client.append(
            [
                {
                    "entity": 1,
                    "attribute": 1,
                    "value": {"String": "sidecar-anchor-1"},
                    "op": "Annotate",
                    "element": 1,
                    "replica": 1,
                    "causal_context": {"frontier": []},
                    "provenance": {
                        "author_principal": "",
                        "agent_id": "",
                        "tool_id": "",
                        "session_id": "",
                        "source_ref": {"uri": "", "digest": None},
                        "parent_datom_ids": [],
                        "confidence": 1.0,
                        "trust_domain": "",
                        "schema_version": "",
                    },
                    "policy": None,
                },
            ]
        )

        client.register_artifact_reference(
            {
                "sidecar_id": "semantic-memory",
                "artifact_id": "doc-1",
                "entity": 41,
                "uri": "s3://aether/docs/doc-1.md",
                "media_type": "text/markdown",
                "byte_length": 256,
                "digest": "sha256:doc-1",
                "metadata": {"kind": {"String": "runbook"}},
                "provenance": {
                    "author_principal": "",
                    "agent_id": "",
                    "tool_id": "",
                    "session_id": "",
                    "source_ref": {"uri": "", "digest": None},
                    "parent_datom_ids": [],
                    "confidence": 1.0,
                    "trust_domain": "",
                    "schema_version": "",
                },
                "policy": None,
                "registered_at": 1,
            }
        )
        client.append(
            [
                {
                    "entity": 1,
                    "attribute": 1,
                    "value": {"String": "sidecar-anchor-2"},
                    "op": "Annotate",
                    "element": 2,
                    "replica": 1,
                    "causal_context": {"frontier": []},
                    "provenance": {
                        "author_principal": "",
                        "agent_id": "",
                        "tool_id": "",
                        "session_id": "",
                        "source_ref": {"uri": "", "digest": None},
                        "parent_datom_ids": [],
                        "confidence": 1.0,
                        "trust_domain": "",
                        "schema_version": "",
                    },
                    "policy": None,
                }
            ]
        )
        client.register_vector_record(
            record={
                "sidecar_id": "semantic-memory",
                "vector_id": "vec-1",
                "entity": 41,
                "source_artifact_id": "doc-1",
                "embedding_ref": "s3://aether/vectors/vec-1.bin",
                "dimensions": 3,
                "metric": "cosine",
                "metadata": {"topic": {"String": "handoff"}},
                "provenance": {
                    "author_principal": "",
                    "agent_id": "",
                    "tool_id": "",
                    "session_id": "",
                    "source_ref": {"uri": "", "digest": None},
                    "parent_datom_ids": [],
                    "confidence": 1.0,
                    "trust_domain": "",
                    "schema_version": "",
                },
                "policy": None,
                "registered_at": 2,
            },
            embedding=[0.9, 0.1, 0.0],
        )

        artifact = client.get_artifact_reference(
            sidecar_id="semantic-memory",
            artifact_id="doc-1",
        )
        self.assertEqual(
            artifact["reference"]["uri"],
            "s3://aether/docs/doc-1.md",
        )

        search = client.search_vectors(
            {
                "sidecar_id": "semantic-memory",
                "query_embedding": [1.0, 0.0, 0.0],
                "top_k": 1,
                "metric": "cosine",
                "as_of": 2,
                "projection": {
                    "predicate": {
                        "id": 81,
                        "name": "semantic_neighbor",
                        "arity": 3,
                    },
                    "query_entity": 999,
                },
            }
        )
        self.assertEqual(len(search["matches"]), 1)
        self.assertEqual(len(search["facts"]), 1)
        self.assertEqual(
            search["facts"][0]["provenance"]["source_datom_ids"],
            [2, 1],
        )

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _wait_for_server(base_url: str, server: subprocess.Popen[str]) -> None:
        deadline = time.time() + 90.0
        while time.time() < deadline:
            if server.poll() is not None:
                output = server.stdout.read() if server.stdout else ""
                raise RuntimeError(f"AETHER test server exited early:\n{output}")
            try:
                with request.urlopen(f"{base_url}/health", timeout=1.0) as response:
                    if response.status == 200:
                        return
            except (error.URLError, TimeoutError):
                time.sleep(1.0)

        output = server.stdout.read() if server.stdout else ""
        raise RuntimeError(f"AETHER test server did not become ready:\n{output}")


if __name__ == "__main__":
    unittest.main()
