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

from aether_sdk import (  # noqa: E402
    AetherClient,
    make_artifact_reference,
    make_datom,
    make_policy,
    make_policy_context,
    make_vector_record,
    value_string,
)


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

        policy_document = """
schema {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  protected_fact(Entity)
  visible_task(Entity)
}

rules {
  visible_task(t) <- task_status(t, "ready")
  visible_task(t) <- protected_fact(t)
}

materialize {
  visible_task
}

facts {
  protected_fact(entity(1))
  protected_fact(entity(2)) @capability("executor")
}

query current_cut {
  current
  goal visible_task(t)
  keep t
}
"""
        client.append(
            [
                make_datom(
                    entity=1,
                    attribute=1,
                    value=value_string("ready"),
                    element=1,
                ),
                make_datom(
                    entity=3,
                    attribute=1,
                    value=value_string("ready"),
                    element=2,
                    policy=make_policy(capability="executor"),
                ),
            ]
        )

        default_policy_run = client.run_document(policy_document)
        self.assertEqual(
            [row["values"] for row in default_policy_run["query"]["rows"]],
            [[{"Entity": 1}]],
        )

        executor_policy_run = client.run_document(
            policy_document,
            policy_context=make_policy_context(capabilities=["executor"]),
        )
        self.assertEqual(
            [row["values"] for row in executor_policy_run["query"]["rows"]],
            [[{"Entity": 1}], [{"Entity": 2}], [{"Entity": 3}]],
        )

        client.append(
            [
                make_datom(
                    entity=1,
                    attribute=1,
                    value=value_string("sidecar-anchor-1"),
                    element=3,
                    op="Annotate",
                ),
            ]
        )

        client.register_artifact_reference(
            make_artifact_reference(
                sidecar_id="semantic-memory",
                artifact_id="doc-1",
                entity=41,
                uri="s3://aether/docs/doc-1.md",
                media_type="text/markdown",
                byte_length=256,
                digest="sha256:doc-1",
                metadata={"kind": {"String": "runbook"}},
                registered_at=3,
                policy=make_policy(capability="memory_reader"),
            )
        )
        client.append(
            [
                make_datom(
                    entity=1,
                    attribute=1,
                    value=value_string("sidecar-anchor-2"),
                    element=4,
                    op="Annotate",
                )
            ]
        )
        client.register_vector_record(
            record=make_vector_record(
                sidecar_id="semantic-memory",
                vector_id="vec-1",
                entity=41,
                source_artifact_id="doc-1",
                embedding_ref="s3://aether/vectors/vec-1.bin",
                dimensions=3,
                metric="cosine",
                metadata={"topic": {"String": "handoff"}},
                registered_at=4,
                policy=make_policy(capability="memory_reader"),
            ),
            embedding=[0.9, 0.1, 0.0],
        )

        with self.assertRaisesRegex(Exception, "policy denied"):
            client.get_artifact_reference(
                sidecar_id="semantic-memory",
                artifact_id="doc-1",
            )

        artifact = client.get_artifact_reference(
            sidecar_id="semantic-memory",
            artifact_id="doc-1",
            policy_context=make_policy_context(capabilities=["memory_reader"]),
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
                "as_of": 4,
                "projection": {
                    "predicate": {
                        "id": 81,
                        "name": "semantic_neighbor",
                        "arity": 3,
                    },
                    "query_entity": 999,
                },
                "policy_context": make_policy_context(capabilities=["memory_reader"]),
            }
        )
        self.assertEqual(len(search["matches"]), 1)
        self.assertEqual(len(search["facts"]), 1)
        self.assertEqual(
            search["facts"][0]["provenance"]["source_datom_ids"],
            [4, 3],
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
