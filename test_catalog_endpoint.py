#!/usr/bin/env python3
"""The client-side catalog loader (seed_tpcds.load_rest_catalog) must hand out
FileIOs on the CLIENT's S3 endpoint, never the one the catalog advertises.

A co-located node (CATALOG_CHOICE=1) runs Nessie beside the gateway; its REST
config advertises the container address s3.endpoint=http://minioserver:9000,
which does not resolve on the host. `blimp --register` then failed 0/24
("Could not connect to the endpoint URL: http://minioserver:9000/tpcds-mv/…").

A fake Iceberg REST server advertises minioserver in all three places pyiceberg
reads it from: /v1/config overrides, the table metadata properties and the
loadTable config. Needs pyiceberg (the kit venv):

    ~/.blimp_venv/bin/python3 -m unittest test_catalog_endpoint -v
"""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import seed_tpcds as S

ADVERTISED = "http://minioserver:9000"
CLIENT = "http://127.0.0.1:9000"

METADATA = {
    "format-version": 2, "table-uuid": "8f31582f-0000-0000-0000-000000000000",
    "location": "s3://tpcds-mv/tpcds_clean/date_dim", "last-sequence-number": 0,
    "last-updated-ms": 0, "last-column-id": 1, "current-schema-id": 0,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": [
        {"id": 1, "name": "d_date_sk", "required": False, "type": "long"}]}],
    "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 999, "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {"s3.endpoint": ADVERTISED},
    "snapshots": [], "snapshot-log": [], "metadata-log": [],
}


class FakeRest(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, body):
        b = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/v1/config"):
            return self._json({"defaults": {}, "overrides": {
                "s3.endpoint": ADVERTISED, "s3.access-key-id": "SERVERKEY",
                "s3.secret-access-key": "SERVERSECRET", "s3.session-token": "SERVERTOKEN"}})
        if "/tables/" in self.path:
            return self._json({
                "metadata-location": "s3://tpcds-mv/tpcds_clean/date_dim/metadata/00000.metadata.json",
                "metadata": METADATA, "config": {"s3.endpoint": ADVERTISED}})
        self.send_response(404)
        self.end_headers()


class TestClientEndpointWins(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), FakeRest)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.uri = "http://127.0.0.1:%d" % cls.srv.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_plain_restcatalog_takes_the_advertised_endpoint(self):
        # The failure mechanism in the installed pyiceberg: the server wins.
        from pyiceberg.catalog.rest import RestCatalog
        cat = RestCatalog("kit", uri=self.uri, **{"s3.endpoint": CLIENT})
        self.assertEqual(cat.properties["s3.endpoint"], ADVERTISED)
        self.assertEqual(cat.load_table("ns.date_dim").io.properties["s3.endpoint"], ADVERTISED)

    def test_loader_pins_client_endpoint_region_and_keys(self):
        cat = S.load_rest_catalog("kit", self.uri, "s3://tpcds-mv", s3_endpoint=CLIENT,
                                  region="us-east-1", key="CLIENTKEY", secret="CLIENTSECRET")
        for props in (cat.properties, cat.load_table("ns.date_dim").io.properties,
                      cat._load_file_io({"s3.endpoint": ADVERTISED}).properties):
            self.assertEqual(props["s3.endpoint"], CLIENT)
            self.assertEqual(props["s3.region"], "us-east-1")
            self.assertEqual(props["s3.path-style-access"], "true")
            self.assertEqual(props["s3.access-key-id"], "CLIENTKEY")
            self.assertEqual(props["s3.secret-access-key"], "CLIENTSECRET")
            self.assertNotIn("s3.session-token", props)

    def test_keys_default_to_the_env_the_kit_exports(self):
        import os
        old = {k: os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}
        os.environ.update(AWS_ACCESS_KEY_ID="ENVKEY", AWS_SECRET_ACCESS_KEY="ENVSECRET")
        try:
            cat = S.load_rest_catalog("kit", self.uri, s3_endpoint=CLIENT, region="us-east-1")
            io = cat.load_table("ns.date_dim").io.properties
            self.assertEqual((io["s3.endpoint"], io["s3.access-key-id"]), (CLIENT, "ENVKEY"))
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_env_temporary_keys_keep_their_session_token(self):
        # SSO / assumed-role keys (ASIA…) need AWS_SESSION_TOKEN; dropping it
        # (with the vended token) made S3 reject every request (review of f4139ba).
        import os
        names = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
        old = {k: os.environ.get(k) for k in names}
        os.environ.update(AWS_ACCESS_KEY_ID="ASIATEMP", AWS_SECRET_ACCESS_KEY="S", AWS_SESSION_TOKEN="TOK")
        try:
            cat = S.load_rest_catalog("kit", self.uri, s3_endpoint=CLIENT)
            for props in (cat.properties, cat.load_table("ns.date_dim").io.properties,
                          cat._load_file_io({"s3.session-token": "VENDED", "client.session-token": "VENDED"}).properties):
                self.assertEqual(props.get("s3.session-token"), "TOK")
                self.assertNotEqual(props.get("client.session-token"), "VENDED")
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_no_client_endpoint_keeps_the_catalogs(self):
        # Nothing to pin (AWS via IAM, no env keys): the catalog's value stands.
        import os
        old = {k: os.environ.pop(k, None) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}
        try:
            cat = S.load_rest_catalog("kit", self.uri)
            self.assertEqual(cat.load_table("ns.date_dim").io.properties["s3.endpoint"], ADVERTISED)
        finally:
            os.environ.update({k: v for k, v in old.items() if v is not None})


# --- query_pools: the key-pool builder's DuckDB ATTACH --------------------
# DuckDB's iceberg extension defaults to vended credentials and builds the
# table's S3 secret from the loadTable config — endpoint included — so every
# dim lookup went to minioserver:9000 ("pools: skip item ... Could not resolve
# hostname", then "q3: NO usable pool", uniform tick draws).
SNAP_META = dict(METADATA, **{
    "current-snapshot-id": 1, "refs": {"main": {"snapshot-id": 1, "type": "branch"}},
    "snapshots": [{"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": 0, "schema-id": 0,
                   "manifest-list": "s3://tpcds-mv/tpcds_clean/date_dim/metadata/snap-1.avro",
                   "summary": {"operation": "append"}}]})


class FakeRestAndS3(BaseHTTPRequestHandler):
    """Catalog AND the client's S3 endpoint on one port; records every GET."""
    seen = []

    def log_message(self, *a):
        pass

    def _json(self, body):
        FakeRest._json(self, body)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        FakeRestAndS3.seen.append(self.path)
        p = self.path
        if p.startswith("/v1/config"):
            return self._json({"defaults": {}, "overrides": {"s3.endpoint": ADVERTISED, "prefix": "main"}})
        if p.startswith("/v1/") and "/tables/" in p:
            return self._json({
                "metadata-location": "s3://tpcds-mv/tpcds_clean/date_dim/metadata/00000.metadata.json",
                "metadata": SNAP_META, "config": {
                    "s3.endpoint": ADVERTISED, "s3.access-key-id": "SERVERKEY",
                    "s3.secret-access-key": "SERVERSECRET", "s3.session-token": "SERVERTOKEN"}})
        if p.startswith("/v1/") and "/namespaces" in p:
            return self._json({"namespace": ["ns"], "properties": {}})
        self.send_response(404)
        self.end_headers()


def _pool_args(**kw):
    import types
    a = dict(s3_endpoint=CLIENT, s3_key="CLIENTKEY", s3_secret="CLIENTSECRET",
             s3_region="us-east-1", catalog="http://127.0.0.1:8181", warehouse="tpcds-mv")
    a.update(kw)
    return types.SimpleNamespace(**a)


class TestPoolBuilderUsesClientEndpoint(unittest.TestCase):
    def test_attach_sql_pins_the_client_secret_and_no_vended_credentials(self):
        import query_pools as QP
        sec, _, att = QP.duck_source_sql(_pool_args())
        self.assertIn("ENDPOINT '127.0.0.1:9000'", sec)
        self.assertIn("KEY_ID 'CLIENTKEY'", sec)
        self.assertIn("URL_STYLE 'path'", sec)
        self.assertIn("ACCESS_DELEGATION_MODE 'none'", att)

    def test_env_temporary_keys_keep_their_session_token(self):
        import os
        import query_pools as QP
        names = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
        old = {k: os.environ.get(k) for k in names}
        os.environ.update(AWS_ACCESS_KEY_ID="ASIATEMP", AWS_SECRET_ACCESS_KEY="S", AWS_SESSION_TOKEN="TOK")
        try:
            sec, _, att = QP.duck_source_sql(_pool_args(s3_endpoint="", s3_key="", s3_secret=""))
            self.assertIn("SESSION_TOKEN 'TOK'", sec)
            self.assertIn("ACCESS_DELEGATION_MODE 'none'", att)
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_nothing_given_keeps_the_catalogs_vended_credentials(self):
        import os
        import query_pools as QP
        old = {k: os.environ.pop(k, None) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}
        try:
            sec, _, att = QP.duck_source_sql(_pool_args(s3_endpoint="", s3_key="", s3_secret=""))
            self.assertIn("credential_chain", sec)
            self.assertNotIn("ACCESS_DELEGATION_MODE", att)
        finally:
            os.environ.update({k: v for k, v in old.items() if v is not None})

    def test_real_duckdb_attach_reads_through_the_client_endpoint(self):
        try:
            import duckdb  # noqa: F401
        except ImportError:
            self.skipTest("duckdb not installed")
        import query_pools as QP
        srv = HTTPServer(("127.0.0.1", 0), FakeRestAndS3)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        uri = "http://127.0.0.1:%d" % srv.server_address[1]
        FakeRestAndS3.seen = []
        try:
            con = QP.duck_connect(_pool_args(s3_endpoint=uri, catalog=uri))
            with self.assertRaises(Exception) as e:   # the fake S3 404s the manifest list
                con.execute("SELECT count(*) FROM src.ns.date_dim").fetchall()
        finally:
            srv.shutdown()
        self.assertNotIn("minioserver", str(e.exception))
        self.assertIn("/tpcds-mv/tpcds_clean/date_dim/metadata/snap-1.avro", FakeRestAndS3.seen)


if __name__ == "__main__":
    unittest.main()
