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


if __name__ == "__main__":
    unittest.main()
