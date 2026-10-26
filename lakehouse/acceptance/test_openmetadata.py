"""Acceptance test for OpenMetadata, the lakehouse's ontology layer (Phase 5).

    docker compose --profile core exec -T jupyter pytest -q lakehouse/acceptance/test_openmetadata.py

OpenMetadata 2.0.5 runs at https://<LAKE_TS_HOST>/openmetadata/ (BASE_PATH), with OpenSearch for search and its
own database on catalog-pg. Nothing from the quickstart's published defaults survives: the admin password, the
Fernet key and the JWT signing keys are all generated on this VM by bootstrap.sh, and self-signup is off.
"""
import base64
import os
from pathlib import Path

import psycopg2
import pytest
import requests

OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata")
OPENSEARCH = os.environ.get("OPENSEARCH_URL", "http://opensearch:9200")
KEYS = Path(os.environ.get("OM_KEYS", "/lake/openmetadata/keys"))
ADMIN = "admin@open-metadata.org"


def login(password):
    """POST OpenMetadata's basic-auth login; returns the response (the password goes base64-encoded, as the UI sends it)."""
    return requests.post(f"{OM}/api/v1/users/login", timeout=30,
                         json={"email": ADMIN, "password": base64.b64encode(password.encode()).decode()})


@pytest.fixture(scope="module")
def token():
    """An admin access token, from the password bootstrap.sh generated."""
    r = login(os.environ["OM_ADMIN_PASSWORD"])
    r.raise_for_status()
    return r.json()["accessToken"]


def test_server_is_up_at_the_pinned_version():
    """The server answers under its base path and reports 2.0.5."""
    r = requests.get(f"{OM}/api/v1/system/version", timeout=30)
    r.raise_for_status()
    assert r.json()["version"].startswith("2.0.5")


def test_search_runs_on_opensearch():
    """OpenMetadata's search indexes live in OpenSearch."""
    info = requests.get(OPENSEARCH, timeout=30).json()
    assert info["version"]["distribution"] == "opensearch"
    indexes = requests.get(f"{OPENSEARCH}/_cat/indices?format=json", timeout=30).json()
    assert any("glossary_term_search_index" in i["index"] for i in indexes)


def test_metadata_lives_in_catalog_pg():
    """Its database is openmetadata_db on catalog-pg, owned by its own openmetadata role."""
    with psycopg2.connect(host="catalog-pg", dbname="ducklake", user="lake_rw",
                          password=os.environ["PGPASSWORD"]) as pg, pg.cursor() as cur:
        cur.execute("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'openmetadata_db'")
        assert cur.fetchall() == [("openmetadata",)]


def test_default_admin_password_is_rejected():
    """The quickstart's well-known admin password doesn't log in."""
    assert login("admin").status_code in (400, 401)


def test_generated_admin_password_logs_in(token):
    """The admin password from .env gets a token."""
    assert token


def test_self_signup_is_off():
    """Nobody can create their own account."""
    r = requests.get(f"{OM}/api/v1/system/config/auth", timeout=30)
    r.raise_for_status()
    assert r.json().get("enableSelfSignup") is False


def test_tokens_are_signed_with_our_own_key():
    """The public key OpenMetadata publishes is the one bootstrap.sh generated, not the image's built-in default."""
    from cryptography.hazmat.primitives.serialization import load_der_public_key
    ours = load_der_public_key((KEYS / "public_key.der").read_bytes()).public_numbers().n
    keys = requests.get(f"{OM}/api/v1/system/config/jwks", timeout=30).json()["keys"]
    published = {int.from_bytes(base64.urlsafe_b64decode(k["n"] + "=" * (-len(k["n"]) % 4)), "big") for k in keys}
    assert published == {ours}


def test_mcp_reader_is_a_read_only_user(token):
    """mcp-reader (the MCP server's account) logs in with OM_MCP_PASSWORD, isn't an admin, holds only DataConsumer,
    and can't change the glossary."""
    r = requests.post(f"{OM}/api/v1/users/login", timeout=30, json={
        "email": "mcp-reader@open-metadata.org",
        "password": base64.b64encode(os.environ["OM_MCP_PASSWORD"].encode()).decode()})
    r.raise_for_status()
    reader = r.json()["accessToken"]
    user = requests.get(f"{OM}/api/v1/users/name/mcp-reader", params={"fields": "roles"}, timeout=30,
                        headers={"Authorization": f"Bearer {token}"}).json()
    assert not user.get("isAdmin") and [x["name"] for x in user["roles"]] == ["DataConsumer"]
    write = requests.put(f"{OM}/api/v1/glossaries", headers={"Authorization": f"Bearer {reader}"}, timeout=30,
                         json={"name": "Lakehouse", "description": "overwritten by a reader"})
    assert write.status_code == 403

