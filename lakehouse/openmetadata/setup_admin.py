"""OpenMetadata's accounts, from .env. Idempotent.

  - admin: replace the well-known default password with OM_ADMIN_PASSWORD
  - mcp-reader: the MCP server's read-only account (role DataConsumer only), password OM_MCP_PASSWORD

    python lakehouse/openmetadata/setup_admin.py           # fix whatever isn't in place
    python lakehouse/openmetadata/setup_admin.py --check   # change nothing; exit 1 unless both log in

bootstrap.sh runs it in the jupyter container once the server is healthy. Login takes the password base64-encoded
(as OpenMetadata's UI sends it); the change-password call takes plain text.
"""
import base64
import os
import sys

import requests

OM = os.environ.get("OM_URL", "http://openmetadata-server:8585/openmetadata")
ADMIN = "admin@open-metadata.org"
DEFAULT_PASSWORD = "admin"   # what a fresh OpenMetadata starts with


def b64(text):
    """Base64 of a password, as OpenMetadata's API expects it."""
    return base64.b64encode(text.encode()).decode()


def login(password, email=ADMIN):
    """An access token for this user and password, or None if it doesn't log in."""
    r = requests.post(f"{OM}/api/v1/users/login", json={"email": email, "password": b64(password)}, timeout=30)
    return r.json().get("accessToken") if r.ok else None


READER, READER_EMAIL = "mcp-reader", "mcp-reader@open-metadata.org"


def ensure_reader(admin_token, check):
    """mcp-reader exists, holds only DataConsumer, and logs in with OM_MCP_PASSWORD. Returns a status line."""
    password = os.environ["OM_MCP_PASSWORD"]
    if login(password, READER_EMAIL):
        return "mcp-reader in place"
    if check:
        return None
    h = {"Authorization": f"Bearer {admin_token}"}
    exists = requests.get(f"{OM}/api/v1/users/name/{READER}", headers=h, timeout=30).ok
    if exists:   # e.g. OM_MCP_PASSWORD changed in .env: reset it as admin
        r = requests.put(f"{OM}/api/v1/users/changePassword", headers=h, timeout=30,
                         json={"username": READER, "newPassword": password, "confirmPassword": password,
                               "requestType": "USER"})
    else:
        role = requests.get(f"{OM}/api/v1/roles/name/DataConsumer", headers=h, timeout=30).json()["id"]
        r = requests.post(f"{OM}/api/v1/users", headers=h, timeout=30,
                          json={"name": READER, "email": READER_EMAIL, "displayName": "MCP reader",
                                "description": "Read-only account the lakehouse MCP server uses for define_term.",
                                "isAdmin": False, "isBot": False, "roles": [role],
                                "createPasswordType": "ADMIN_CREATE", "password": password,
                                "confirmPassword": password})
    if not r.ok or not login(password, READER_EMAIL):
        raise RuntimeError(f"mcp-reader setup failed ({r.status_code}: {r.text[:200]})")
    return "mcp-reader created" if not exists else "mcp-reader password reset"


def main(argv):
    """Check or fix the admin password, then the mcp-reader account; print one summary line."""
    check, ours = "--check" in argv, os.environ["OM_ADMIN_PASSWORD"]
    token = login(ours)
    if token:
        admin = "admin password in place"
    elif check:
        print("OpenMetadata accounts: the generated admin password does not log in (run bootstrap.sh without --check)")
        return 1
    else:
        token = login(DEFAULT_PASSWORD)
        if not token:
            print("OpenMetadata accounts: neither the generated nor the default admin password logs in")
            return 1
        r = requests.put(f"{OM}/api/v1/users/changePassword", headers={"Authorization": f"Bearer {token}"},
                         timeout=30, json={"username": "admin", "oldPassword": DEFAULT_PASSWORD, "newPassword": ours,
                                           "confirmPassword": ours, "requestType": "SELF"})   # plain text, unlike login
        token = login(ours)
        if not r.ok or not token:
            print(f"OpenMetadata accounts: admin password change failed ({r.status_code}: {r.text[:200]})")
            return 1
        admin = "admin password replaced the default"
    try:
        reader = ensure_reader(token, check)
    except RuntimeError as e:
        print(f"OpenMetadata accounts: {e}")
        return 1
    if reader is None:
        print("OpenMetadata accounts: mcp-reader does not log in (run bootstrap.sh without --check)")
        return 1
    print(f"OpenMetadata accounts: {admin}; {reader}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
