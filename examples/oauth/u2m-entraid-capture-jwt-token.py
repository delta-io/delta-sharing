import base64
import hashlib
import http.server
import json
import secrets
import threading
import urllib.parse
import webbrowser

import requests
import jwt  # pyjwt
import argparse

# ---------------- CONFIG ----------------

parser = argparse.ArgumentParser()
parser.add_argument("--tenant-id", required=True)
args = parser.parse_args()

TENANT_ID = args.tenant_id

CLIENT_ID = "a672d62c-fc7b-4e81-a576-e60dc46e951d"  # Databricks published multi tenant app for PowerBI Delta Sharing
REDIRECT_URI = "http://localhost:8080"

# Scopes: use what your app needs (e.g. user.read, or your API's scope)
SCOPES = ["64978f70-f6a6-4204-a29e-87d74bfea138/Read", "offline_access"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
AUTH_URL = f"{AUTHORITY}/oauth2/v2.0/authorize"
TOKEN_URL = f"{AUTHORITY}/oauth2/v2.0/token"

LISTEN_HOST = "localhost"
LISTEN_PORT = 8080

# ------------- PKCE HELPERS -------------

def generate_code_verifier(length: int = 64) -> str:
    # high-entropy string
    return base64.urlsafe_b64encode(secrets.token_bytes(length)).decode("ascii").rstrip("=")


def generate_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


# ------------- HTTP SERVER TO CATCH REDIRECT -------------

class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    # shared state between server and main thread
    auth_result = {"code": None, "error": None}

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        qs = urllib.parse.parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        error = qs.get("error", [None])[0]

        OAuthCallbackHandler.auth_result["code"] = code
        OAuthCallbackHandler.auth_result["error"] = error

        # Simple response in browser
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body><h2>Authentication complete.</h2>"
                         b"You can close this window.</body></html>")

    def log_message(self, format, *args):
        # silence default logging
        return


def start_http_server():
    server = http.server.HTTPServer((LISTEN_HOST, LISTEN_PORT), OAuthCallbackHandler)
    # Run in a separate thread so main thread can open browser and wait
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ------------- MAIN FLOW -------------

def main():
    print("This Python script simulates a U2M OAuth login (e.g., PowerBI Delta Sharing) against an Entra ID tenant.")

    # 1) Start local HTTP server
    server = start_http_server()
    print(f"Listening on http://{LISTEN_HOST}:{LISTEN_PORT} for OAuth redirect...")

    # 2) Build auth URL with PKCE
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)

    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "response_mode": "query",
        "scope": " ".join(SCOPES),
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    auth_url = AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("Opening browser for login...")
    webbrowser.open(auth_url)

    # 3) Wait until we get the auth code (or error)
    print("Waiting for redirect with authorization code...")
    while OAuthCallbackHandler.auth_result["code"] is None and OAuthCallbackHandler.auth_result["error"] is None:
        pass  # simple spin; in real code you might sleep briefly

    server.shutdown()

    if OAuthCallbackHandler.auth_result["error"]:
        raise RuntimeError(f"OAuth error: {OAuthCallbackHandler.auth_result['error']}")

    code = OAuthCallbackHandler.auth_result["code"]
    print(f"Received authorization code")

    # 4) Exchange code for tokens
    token_data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": code_verifier,
    }

    resp = requests.post(TOKEN_URL, data=token_data)
    if resp.status_code != 200:
        print("Token endpoint error:")
        print(resp.text)
        raise SystemExit(1)

    tokens = resp.json()

    access_token = tokens.get("access_token")
    id_token = tokens.get("id_token")

    #print("\n=== RAW ACCESS TOKEN (JWT) ===")
    #print(access_token)

    # 5) Optionally decode JWT locally (no signature verification, debugging only)
    if access_token:
        decoded_access = jwt.decode(access_token, options={"verify_signature": False})
        print("\n=== DECODED ACCESS TOKEN CLAIMS ===")
        print(json.dumps(decoded_access, indent=2))

    if id_token:
        decoded_id = jwt.decode(id_token, options={"verify_signature": False})
        print("\n=== DECODED ID TOKEN CLAIMS (ID TOKEN) ===")
        print(json.dumps(decoded_id, indent=2))


if __name__ == "__main__":
    main()
