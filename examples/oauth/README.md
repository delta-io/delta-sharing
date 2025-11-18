# Entra ID OAuth Debugger (U2M / PowerBI-Style Flow)

This script simulates the **U2M OAuth authorization code + PKCE flow** used by tools like **Power BI Delta Sharing** when authenticating against an **Entra ID (Azure AD)** tenant.

It:

- Opens a browser for interactive login  
- Runs a local webserver to receive the OAuth redirect  
- Exchanges the authorization code for tokens  
- Prints the raw **JWT access token**  
- Decodes and displays the token claims (useful for debugging missing `groups`, `aud`, etc.)

---

## 1. Create and Activate a Virtual Environment

### macOS / Linux
```bash
python3 -m venv venv
source venv/bin/activate
```

Windows (PowerShell)
```powershell
python -m venv venv
.\venv\Scripts\Activate
```

When activated, your shell prompt will look like:
```
(venv)
```

## 2. Install Dependencies

With the virtual environment active:
```bash
pip install -r requirements.txt
```

## 3. Run the script

Run this script directly on your desktop or laptop. When executed, it will automatically open a browser window and prompt you to authenticate using your own Entra ID credentials.
After successful authentication, the script captures and prints the decoded JWT access token and ID token claims.

You can authenticate in one of two ways:

### Option 1: Use a Delta Sharing OIDC Recipient Endpoint

If you have a Delta Sharing OIDC recipient configured in Databricks, you can pass its full REST API endpoint.
The script will automatically perform OIDC discovery:

Example:
```bash
python3 u2m-entraid-capture-jwt-token.py --delta-sharing-oidc-recipient-endpoint https://oregon.databricks.com/api/2.0/delta-sharing/metastores/11111111-2222-3333-4444-555555555555/recipients/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
```
(Replace the metastore ID and recipient ID above with your real values.
The UUIDs shown are sample placeholders.)

#### Option 2: Use Your Entra ID Tenant ID Directly

If you prefer to authenticate using your Entra tenant, pass the tenant ID (GUID).
The script will use the standard Microsoft OAuth2 v2.0 endpoints.

Example:
```bash
python3 u2m-entraid-capture-jwt-token.py --entraid-tenant-id 77777777-8888-9999-0000-666666666666
```
(Replace this with your actual tenant ID.)

After login completes, the script will print the captured and decoded JWT token.
This is useful for verifying that all required claims (such as groups) are present.
 
