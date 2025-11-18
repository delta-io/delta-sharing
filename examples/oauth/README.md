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

### 3. Run the script

Run the script on your desktop machine. It will open a browser window where you can authenticate using your own Entra ID tenant credentials.
Make sure to pass your tenant ID (not the example below).
 
```bash
python u2m-entraid-capture-jwt-token.py --tenant-id 9fca8007-4338-4251-99db-4791001e3151
```

After login completes, the script will print the captured and decoded JWT token.
This is useful for verifying that all required claims (such as groups) are present.
 
