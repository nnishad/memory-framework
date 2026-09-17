"""Desktop OAuth with a loopback callback, PKCE, and credentials kept out of logs."""
import base64
import hashlib
import json
import secrets
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

SCOPE = 'https://www.googleapis.com/auth/gmail.readonly'
TOKEN_URL = 'https://oauth2.googleapis.com/token'


def token_request(fields):
    try:
        with urlopen(Request(TOKEN_URL, data=urlencode(fields).encode(),
                             headers={'Content-Type':'application/x-www-form-urlencoded'}),timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        # Provider response bodies may contain credentials or user identifiers.
        raise ValueError(f'Google token request failed (HTTP {error.code}); reauthorize if access was revoked') from None


def authorize(client_file, timeout=600, on_ready=None):
    document=json.loads(Path(client_file).read_text(encoding='utf-8-sig'))
    client=document.get('installed')
    if not isinstance(client,dict) or not client.get('client_id') or not client.get('client_secret'):
        raise ValueError('Expected a Google desktop OAuth client configuration')
    if client.get('token_uri') != TOKEN_URL:
        raise ValueError('Unexpected OAuth token endpoint')
    state=secrets.token_urlsafe(32); verifier=secrets.token_urlsafe(64)
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    result={}
    class Callback(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            parsed=urlparse(self.path); values=parse_qs(parsed.query)
            if parsed.path!='/callback' or not secrets.compare_digest(values.get('state',[''])[0],state):
                self.send_response(400); self.end_headers(); return
            result.update(code=values.get('code',[None])[0],error=values.get('error',[None])[0])
            self.send_response(200); self.send_header('Content-Type','text/plain'); self.send_header('Cache-Control','no-store'); self.end_headers()
            self.wfile.write(b'Authorization received. You may close this tab and return to the terminal.')
    with HTTPServer(('127.0.0.1',0),Callback) as server:
        server.timeout=1
        redirect=f'http://127.0.0.1:{server.server_port}/callback'
        url='https://accounts.google.com/o/oauth2/v2/auth?'+urlencode({
            'client_id':client['client_id'],'redirect_uri':redirect,'response_type':'code',
            'scope':SCOPE,'state':state,'code_challenge':challenge,'code_challenge_method':'S256',
            'access_type':'offline','prompt':'consent'})
        if on_ready: on_ready(url)
        else: webbrowser.open(url)
        deadline=time.monotonic()+timeout
        while not result and time.monotonic()<deadline: server.handle_request()
    if result.get('error') or not result.get('code'):
        raise ValueError('Google authorization was denied or timed out')
    token=token_request({'client_id':client['client_id'],'client_secret':client['client_secret'],
        'code':result['code'],'code_verifier':verifier,'redirect_uri':redirect,'grant_type':'authorization_code'})
    if not token.get('refresh_token') or SCOPE not in token.get('scope','').split():
        raise ValueError('Gmail read-only access and an offline refresh token are required')
    return {'client_id':client['client_id'],'client_secret':client['client_secret'],
            'refresh_token':token['refresh_token'],'scope':SCOPE}
