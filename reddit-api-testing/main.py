# This is mostly based on https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example
# The goal here to to get Oath for a script Reddit app (It must be a script app)
# The redddit user name/pass and client id/token are stored in .env
import requests
import requests.auth
from dotenv import load_dotenv
import os

load_dotenv()

ACCESSS_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
BEARER_TOEKN_URL =  "https://oauth.reddit.com"

# Load all env vars that will be needed
USERNAME = os.environ["R_USERNAME"]  # THIS HASE TO BE R_USERNAME USERNAME already is set in the env
PASS = os.environ["PASSWORD"]
ID = os.environ["ID"]
SECERET = os.environ["SECERET"]
USERAGENT = os.environ["USERAGENT"]

# Create the auth 
req_auth = requests.auth.HTTPBasicAuth(ID, SECERET)

# Creat post body content
post_data = {
        "grant_type": "password",
        "username": USERNAME,
        "password": PASS
        }

# Create user-agent
at_headers = {"User-Agent": USERAGENT}

# Now request time
at_resp = requests.post(ACCESSS_TOKEN_URL, auth=req_auth, data=post_data, headers=at_headers)

# If the call fails exit
if at_resp.status_code != 200:
    print(f"ERR: at_resp malformed got {at_resp.status_code} from {at_resp.url}")
    os.exit()
print(at_resp.url)
resp_dict = at_resp.json()

if resp_dict == {}:
    print(f"ERR: from {at_resp.url} no json")
    os.exit() 


# Unpack json
print(resp_dict)
bearer_token = resp_dict["access_token"]
expires = resp_dict["expires_in"]

bearer_token_auth_str = "bearer " + bearer_token

bt_headers = {
        "Authorization": bearer_token_auth_str,
        "User-Agent": USERAGENT
        }

bt_resp = requests.get(BEARER_TOEKN_URL, headers=bt_headers)

print(bt_resp.json())
