from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession

def invoke_cloud_function():

    url = "<url>"
    request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request) # If your cloud function url has query parameters, remove them before passing to the audience

    resp = AuthorizedSession(id_token_credentials).request("GET", url=url) # the authorized session object is used to access the Cloud Function

    print(resp.status_code) # should return 200
    print(resp.content) # the body of the HTTP response