from google.cloud import secretmanager

class GetAPISecret:
    def __init__(self, secret_id, project_id = "franchisecric", version_id = 1):
        self.secret_id = secret_id
        self.project_id = project_id
        self.version_id = version_id

    def getAPISecret(self): 
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/{self.version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")