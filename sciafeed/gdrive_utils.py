from os.path import exists, join
import pickle

import requests

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


def get_gdrive_credentials(credentials_folder):
    creds = None
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    credentials_path = join(credentials_folder, 'credentials.json')
    token_path = join(credentials_folder, 'token.pickle')
    if exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_gdrive_service(credentials_folder):
    creds = get_gdrive_credentials(credentials_folder)
    service = build('drive', 'v3', credentials=creds)
    return service


def get_gdrive_filelist(folder_id, credentials_folder):
    """
    Return the dictionary {filename:id} of files contained in a google drive folder.
    The folder_id must be accessible by the google service `theservice`.

    :param folder_id: id of the public folder
    :param credentials_folder: folder with google credential files
    :return: dictionary {filename:id}
    """
    ret_value = dict()
    page_token = None
    service = get_gdrive_service(credentials_folder)
    while True:
        response = service.files().list(
            q="'%s' in parents" % folder_id,
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token).execute()
        for file in response.get('files', []):
            ret_value[file.get('name')] = file.get('id')
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return ret_value


def download_gdrive_public_file(file_id, output_path):
    """download a public shared google drive file"""
    session = requests.Session()
    DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'
    CHUNK_SIZE = 32768
    response = session.get(DOWNLOAD_URL, params={'id': file_id}, stream=True)
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
