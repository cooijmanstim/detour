import os
key_path = os.path.join(os.environ["HOME"], ".experiment_drivekey.json")

import numpy as np
npz_path = "test.npz"
np.savez_compressed(npz_path, x=[1,2,3])
base_path = "testfolder"

from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http

scopes = ["https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_path, scopes=scopes)
http_auth = credentials.authorize(Http())

from apiclient.discovery import build

drive = build('drive', 'v3', http=http_auth)

# TODO: decide if i want to deal with a directory structure and cram hyperparameter values into unique directory names, or be more drivelike and allow multiple runs to have the same name

# generate a name based on timestamp, invocation and hash of hyperparameters and code
# local launch script creates folder, pass its fileId to the job
# dump hyperparameters and code
# job checks equality of hyperparameters and code?
# local launch script has switch to be asked for experiment notes before doing anything
# local launch script watches folder

def putfile(file, path, drive):
  for part in os.path.split(os.path.dirname(path)):
    folder = get_or_create_folder(part, parent=folder)
  update_or_create_file(os.path.basename(path), file)

def getfile(path, drive):
  for part in os.path.split(os.path.dirname(path)):
    folder = get_or_create_folder(part, parent=folder)
  update_or_create_file(os.path.basename(path), file)
  

folder = drive.files().create(
  body=dict(name=base_path, mimeType="application/vnd.google-apps.folder"),
  fields="id"
).execute()

drive.permissions().create(
  fileId=folder["id"],
  body=dict(
    emailAddress="cooijmat@lisa.iro.umontreal.ca",
    type="user",
    role="writer",
)).execute()

file = drive.files().create(
  body=dict(name="test.npz", parents=[folder["id"]]),
  media_body=npz_path,
  media_mime_type="application/zip"
).execute()

print(drive.files().list().execute())
files = drive.files().list().execute()

batch = drive.new_batch_http_request()
for file in files["files"]:
  batch.add(drive.files().delete(fileId=file["id"]))
batch.execute()

print(drive.files().list().execute())
