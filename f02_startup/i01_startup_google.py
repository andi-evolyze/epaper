# Google Drive Access
from pydrive.auth import GoogleAuth
GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = "f01_config/client_secrets.json"
from pydrive.drive import GoogleDrive

gauth = GoogleAuth(settings_file='f01_config/settings.yaml')
gauth.CommandLineAuth() # client_secrets.json need to be in the same directory as the script
drive = GoogleDrive(gauth)



# print("Vorbereitung Google abgeschlossen")