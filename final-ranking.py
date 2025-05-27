import json
import io
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"      
OUTPUT_FOLDER_ID = "1oSYINzluIqyg9qWG88zRGLkaffKxjs8Q"      
INPUT_FILE_NAME = "reduced_votes.json"
OUTPUT_FILE_NAME = "final_ranking.txt"                    

# === AUTHENTICATE TO GOOGLE DRIVE ===
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# === DOWNLOAD INPUT FILE FROM GOOGLE DRIVE ===
print(f"📥 Zoeken naar '{INPUT_FILE_NAME}' in Google Drive-map...")
query = f"name='{INPUT_FILE_NAME}' and '{INPUT_FOLDER_ID}' in parents"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

if not items:
    raise Exception(f"❌ Bestand '{INPUT_FILE_NAME}' niet gevonden in opgegeven Drive-map.")

file_id = items[0]['id']
request = drive_service.files().get_media(fileId=file_id)
fh = io.FileIO(INPUT_FILE_NAME, 'wb')
downloader = MediaIoBaseDownload(fh, request)

done = False
while not done:
    status, done = downloader.next_chunk()

print(f"✅ '{INPUT_FILE_NAME}' succesvol gedownload.")

# === VERWERK STEMMEN ===
with open(INPUT_FILE_NAME, "r") as f:
    data = json.load(f)

total_votes = defaultdict(int)

for entry in data:
    for vote in entry["votes"]:
        song_number = vote["song_number"]
        count = vote["count"]
        total_votes[song_number] += count

# Sorteer op stemmen
final_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)

# === SCHRIJF OUTPUT NAAR TEKSTBESTAND ===
with open(OUTPUT_FILE_NAME, "w") as f:
    f.write("🎵 Final Song Ranking (by total votes):\n\n")
    for i, (song, votes) in enumerate(final_ranking, start=1):
        line = f"{i}. Song {song}: {votes} votes"
        print(line)
        f.write(line + "\n")

print(f"✅ '{OUTPUT_FILE_NAME}' lokaal opgeslagen.")

# === UPLOAD OUTPUT NAAR GOOGLE DRIVE ===
file_metadata = {
    "name": OUTPUT_FILE_NAME,
    "parents": [OUTPUT_FOLDER_ID]
}
media = MediaFileUpload(OUTPUT_FILE_NAME, mimetype="text/plain")

upload_response = drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

print(f"📤 Bestand '{OUTPUT_FILE_NAME}' succesvol geüpload naar Google Drive (ID: {upload_response.get('id')})")
