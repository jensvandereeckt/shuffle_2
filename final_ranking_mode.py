import json
import io
from collections import defaultdict
from statistics import mode
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"     # Folder waar reduced_votes.json zit
OUTPUT_FOLDER_ID = "1oSYINzluIqyg9qWG88zRGLkaffKxjs8Q"    # Folder waar final_ranking_mode.txt moet komen
INPUT_FILE_NAME = "reduced_votes.json"
OUTPUT_FILE_NAME = "final_ranking_mode.txt"

# === AUTHENTICATE TO GOOGLE DRIVE ===
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# === DOWNLOAD INPUT FILE FROM GOOGLE DRIVE ===
print(f"📅 Zoeken naar '{INPUT_FILE_NAME}' in Google Drive-map...")
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
try:
    with open(INPUT_FILE_NAME, "r") as f:
        data = json.load(f)

    total_votes = defaultdict(int)
    all_votes_flat = []

    for entry in data:
        for vote in entry["votes"]:
            song = vote["song_number"]
            count = vote["count"]
            total_votes[song] += count
            all_votes_flat.extend([song] * count)

    final_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)
    most_voted_song = mode(all_votes_flat)
    top_ranked_song = final_ranking[0][0]
    top_ranked_votes = final_ranking[0][1]

    output_lines = [
        f"🎵 Most voted song using mode(): Song {most_voted_song}",
        f"🏆 Top-ranked song by total votes: Song {top_ranked_song} with {top_ranked_votes} votes",
        "✅ Both methods agree on the winner!" if most_voted_song == top_ranked_song else "⚠️ Warning: Mode and total vote count give different winners!"
    ]

    with open(OUTPUT_FILE_NAME, "w") as f:
        for line in output_lines:
            f.write(line + "\n")

    print(f"✅ '{OUTPUT_FILE_NAME}' lokaal opgeslagen.")

    # === UPLOAD NAAR GOOGLE DRIVE ===
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

    print(f"📄 Bestand '{OUTPUT_FILE_NAME}' succesvol geüpload naar Google Drive (ID: {upload_response.get('id')})")

except FileNotFoundError:
    print(f"File '{INPUT_FILE_NAME}' not found.")
except json.JSONDecodeError:
    print(f"Error decoding JSON from '{INPUT_FILE_NAME}'.")
except Exception as e:
    print(f"Unexpected error: {e}")

