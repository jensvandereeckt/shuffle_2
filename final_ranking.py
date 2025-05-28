import json
import io
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"     # reduced_votes folder
OUTPUT_FOLDER_ID = "1oSYINzluIqyg9qWG88zRGLkaffKxjs8Q"    # ranking_votes folder

# === AUTHENTICATE TO GOOGLE DRIVE ===
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# === ZOEK ALLE reduced_votes_*.json BESTANDEN ===
print("🔍 Zoeken naar stem-bestanden (reduced_votes_*.json)...")
query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception("❌ Geen stem-bestanden gevonden in opgegeven Drive-map.")

total_votes = defaultdict(int)

for file in files:
    filename = file['name']
    file_id = file['id']
    country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
    output_filename = f"final_ranking_{country_code}.txt"

    print(f"⬇️ Downloaden van '{filename}'...")
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    data = json.load(fh)

    country_votes = defaultdict(int)

    for entry in data:
        for vote in entry["votes"]:
            song = vote["song_number"]
            count = vote["count"]
            country_votes[song] += count
            total_votes[song] += count

    final_ranking = sorted(country_votes.items(), key=lambda x: x[1], reverse=True)

    with open(output_filename, "w") as f:
        f.write(f"🎵 Final Song Ranking for {country_code.upper()}:\n\n")
        for i, (song, votes) in enumerate(final_ranking, start=1):
            line = f"{i}. Song {song}: {votes} votes"
            print(line)
            f.write(line + "\n")

    print(f"✅ '{output_filename}' lokaal opgeslagen.")

    # Verwijder bestaand bestand indien nodig
    existing = drive_service.files().list(
        q=f"name='{output_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
        fields="files(id)"
    ).execute().get("files", [])

    for old_file in existing:
        drive_service.files().delete(fileId=old_file["id"]).execute()
        print(f"🗑️ Oud bestand '{output_filename}' verwijderd uit Google Drive.")

    file_metadata = {
        "name": output_filename,
        "parents": [OUTPUT_FOLDER_ID]
    }
    media = MediaFileUpload(output_filename, mimetype="text/plain")
    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    print(f"📤 Bestand '{output_filename}' geüpload naar Google Drive.")

# === GLOBALE RANKING ===
global_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)
global_output_filename = "global_winner_ranking.txt"

with open(global_output_filename, "w") as f:
    f.write("🌍 Global Final Song Ranking:\n\n")
    for i, (song, votes) in enumerate(global_ranking, start=1):
        line = f"{i}. Song {song}: {votes} votes"
        f.write(line + "\n")
        print(line)

# Verwijder bestaand globaal bestand indien nodig
existing = drive_service.files().list(
    q=f"name='{global_output_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
    fields="files(id)"
).execute().get("files", [])

for old_file in existing:
    drive_service.files().delete(fileId=old_file["id"]).execute()
    print(f"🗑️ Oud bestand '{global_output_filename}' verwijderd uit Google Drive.")

file_metadata = {
    "name": global_output_filename,
    "parents": [OUTPUT_FOLDER_ID]
}
media = MediaFileUpload(global_output_filename, mimetype="text/plain")
drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

print(f"🌍 📤 Globaal rankingbestand '{global_output_filename}' geüpload naar Drive.")
