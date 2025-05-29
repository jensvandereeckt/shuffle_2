import json
import io
import time
from collections import defaultdict
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"     # reduced_votes folder
OUTPUT_FOLDER_ID = "1oSYINzluIqyg9qWG88zRGLkaffKxjs8Q"    # ranking_votes folder
LOGS_FOLDER_ID = "1hXa-sxiy11T4NKLxWfDodhkliWcQr2Ba"       # logs folder
CHECK_INTERVAL = 15
TOTAL_RUNTIME = 120

log_filename = f"ranking_log_shuffle2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def log(message):
    timestamped = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped, flush=True)
    with open(log_filename, "a") as f:
        f.write(timestamped + "\n")

def authenticate_drive():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials)

def upload_to_drive(filename, folder_id):
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(filename, mimetype="text/plain")
    return drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

def delete_existing_drive_file(filename, folder_id):
    existing = drive_service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents",
        fields="files(id)"
    ).execute().get("files", [])
    for file in existing:
        drive_service.files().delete(fileId=file["id"]).execute()

def generate_global_ranking(total_votes):
    global_output_filename = "global_winner_ranking.txt"
    global_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)

    with open(global_output_filename, "w") as f:
        f.write("\U0001F30D Global Final Song Ranking:\n\n")
        for i, (song, votes) in enumerate(global_ranking, start=1):
            line = f"{i}. Song {song}: {votes} votes"
            print(line, flush=True)
            f.write(line + "\n")

    delete_existing_drive_file(global_output_filename, OUTPUT_FOLDER_ID)
    upload_to_drive(global_output_filename, OUTPUT_FOLDER_ID)
    log(f"Globale ranking geüpload naar Google Drive als '{global_output_filename}'.")

def main():
    global drive_service
    drive_service = authenticate_drive()
    total_votes = defaultdict(int)
    processed_files = set()
    start_time = time.time()
    interrupted = False

    log("🟢 Start stemverwerking voor 2 minuten (Ctrl+C om te stoppen)...")

    try:
        while time.time() - start_time < TOTAL_RUNTIME:
            query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            for file in files:
                filename = file['name']
                if filename in processed_files:
                    continue
                processed_files.add(filename)
                file_id = file['id']
                country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
                output_filename = f"final_ranking_{country_code}.txt"

                log(f"⬇️ Downloaden van '{filename}'...")
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
                    f.write(f"\U0001F3B5 Final Song Ranking for {country_code.upper()}:\n\n")
                    for i, (song, votes) in enumerate(final_ranking, start=1):
                        line = f"{i}. Song {song}: {votes} votes"
                        print(line, flush=True)
                        f.write(line + "\n")

                log(f"✅ '{output_filename}' lokaal opgeslagen.")
                delete_existing_drive_file(output_filename, OUTPUT_FOLDER_ID)
                upload_to_drive(output_filename, OUTPUT_FOLDER_ID)
                log(f"📤 Bestand '{output_filename}' geüpload naar Google Drive.")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        interrupted = True
        log("🛑 Handmatig gestopt door gebruiker.")

    log("📊 Genereren van globale ranking...")
    generate_global_ranking(total_votes)

    try:
        upload_to_drive(log_filename, LOGS_FOLDER_ID)
        log("📝 Logbestand geüpload naar Google Drive.")
    except Exception as e:
        log(f"⚠️ Fout bij uploaden logbestand: {e}")

    if not interrupted:
        log("🏁 Script normaal afgesloten.")

if __name__ == "__main__":
    main()

