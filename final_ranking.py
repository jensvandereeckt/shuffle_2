import json
import io
import time
from datetime import datetime
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"     # reduced_votes folder
OUTPUT_FOLDER_ID = "1oSYINzluIqyg9qWG88zRGLkaffKxjs8Q"    # ranking_votes folder
LOGS_FOLDER_ID = "1hXa-sxiy11T4NKLxWfDodhkliWcQr2Ba"       # logs folder op Drive
RUNTIME_SECONDS = 120
CHECK_INTERVAL = 15

log_filename = f"ranking_log_shuffle2_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# === LOG FUNCTIE ===
def log(message):
    timestamp = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamp, flush=True)
    with open(log_filename, "a") as f:
        f.write(timestamp + "\n")

# === DRIVE HULPFUNCTIES ===
def authenticate_drive():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials)

def download_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

def remove_existing_drive_file(filename, folder_id):
    existing = drive_service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents",
        fields="files(id)"
    ).execute().get("files", [])
    for file in existing:
        drive_service.files().delete(fileId=file["id"]).execute()

def upload_to_drive(filename, folder_id):
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(filename, mimetype="text/plain")
    drive_service.files().create(body=metadata, media_body=media, fields="id").execute()

# === MAIN LOOP ===
def main():
    global drive_service
    drive_service = authenticate_drive()

    seen_files = set()
    total_votes = defaultdict(int)
    start_time = time.time()

    log("🟢 Start stemverwerking voor 2 minuten (Ctrl+C om te stoppen)...")

    try:
        while time.time() - start_time < RUNTIME_SECONDS:
            query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            for file in files:
                filename = file['name']
                file_id = file['id']
                if filename in seen_files:
                    continue

                seen_files.add(filename)
                country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
                output_filename = f"final_ranking_{country_code}.txt"

                log(f"📄 Nieuw bestand gevonden: {filename}")
                data = download_file(file_id)
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
                        f.write(line + "\n")
                        log(line)

                log(f"💾 Bestand opgeslagen: {output_filename}")
                remove_existing_drive_file(output_filename, OUTPUT_FOLDER_ID)
                upload_to_drive(output_filename, OUTPUT_FOLDER_ID)
                log(f"☁️ Upload voltooid naar Drive ({output_filename})")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Manueel gestopt door gebruiker.")

    # === GLOBALE RANKING ===
    global_output_filename = "global_winner_ranking.txt"
    global_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)

    with open(global_output_filename, "w") as f:
        f.write("🌍 Global Final Song Ranking:\n\n")
        for i, (song, votes) in enumerate(global_ranking, start=1):
            line = f"{i}. Song {song}: {votes} votes"
            f.write(line + "\n")
            log(line)

    log(f"💾 Globale rankingbestand opgeslagen: {global_output_filename}")
    remove_existing_drive_file(global_output_filename, OUTPUT_FOLDER_ID)
    upload_to_drive(global_output_filename, OUTPUT_FOLDER_ID)
    log(f"📤 Globale rankingbestand geüpload naar Drive: {global_output_filename}")

    # === LOG UPLOAD ===
    remove_existing_drive_file(log_filename, LOGS_FOLDER_ID)
    upload_to_drive(log_filename, LOGS_FOLDER_ID)
    log(f"📝 Logbestand '{log_filename}' geüpload naar logs folder op Drive.")
    log("🏁 Verwerking voltooid.")

if __name__ == "__main__":
    main()
