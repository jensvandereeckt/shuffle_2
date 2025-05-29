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
LOGS_FOLDER_ID = "1hXa-sxiy11T4NKLxWfDodhkliWcQr2Ba"       # logs folder
TOTAL_RUNTIME = 120  # in seconden
CHECK_INTERVAL = 15  # in seconden

# === LOG FUNCTIE ===
def log(message, logfile):
    timestamped = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped)
    with open(logfile, "a") as f:
        f.write(timestamped + "\n")

# === AUTHENTICATE TO GOOGLE DRIVE ===
def authenticate_drive():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials)

# === FUNCTIES ===
def upload_text_file_to_drive(filename, folder_id):
    file_metadata = {
        "name": filename,
        "parents": [folder_id]
    }
    media = MediaFileUpload(filename, mimetype="text/plain")
    return drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

def remove_existing_file(filename, folder_id):
    existing = drive_service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents",
        fields="files(id)"
    ).execute().get("files", [])
    for old_file in existing:
        drive_service.files().delete(fileId=old_file["id"]).execute()

def download_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

def process_vote_file(file, processed_files, total_votes, log_filename):
    filename = file['name']
    file_id = file['id']

    if filename in processed_files:
        return

    country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
    output_filename = f"final_ranking_{country_code}.txt"

    log(f"Downloaden en verwerken van {filename}...", log_filename)
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

    log(f"✅ '{output_filename}' lokaal opgeslagen.", log_filename)

    remove_existing_file(output_filename, OUTPUT_FOLDER_ID)
    upload_text_file_to_drive(output_filename, OUTPUT_FOLDER_ID)
    log(f"'{output_filename}' geüpload naar Google Drive.", log_filename)
    processed_files.add(filename)

def generate_global_ranking(total_votes, log_filename):
    global_output_filename = "global_winner_ranking.txt"
    global_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)

    with open(global_output_filename, "w") as f:
        f.write("Global Final Song Ranking:\n\n")
        for i, (song, votes) in enumerate(global_ranking, start=1):
            line = f"{i}. Song {song}: {votes} votes"
            f.write(line + "\n")

    log(f"🌍 Globale ranking '{global_output_filename}' lokaal opgeslagen.", log_filename)

    remove_existing_file(global_output_filename, OUTPUT_FOLDER_ID)
    upload_text_file_to_drive(global_output_filename, OUTPUT_FOLDER_ID)
    log(f"'{global_output_filename}' geüpload naar Google Drive.", log_filename)

# === MAIN LOOP ===
def main():
    global drive_service
    drive_service = authenticate_drive()
    processed_files = set()
    total_votes = defaultdict(int)
    log_filename = log_filename = f"ranking_log_shuffle2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    start_time = time.time()

    log("🔁 Start loop voor 2 minuten of tot handmatige onderbreking...", log_filename)

    try:
        while time.time() - start_time < TOTAL_RUNTIME:
            query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            for file in files:
                process_vote_file(file, processed_files, total_votes, log_filename)

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Handmatig gestopt.", log_filename)

    generate_global_ranking(total_votes, log_filename)
    upload_text_file_to_drive(log_filename, LOGS_FOLDER_ID)
    log("✅ Logbestand geüpload naar Google Drive.", log_filename)

if __name__ == "__main__":
    main()
