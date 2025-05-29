import json
import io
import re
import time
from collections import defaultdict
from statistics import mode
from datetime import datetime
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

log_filename = f"ranking_log_shuffle2_mode_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def log(msg):
    timestamped = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(timestamped, flush=True)
    with open(log_filename, "a") as f:
        f.write(timestamped + "\n")

def upload_text_file_to_drive(filename, folder_id):
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(filename, mimetype="text/plain")
    return drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

def main():
    global drive_service

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build("drive", "v3", credentials=credentials)

    seen_files = set()
    total_votes = defaultdict(int)
    total_all_votes_flat = []
    start_time = time.time()

    log("🔁 Starten met het controleren van stem-bestanden (mode-methode)...")

    try:
        while time.time() - start_time < RUNTIME_SECONDS:
            query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            for file in files:
                filename = file['name']
                if filename in seen_files:
                    continue
                seen_files.add(filename)
                file_id = file['id']
                country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
                output_filename = f"final_ranking_mode_{country_code}.txt"

                log(f"📄 Bestand gevonden: {filename}")
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                data = json.load(fh)

                country_votes = defaultdict(int)
                country_flat = []

                for entry in data:
                    for vote in entry["votes"]:
                        song = vote["song_number"]
                        count = vote["count"]
                        country_votes[song] += count
                        total_votes[song] += count
                        country_flat.extend([song] * count)
                        total_all_votes_flat.extend([song] * count)

                final_ranking = sorted(country_votes.items(), key=lambda x: x[1], reverse=True)
                most_voted_song = mode(country_flat)
                top_ranked_song = final_ranking[0][0]
                top_ranked_votes = final_ranking[0][1]

                output_lines = [
                    f"🎵 Most voted song using mode(): Song {most_voted_song}",
                    f"🏆 Top-ranked song by total votes: Song {top_ranked_song} with {top_ranked_votes} votes",
                    "✅ Both methods agree on the winner!" if most_voted_song == top_ranked_song else "⚠️ Warning: Mode and total vote count give different winners!"
                ]

                with open(output_filename, "w") as f:
                    for line in output_lines:
                        f.write(line + "\n")

                log(f"💾 Resultaten opgeslagen in {output_filename}")

                # Oud bestand verwijderen
                old_files = drive_service.files().list(
                    q=f"name='{output_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
                    fields="files(id)"
                ).execute().get("files", [])
                for old in old_files:
                    drive_service.files().delete(fileId=old["id"]).execute()

                media = MediaFileUpload(output_filename, mimetype="text/plain")
                drive_service.files().create(
                    body={"name": output_filename, "parents": [OUTPUT_FOLDER_ID]},
                    media_body=media,
                    fields="id"
                ).execute()
                log(f"☁️ Geüpload naar Google Drive: {output_filename}")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Handmatig gestopt door gebruiker.")

    finally:
        if total_votes:
            log("🌍 Verwerken van globale ranking...")
            total_final_ranking = sorted(total_votes.items(), key=lambda x: x[1], reverse=True)
            most_voted_total = mode(total_all_votes_flat)
            top_song_total = total_final_ranking[0][0]
            top_votes_total = total_final_ranking[0][1]

            total_output_lines = [
                f"🌍 TOTAAL: Most voted song using mode(): Song {most_voted_total}",
                f"🌍 TOTAAL: Top-ranked song by total votes: Song {top_song_total} with {top_votes_total} votes",
                "✅ Both methods agree on the global winner!" if most_voted_total == top_song_total else "⚠️ Disagreement between mode and total votes in global ranking."
            ]

            total_filename = "global_winner_ranking_mode.txt"
            with open(total_filename, "w") as f:
                for line in total_output_lines:
                    f.write(line + "\n")
                    log(line)

            old_global = drive_service.files().list(
                q=f"name='{total_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
                fields="files(id)"
            ).execute().get("files", [])
            for old in old_global:
                drive_service.files().delete(fileId=old["id"]).execute()

            media = MediaFileUpload(total_filename, mimetype="text/plain")
            drive_service.files().create(
                body={"name": total_filename, "parents": [OUTPUT_FOLDER_ID]},
                media_body=media,
                fields="id"
            ).execute()
            log(f"📤 Globale ranking geüpload naar Drive als '{total_filename}'")

        upload_text_file_to_drive(log_filename, LOGS_FOLDER_ID)
        log("✅ Logbestand succesvol geüpload.")
        log("🏁 Script afgesloten.")

if __name__ == "__main__":
    main()
