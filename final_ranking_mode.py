import json
import io
import re
from collections import defaultdict
from statistics import mode
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

# === HAAL ALLE REDUCED STEMFILES OP ===
print(" Zoeken naar bestanden met prefix 'reduced_votes_'...")
query = f"'{INPUT_FOLDER_ID}' in parents and name contains 'reduced_votes_' and name contains '.json'"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception("❌ Geen stem-bestanden gevonden in de Drive-map.")

total_votes = defaultdict(int)
total_all_votes_flat = []

for file in files:
    filename = file['name']
    file_id = file['id']
    country_code = filename.replace("reduced_votes_", "").replace(".json", "").lower()
    output_filename = f"final_ranking_mode_{country_code}.txt"

    print(f" Downloaden van {filename}...")
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    data = json.load(fh)

    # Verwerk stemmen per land
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

    print(f" '{output_filename}' lokaal opgeslagen.")

    # Verwijder eventueel bestaand bestand met dezelfde naam
    existing_files = drive_service.files().list(
        q=f"name='{output_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
        fields="files(id)"
    ).execute().get("files", [])

    for old_file in existing_files:
        drive_service.files().delete(fileId=old_file["id"]).execute()
        print(f" Oud bestand '{output_filename}' verwijderd van Drive.")

    # Upload nieuw bestand
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
    print(f" Bestand '{output_filename}' succesvol geüpload naar Google Drive.")

# === TOTAAL VOOR ALLE LANDEN ===
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

# Verwijder eventueel bestaand totaalbestand
existing_total = drive_service.files().list(
    q=f"name='{total_filename}' and '{OUTPUT_FOLDER_ID}' in parents",
    fields="files(id)"
).execute().get("files", [])

for old_file in existing_total:
    drive_service.files().delete(fileId=old_file["id"]).execute()
    print(f" Oud bestand '{total_filename}' verwijderd van Drive.")

# Upload totaalbestand
file_metadata = {
    "name": total_filename,
    "parents": [OUTPUT_FOLDER_ID]
}
media = MediaFileUpload(total_filename, mimetype="text/plain")
drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

print(f" Globaal rankingbestand '{total_filename}' geüpload naar Drive.")
