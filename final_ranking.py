import time
import json
import io
import re
from datetime import datetime
from collections import defaultdict
from pyspark.sql import SparkSession
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1dvuwVEPnCEiN7uK1lkTl8ZZL_lAta5kB"  # generated_votes
OUTPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"  # reduced_votes
LOGS_FOLDER_ID = "1hXa-sxiy11T4NKLxWfDodhkliWcQr2Ba"     # logs folder
CHECK_INTERVAL = 15  # seconden
TOTAL_RUNTIME = 120  # seconden
COUNTRY_FILTER = ["be"]

# === LOGGING FUNCTIE ===
LOG_FILENAME = f"vote_log_shuffle1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def log(message):
    timestamped = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped, flush=True)
    with open(LOG_FILENAME, "a") as f:
        f.write(timestamped + "\n")

# === GOOGLE DRIVE AUTHENTICATIE ===
def authenticate_drive():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials)

# === SPARK SETUP ===
def init_spark():
    return SparkSession.builder.appName("SongVoteCount").master("local[*]").getOrCreate()

# === DRIVE FILE MANAGEMENT ===
def get_vote_files(drive_service, seen_files):
    query = f"'{INPUT_FOLDER_ID}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])
    pattern = re.compile(r"^generated_votes_([a-z]{2})\.txt$")
    filtered = []
    for item in items:
        match = pattern.match(item["name"])
        if match:
            country_code = match.group(1)
            if country_code in COUNTRY_FILTER and item["name"] not in seen_files:
                filtered.append((item["name"], item["id"]))
    return filtered

def download_file(drive_service, file_id, filename):
    request = drive_service.files().get_media(fileId=file_id)
    with open(filename, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def upload_to_drive(drive_service, filename, folder_id):
    metadata = {"name": filename, "parents": [folder_id]}
    mimetype = "application/json" if filename.endswith(".json") else "text/plain"
    media = MediaFileUpload(filename, mimetype=mimetype)
    return drive_service.files().create(body=metadata, media_body=media, fields="id").execute()

def remove_old_drive_file(drive_service, filename):
    query = f"name='{filename}' and '{OUTPUT_FOLDER_ID}' in parents"
    old_files = drive_service.files().list(q=query, fields="files(id)").execute().get("files", [])
    for file in old_files:
        drive_service.files().delete(fileId=file["id"]).execute()

# === STEMMEN VERWERKEN ===
def process_votes(spark, input_filename):
    rdd = spark.sparkContext.textFile(input_filename)
    mapped = rdd.map(lambda line: line.strip().split("\t")) \
                .filter(lambda fields: len(fields) >= 3 and fields[2].isdigit()) \
                .map(lambda fields: ((fields[0], fields[2]), 1))
    reduced = mapped.reduceByKey(lambda a, b: a + b)
    grouped = reduced.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                     .groupByKey() \
                     .mapValues(list)
    result = grouped.map(lambda x: {
        "country": x[0],
        "votes": [{"song_number": song, "count": count} for song, count in x[1]]
    }).collect()
    return result

# === MAIN LOOP ===
def main():
    spark = init_spark()
    drive_service = authenticate_drive()
    seen_files = set()
    start_time = time.time()

    log("🟢 Start stemverwerking voor 2 minuten (Ctrl+C om te stoppen)...")

    try:
        while time.time() - start_time < TOTAL_RUNTIME:
            new_files = get_vote_files(drive_service, seen_files)

            if new_files:
                for filename, file_id in new_files:
                    log(f"📄 Nieuw bestand gevonden: {filename}")
                    download_file(drive_service, file_id, filename)
                    seen_files.add(filename)

                    log("⚙️ Verwerken met Spark...")
                    result = process_votes(spark, filename)

                    output_filename = filename.replace("generated_votes", "reduced_votes").replace(".txt", ".json")
                    with open(output_filename, "w") as f:
                        json.dump(result, f, indent=4)

                    log(f"💾 Bestand opgeslagen: {output_filename}")
                    remove_old_drive_file(drive_service, output_filename)
                    response = upload_to_drive(drive_service, output_filename, OUTPUT_FOLDER_ID)
                    log(f"☁️ Upload voltooid naar Drive (ID: {response.get('id')})")
            else:
                log("⏳ Geen nieuwe bestanden gevonden, opnieuw proberen...")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        log("🛑 Handmatig gestopt.")
    except Exception as e:
        log(f"❌ Fout opgetreden: {e}")
    finally:
        spark.stop()
        log("🏁 Spark sessie afgesloten.")

        try:
            response = upload_to_drive(drive_service, LOG_FILENAME, LOGS_FOLDER_ID)
            log(f"📤 Logbestand geüpload naar Drive (ID: {response.get('id')})")
        except Exception as e:
            log(f"⚠️  Upload logbestand mislukt: {e}")

# === START ===
if __name__ == "__main__":
    main()
