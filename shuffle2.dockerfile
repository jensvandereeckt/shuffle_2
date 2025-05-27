FROM python:3.8-bullseye

# Systeemvereisten installeren
RUN apt-get update && \
   apt-get install -y git && \
   apt-get clean

# Google Drive libraries installeren
RUN pip install --no-cache-dir \
    google-api-python-client \
    google-auth \
    google-auth-httplib2 \
    google-auth-oauthlib

# Werkdirectory
WORKDIR /app

# Repo ophalen van GitHub
RUN git clone https://github.com/jensvandereeckt/shuffle_2.git /app/code && \
    cp /app/code/final_ranking.py . && \
    cp /app/code/final_ranking_mode.py .

# Standaardcommando: voer beide scripts uit
CMD ["sh", "-c", "python final_ranking.py && python final_ranking_mode.py"]
