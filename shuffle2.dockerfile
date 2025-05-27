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
RUN git clone https://github.com/jensvandereeckt/shuffle_1.git /app/code

# Kopieer de relevante Python scripts naar werkmap
RUN cp /app/code/final-ranking.py . && \
    cp /app/code/final-ranking-mode.py .

# Kopieer eventueel configuratiebestand mee in run command
# Let op: service_account.json moet via volume mount beschikbaar zijn

# Standaardcommando: voer beide scripts uit
CMD ["sh", "-c", "python final-ranking.py && python final-ranking-mode.py"]
