FROM python:3.8-bullseye

WORKDIR /app

# Installeer Java (nodig voor Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Zet omgeving voor Java (Spark heeft JAVA_HOME nodig)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Vereisten installeren (inclusief pyspark)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Repo ophalen van GitHub
RUN git clone https://github.com/jensvandereeckt/shuffle_2.git /app/code && \
    cp /app/code/final_ranking.py . && \
    cp /app/code/final_ranking_mode.py .

# Standaardcommando: voer beide scripts uit
CMD ["sh", "-c", "python final_ranking.py && python final_ranking_mode.py"]
