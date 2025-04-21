from flask import Flask, jsonify
from confluent_kafka import Producer
import requests
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Configura Kafka con TUS credenciales (usa variables de entorno)
KAFKA_CONFIG = {
  'bootstrap.servers':'d035kvrb92dfgde406p0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092',
  'security.protocol':'SASL_SSL',
  'sasl.mechanism':'SCRAM-SHA-256',
  'sasl.username':'tony',
  'sasl.password':'1234',
}

producer = Producer(KAFKA_CONFIG)
TOPIC = 'acoustic_tracks'  # Tópico personalizado

# URL de tu JSON (pistas acústicas)
JSON_URL = "https://raw.githubusercontent.com/AntonioIR1218/spark-labs/main/results/acoustic_tracks.json"

def delivery_report(err, msg):
    if err:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.info(f"Mensaje enviado a {msg.topic()}: {msg.value().decode('utf-8')}")

@app.route('/send-tracks', methods=['POST'])
def send_tracks():
    try:
        logging.info(f"Descargando datos desde: {JSON_URL}")
        response = requests.get(JSON_URL)
        response.raise_for_status()

        tracks = response.json()  # Parsea el JSON (no JSONL aquí)
        logging.info(f"Total de pistas a enviar: {len(tracks)}")

        for track in tracks:
            message = json.dumps(track)  # Convierte a JSON string
            producer.produce(TOPIC, message.encode('utf-8'), callback=delivery_report)

        producer.flush()
        logging.info("Todas las pistas fueron enviadas correctamente.")
        return jsonify({"status": "success", "message": f"Datos enviados al tópico '{TOPIC}'"}), 200

    except Exception as e:
        logging.error(f"Error al enviar los datos: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    return "ok", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
