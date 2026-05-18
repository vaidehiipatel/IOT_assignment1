import asyncio
asyncio.set_event_loop(asyncio.new_event_loop())

import faust
import joblib
import numpy as np
import os
import ssl
import certifi
from dotenv import load_dotenv

load_dotenv()

# Use getenv so we can run locally without SASL credentials
BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
API_KEY    = os.environ.get("KAFKA_API_KEY", "")
API_SECRET = os.environ.get("KAFKA_API_SECRET", "")

model = joblib.load("model/co_model.pkl")
print("Model loaded.")

FEATURE_COLS = [
    "PT08.S1(CO)", "PT08.S2(NMHC)", "PT08.S3(NOx)",
    "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
]

# Build SSL context using certifi's trusted CA bundle
ssl_ctx = ssl.create_default_context(cafile=certifi.where())
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE  # temporarily disable for debugging

# Build app kwargs conditionally — omit SASL credentials for local PLAINTEXT Kafka
broker_url = f"kafka://{BOOTSTRAP}"
app_kwargs = {
    "broker": broker_url,
    "value_serializer": "json",
    "topic_replication_factor": 3,
}

if API_KEY and API_SECRET:
    app_kwargs["broker_credentials"] = faust.SASLCredentials(
        username=API_KEY,
        password=API_SECRET,
        ssl_context=ssl_ctx,
        mechanism="PLAIN",
    )

app = faust.App("co-predictor", **app_kwargs)

raw_topic         = app.topic("raw-data")
predictions_topic = app.topic("predictions")


@app.agent(raw_topic)
async def predict(records):
    async for record in records:
        try:
            record = dict(record)
            features = np.array([[record[col] for col in FEATURE_COLS]])
            prediction = model.predict(features)[0]
            output = {
                **record,
                "predicted_CO_mg_m3": round(float(prediction), 4),
            }
            await predictions_topic.send(value=output)
            print(f"[PREDICTED] Row {record.get('row_index')} -> CO = {prediction:.4f} mg/m3")
        except Exception as e:
            print(f"[ERROR] {e} — skipping record")


if __name__ == "__main__":
    app.main()