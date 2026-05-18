# Real-Time CO Prediction Pipeline
## ENGR 5785G — Assignment 1

A real-time streaming application using Apache Kafka that reads rows from the UCI Air Quality dataset, streams them as live events, applies a Random Forest ML model using the Faust Streams API, and publishes predictions to an output topic.

---

## Dataset

**Air Quality UCI Dataset**  
Source: https://archive.ics.uci.edu/dataset/360  
9358 hourly air quality readings from sensors in an Italian city (2004–2005).  
Features used: PT08.S1(CO), PT08.S2(NMHC), PT08.S3(NOx), PT08.S4(NO2), PT08.S5(O3), T, RH, AH  
Target: CO concentration in mg/m³

---

## Streams Library

**Python + Faust (faust-streaming 0.11.3)**  
Uses the `@app.agent` decorator to define a streaming processor — not a plain consumer loop.

---

## ML Model

- Algorithm: Random Forest Regressor (100 estimators)
- Training split: 80% train / 20% test
- MAE: 0.27 mg/m³
- R²: 0.91
- Model file: `model/co_model.pkl`
- Trained offline in `model/train_model.py`, loaded once at startup in the Faust processor

---

## Project Structure

```
IoT_assignment1/
├── model/
│   ├── train_model.py       # offline model training script
│   └── co_model.pkl         # trained model file
├── producer.py              # reads dataset, publishes to raw-data topic
├── processor.py             # Faust Streams processor, runs ML inference
├── consumer.py              # reads predictions topic, prints results
├── requirements.txt
└── README.md
```

---

## Setup

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/IoT_assignment1.git
cd IoT_assignment1
```

**2. Create and activate a conda environment**
```bash
conda create -n kafka_env python=3.11 -y
conda activate kafka_env
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Create a .env file in the project root**
```
KAFKA_BOOTSTRAP_SERVERS=your_confluent_bootstrap_server:9092
KAFKA_API_KEY=your_api_key
KAFKA_API_SECRET=your_api_secret
```

**5. Download the dataset**  
Download AirQualityUCI.csv from https://archive.ics.uci.edu/dataset/360  
Place it in the `data/` folder.

**6. Train the model**
```bash
python model/train_model.py
```

---

## Running the Pipeline

Open three terminals, activate `kafka_env` in each, and navigate to the project folder.

**Terminal 1 — Start consumer first:**
```bash
python consumer.py
```

**Terminal 2 — Start Faust processor:**
```bash
faust -A processor worker -l info
```
Wait until you see `[^Worker]: Ready` before starting the producer.

**Terminal 3 — Start producer:**
```bash
python producer.py
```

---

## Expected Output

**Producer (Terminal 3):**
```
Streaming 8991 rows to 'raw-data' topic...
[0] Sent: row 0
[1] Sent: row 1
```

**Processor (Terminal 2):**
```
[PREDICTED] Row 0 -> CO = 1.4440 mg/m3
[PREDICTED] Row 1 -> CO = 1.1440 mg/m3
```

**Consumer (Terminal 1):**
```
[10:10:27] Row 0 | Temp: 13.6C | Predicted CO: 1.4440 mg/m3
[10:10:28] Row 1 | Temp: 12.1C | Predicted CO: 1.1440 mg/m3
```

---

## Kafka Topics

| Topic        | Purpose                           |
|--------------|-----------------------------------|
| raw-data     | Raw sensor readings from producer |
| predictions  | ML predictions from processor     |

---

## Dependencies

| Package         | Version |
|-----------------|---------|
| faust-streaming | 0.11.3  |
| aiokafka        | 0.10.0  |
| kafka-python    | 2.0.2   |
| scikit-learn    | 1.4.2   |
| pandas          | 2.2.2   |
| joblib          | 1.4.0   |
| python-dotenv   | 1.0.1   |
| certifi         | latest  |

---

## Video Demo

[Link to video demo](https://1drv.ms/v/c/79651e8f65c0a4a8/IQDjf0zf3dKtQ52kOdyLTf6xAXNbqinAoA1--Zlq4IINTcc?e=c2FuNm)

---

## Notes

- Confluent Cloud free tier clusters pause after inactivity. Resume the cluster from the Confluent UI before running.
- The `.env` file is not committed. You must create it manually with your own Confluent Cloud credentials.
- The `data/` folder is not committed. Download the dataset separately.
