import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib
import os

os.makedirs("model", exist_ok=True)

df = pd.read_csv("data/AirQualityUCI.csv", sep=";", decimal=",")
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
df.dropna(how="all", inplace=True)
df.replace(-200, np.nan, inplace=True)
df.dropna(subset=["CO(GT)"], inplace=True)

feature_cols = ["PT08.S1(CO)", "PT08.S2(NMHC)", "PT08.S3(NOx)",
                "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"]

df.dropna(subset=feature_cols, inplace=True)

X = df[feature_cols].copy()

# Convert CO concentration to binary classification
# High CO: above 2.0 mg/m³ (WHO guideline threshold)
threshold = 2.0
y = (df["CO(GT)"] > threshold).astype(int)
print(f"Class distribution — High CO: {y.sum()} | Low CO: {(y==0).sum()}")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

model = Pipeline([
    ("scaler", StandardScaler()),
    ("rf", RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1))
])

model.fit(X_train, y_train)
preds = model.predict(X_test)

accuracy = accuracy_score(y_test, preds)
f1 = f1_score(y_test, preds)

print(f"Accuracy: {accuracy:.4f}")
print(f"F1 Score: {f1:.4f}")
print("\nClassification Report:")
print(classification_report(y_test, preds, target_names=["Low CO", "High CO"]))

joblib.dump(model, "model/co_model.pkl")
print("Model saved to model/co_model.pkl")