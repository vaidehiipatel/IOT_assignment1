import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib
import numpy as np

# Load dataset — UCI Air Quality uses ; delimiter and , as decimal
df = pd.read_csv("data/AirQualityUCI.csv", sep=";", decimal=",")

# Drop unnamed trailing columns (common in this dataset)
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# Drop rows with all NaN (end-of-file artifacts)
df.dropna(how="all", inplace=True)

# Target: CO(GT) — CO concentration in mg/m³
# -200 is the sentinel for missing values in this dataset
df.replace(-200, np.nan, inplace=True)
df.dropna(subset=["CO(GT)"], inplace=True)

# Features: sensor readings (PT08 columns are metal oxide sensor responses)
feature_cols = ["PT08.S1(CO)", "PT08.S2(NMHC)", "PT08.S3(NOx)",
                "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"]

df.dropna(subset=feature_cols, inplace=True)

X = df[feature_cols].copy()
y = df["CO(GT)"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = Pipeline([
    ("scaler", StandardScaler()),
    ("rf", RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1))
])

model.fit(X_train, y_train)
preds = model.predict(X_test)

mae = mean_absolute_error(y_test, preds)
r2 = r2_score(y_test, preds)
print(f"MAE: {mae:.4f} mg/m³")
print(f"R²:  {r2:.4f}")

joblib.dump(model, "model/co_model.pkl")
print("Model saved to model/co_model.pkl")