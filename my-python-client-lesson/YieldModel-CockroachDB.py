import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import joblib  # Import joblib for saving the model
import psycopg2


# Load the dataset
file_path = "C:\Users\SST-LAB\Desktop\Bilau-DAT608\DAT608Project\yielddata.csv"
data = pd.read_csv(file_path)

# Define features and target
X = data[['Temperature (Celsius)', 'Pesticides (tonnes)']]
y = data['Yield (hg/ha)']

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a Random Forest model
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Model trained. MAE: {mae}, R2: {r2}")

# Save the model to disk
joblib.dump(model, 'random_forest_model.pkl')


