from flask import Flask, request, jsonify, url_for
from celery import Celery
import requests
import os
import re
from fuzzywuzzy import process

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


CITY_ALIASES = {
    "Киев": "Kyiv",
    "Londn": "London",
    "Токио": "Tokyo"
}

REGION_MAP = {
    "Kyiv": "Europe",
    "London": "Europe",
    "New York": "America",
    "Tokyo": "Asia"
}

API_KEY = "2990be90e3476c3e0b6b6e4068fd7fb8"
WEATHER_API_URL = "http://api.openweathermap.org/data/2.5/weather"

@celery.task(bind=True)
def process_weather_data(self, cities):
    results = {}
    errors = []

   
    normalized_cities = {CITY_ALIASES.get(city, city): city for city in cities}

    for normalized_city, original_city in normalized_cities.items():
        try:
            response = requests.get(
                WEATHER_API_URL,
                params={"q": normalized_city, "appid": API_KEY, "units": "metric"},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            
            if "main" not in data or "temp" not in data["main"]:
                raise ValueError("Incomplete data received from API.")

            temperature = data["main"]["temp"]
            if not (-50 <= temperature <= 50):
                raise ValueError(f"Temperature {temperature} out of range for city {normalized_city}.")

            region = REGION_MAP.get(normalized_city, "Unknown")
            if region not in results:
                results[region] = []

            results[region].append({
                "city": normalized_city,
                "temperature": temperature,
                "description": data["weather"][0]["description"]
            })
        except (requests.RequestException, ValueError) as e:
            errors.append({"city": original_city, "error": str(e)})

    
    task_id = self.request.id
    for region, cities_data in results.items():
        os.makedirs(f"weather_data/{region}", exist_ok=True)
        with open(f"weather_data/{region}/task_{task_id}.json", "w") as f:
            json.dump(cities_data, f)

    
    if errors:
        with open(f"weather_data/task_{task_id}_errors.log", "w") as f:
            json.dump(errors, f)

    return {"results": results, "errors": errors}

@app.route("/weather", methods=["POST"])
def post_weather():
    data = request.get_json()
    if not data or "cities" not in data:
        return jsonify({"error": "Invalid input."}), 400

    cities = data["cities"]
    if not isinstance(cities, list) or not all(isinstance(city, str) for city in cities):
        return jsonify({"error": "Cities must be a list of strings."}), 400

    task = process_weather_data.apply_async(args=[cities])
    return jsonify({"task_id": task.id}), 202

@app.route("/tasks/<task_id>", methods=["GET"])
def get_task_status(task_id):
    task = process_weather_data.AsyncResult(task_id)
    if task.state == "PENDING":
        response = {"status": "pending"}
    elif task.state == "FAILURE":
        response = {"status": "failed", "error": str(task.info)}
    elif task.state == "SUCCESS":
        response = {
            "status": "completed",
            "results_url": {
                region: url_for("get_results", region=region, _external=True)
                for region in task.result["results"]
            },
            "errors": task.result["errors"]
        }
    else:
        response = {"status": task.state}
    return jsonify(response)

@app.route("/results/<region>", methods=["GET"])
def get_results(region):
    task_files = [
        file for file in os.listdir(f"weather_data/{region}")
        if file.startswith("task_") and file.endswith(".json")
    ]

    results = []
    for file in task_files:
        with open(f"weather_data/{region}/{file}") as f:
            results.extend(json.load(f))

    return jsonify({"region": region, "data": results})

if __name__ == "__main__":
    app.run(debug=True)
