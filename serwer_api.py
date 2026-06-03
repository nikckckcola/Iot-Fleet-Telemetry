from flask import Flask, request, jsonify

app = Flask(__name__)

# Na dane
vehicle_cache = {}
alerts_history = []

# Status pojazdu
@app.route('/vehicle/<vehicle_id>/status', methods=['GET'])
def get_vehicle_status(vehicle_id):
    # ID ciężarówki -> jej parametry 
    if vehicle_id in vehicle_cache:
        return jsonify({
            "status": "success",
            "vehicle_id": vehicle_id,
            "data": vehicle_cache[vehicle_id]
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": f"Brak danych telemetrycznych dla pojazdu o ID: {vehicle_id}"
        }), 404

# Przyjęcie zgłoszenia o awarii
@app.route('/alerts', methods=['POST'])
def receive_alert():
    # Konsumer wysyła -> serwer store'uje
    alert_data = request.json
    
    if not alert_data:
        return jsonify({"status": "error", "message": "Puste dane"}), 400

    # Zapisz
    alerts_history.append(alert_data)
    
    # Print informację dla korzystającego
    print(f"\nZarejestrowano nowe zdarzenie: {alert_data}\n")
    
    return jsonify({"status": "success", "message": "Alert przyjęty i zapisany"}), 201

# Dodatkowe funkcje
@app.route('/vehicle/<vehicle_id>/update', methods=['POST'])
def update_vehicle_status(vehicle_id):
    # Aktualizuj
    data = request.json
    if data:
        vehicle_cache[vehicle_id] = data
        return jsonify({"status": "success"}), 200
    return jsonify({"status": "error"}), 400

@app.route('/alerts', methods=['GET'])
def get_all_alerts():
    # Wyświetl wszystkie dotychczasowe alarmy
    return jsonify({
        "status": "success",
        "total_alerts": len(alerts_history),
        "data": alerts_history
    }), 200

if __name__ == '__main__':
    # Uruchomienie serwera - port 5000
    print("Uruchamianie interfejsu [FlaskAPI]")
    app.run(host='0.0.0.0', port=5000, debug=True)