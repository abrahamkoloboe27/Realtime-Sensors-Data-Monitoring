
class SensorData():
    def __init__(self, sensor_id,timestamp, temperature, humidity, pressure, location):
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity
        self.pressure = pressure
        self.location = location
    def __str__(self):
        return f"Id : {self.sensor_id}:{self.timestamp} - Temperature : {self.temperature}"