import json
import random
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ----------------- CẤU HÌNH KẾT NỐI BROKER -----------------
BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883
TOPIC = "factory/workshop1/tuantrung/sensor_data"
# Đặt Client ID riêng biệt để không bị đá văng với Node-RED
CLIENT_ID = f"python_edge_sender_{random.randint(1000, 9999)}"

# ----------------- KHỞI TẠO MQTT CLIENT -----------------
# Tương thích paho-mqtt phiên bản 2.0+ và fallback bản cũ
try:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv311
    )
except AttributeError:
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[SUCCESS] Đã kết nối tới Broker {BROKER_HOST}:{BROKER_PORT}")
        print(f"[INFO] Topic xuất bản: {TOPIC}")
    else:
        print(f"[ERROR] Kết nối thất bại với mã lỗi (RC): {rc}")


client.on_connect = on_connect

# ----------------- THỰC THI GỬI DỮ LIỆU CHU KỲ -----------------
print("[WAIT] Đang kết nối tới MQTT Broker...")
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

# Khởi chạy luồng mạng nền để duy trì kết nối và nhận ACK
client.loop_start()

counter = 0

try:
    while True:
        counter += 1

        # Mô phỏng dữ liệu cảm biến công nghiệp
        payload_dict = {
            "source": "Python_Script_VSCode",
            "device_id": "EDGE_PYTHON_01",
            "counter": counter,
            "temperature": round(random.uniform(25.0, 40.0), 2),
            "pressure": round(random.uniform(1.0, 5.0), 2),
            "status": "RUNNING",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Chuyển đổi Dictionary sang chuỗi JSON
        payload_json = json.dumps(payload_dict)

        # Gửi dữ liệu với QoS 0
        result = client.publish(TOPIC, payload=payload_json, qos=0, retain=False)

        # Kiểm tra trạng thái đẩy vào hàng đợi
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[SENT #{counter}] Payload: {payload_json}")
        else:
            print(f"[FAILED #{counter}] Không thể đẩy tin nhắn vào hàng đợi.")

        # Chu kỳ gửi 3 giây/lần
        time.sleep(3)

except KeyboardInterrupt:
    print("\n[STOP] Đang dừng tiến trình gửi dữ liệu...")
    client.loop_stop()
    client.disconnect()
    print("[DONE] Đã đóng kết nối an toàn.")