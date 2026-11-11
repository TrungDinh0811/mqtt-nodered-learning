import json
import random
import paho.mqtt.client as mqtt

# ----------------- CẤU HÌNH KẾT NỐI BROKER -----------------
BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883
TOPIC = "factory/workshop1/tuantrung/sensor_data"
# Client ID ngẫu nhiên riêng biệt để không bị broker kick kết nối
CLIENT_ID = f"python_terminal_sub_{random.randint(1000, 9999)}"

# ----------------- HÀM XỬ LÝ SỰ KIỆN (CALLBACKS) -----------------
def on_connect(client, userdata, flags, rc, properties=None):
    """Kích hoạt khi client kết nối thành công hoặc thất bại với Broker."""
    if rc == 0:
        print("=" * 60)
        print(f"[SUCCESS] Đã kết nối Broker: {BROKER_HOST}:{BROKER_PORT}")
        print(f"[INFO] Client ID: {CLIENT_ID}")
        # Đăng ký nhận tin nhắn trên Topic với QoS 0
        client.subscribe(TOPIC, qos=0)
        print(f"[SUBSCRIBED] Đang lắng nghe trên topic: '{TOPIC}'")
        print("=" * 60)
        print("Đang chờ gói tin mới... (Nhấn Ctrl+C để thoát)\n")
    else:
        print(f"[ERROR] Kết nối Broker thất bại, mã lỗi (RC): {rc}")


def on_message(client, userdata, msg):
    """Kích hoạt bất cứ khi nào có tin nhắn mới gửi tới Topic đã đăng ký."""
    try:
        # Giải mã chuỗi bytes nhận được từ mạng sang UTF-8
        raw_payload = msg.payload.decode("utf-8")
        
        # Parse chuỗi JSON sang Dictionary trong Python
        data = json.loads(raw_payload)

        # In thông tin đã được định dạng ra terminal
        print(f"📩 [MESSAGE RECEIVED] Topic: {msg.topic} | QoS: {msg.qos}")
        print(f"   ├─ Nguồn phát : {data.get('source', 'Node-RED / Khác')}")
        print(f"   ├─ Thiết bị   : {data.get('device_id', 'N/A')}")
        print(f"   ├─ Trạng thái : {data.get('status', 'N/A')}")
        
        # Nếu là gói tin từ publisher.py có chứa nhiệt độ & áp suất
        if "temperature" in data and "pressure" in data:
            print(f"   ├─ Nhiệt độ   : {data.get('temperature')} °C")
            print(f"   ├─ Áp suất    : {data.get('pressure')} Bar")
            print(f"   └─ Đếm gói    : #{data.get('counter')}")
        else:
            # Nếu là gói tin đơn giản từ Node-RED
            print(f"   └─ Nội dung   : {data.get('message')}")
            
        print(f"   ⏰ Timestamp  : {data.get('timestamp')}")
        print("-" * 60)

    except json.JSONDecodeError:
        # Trường hợp tin nhắn nhận được là text thô, không phải JSON
        print(f"📩 [RAW TEXT] Topic: {msg.topic} | Payload: {msg.payload.decode('utf-8')}")
        print("-" * 60)
    except Exception as e:
        print(f"[ERROR] Lỗi xử lý gói tin: {e}")


# ----------------- KHỞI TẠO CLIENT & LẮNG NGHE -----------------
try:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv311
    )
except AttributeError:
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)

client.on_connect = on_connect
client.on_message = on_message

print("[WAIT] Đang kết nối tới MQTT Broker...")
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

try:
    # loop_forever() sẽ giữ tiến trình chạy vô hạn để lắng nghe liên tục
    client.loop_forever()
except KeyboardInterrupt:
    print("\n[STOP] Đang ngắt kết nối subscriber...")
    client.disconnect()
    print("[DONE] Đã thoát chương trình an toàn.")