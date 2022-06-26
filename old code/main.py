import Publish_to_MQTT
import Modbus_message_to_json

second = Modbus_message_to_json.sleeptime(0, 0, 30)
while 1 == 1:
    try:
        Modbus_message_to_json.running()
    except:
        Modbus_message_to_json.error()
    Modbus_message_to_json.preview()
    Publish_to_MQTT.run()
    Modbus_message_to_json.sleep(second)
