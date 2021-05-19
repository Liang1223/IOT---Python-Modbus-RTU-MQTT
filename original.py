import time
import json
import serial
import modbus_tk.exceptions
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import random
from paho.mqtt import client as mqtt_client

broker = '13.114.3.126'
port = 1883
topic = "homework"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port, 60)
    return client


def publish(client):
    time.sleep(1)
    with open('test01.json', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        last_line = lines[-1]
    msg = last_line
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send msg to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)
    client.loop_stop()


def mod(PORT="com3"):  # (read,alarm)
    read = []
    alarm = ""
    try:
        # 基本設定
        master = modbus_rtu.RtuMaster(serial.Serial(port=PORT,
                                                    baudrate=9600, bytesize=8, parity='N', stopbits=1))
        master.set_timeout(0.1)
        master.set_verbose(True)

        # 讀取保持寄存器
        read = master.execute(1, cst.READ_HOLDING_REGISTERS, 0,
                              21)  # 1—從設備地址 cst.READ_HOLDING_REGISTERS—讀保持寄存器 0—開始地址 3—讀三個字節
        # READ_COILS = 1 讀線圈
        # READ_DISCRETE_INPUTS = 2 讀離散輸入
        # READ_HOLDING_REGISTERS = 3 【讀保持寄存器】
        # READ_INPUT_REGISTERS = 4 讀輸入寄存器
        alarm = "running"
        return list(read), alarm
    except modbus_tk.exceptions.ModbusInvalidResponseError:
        alarm = 'no power'
        return list(read), alarm  # 異常訊息返回
    except Exception as e:
        alarm = (str(e))
        return list(read), alarm  # 異常訊息返回


def sleeptime(hour, min, sec):
    return hour * 3600 + min * 60 + sec


second = sleeptime(0, 0, 30)
while 1 == 1:
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    inf = mod()
    print('當地時間：', now)
    print('連線狀態：', inf[1])
    try:
        if inf[0][1] == 0:
            print('運轉狀態： 待機中')
        else:
            print('運轉狀態： 工作中')
        if inf[0][2] == 0:
            print('是否異常： 正常')
        else:
            print('是否異常： 異常')
        print('生產數量：', inf[0][3], '個')
        print('異常時間：', inf[0][4], 'min')
        print('待機時間：', inf[0][5], 'min')
        print('運轉時間：', inf[0][6], 'min')
        print('關機時間：', inf[0][7], 'min')
        print('------------------------------')
        data = {"measurement": "test01", "file": [
            {"local time": now, "power": inf[0][0], "status": inf[0][1], "error": inf[0][1], "number": inf[0][3],
             "error time": inf[0][4], "state time": inf[0][5], "running time": inf[0][6], "close time": inf[0][7],
             "new_info01": inf[0][8], "new_info02": inf[0][9], "new_info03": inf[0][10], "new_info04": inf[0][11],
             "new_info05": inf[0][12], "new_info06": inf[0][13]}]}
        with open('Running message.json', 'a', encoding='utf-8') as file:
            json.dump(data, file)
            file.write('\n')
    except:
        data = {"measurement": "test01", 'time': now, 'status': inf[1]}
        with open('test01.json', 'a', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False)
            file.write('\n')
    run()
    time.sleep(second)
