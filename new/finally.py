import configparser
import datetime
import json
import os
import time
import serial
import modbus_tk.exceptions
import modbus_tk
from modbus_tk import modbus_rtu
import modbus_tk.utils
import modbus_tk.modbus
import modbus_tk.defines as cst
import modbus_tk.modbus_tcp as modbus_tcp
from influxdb import InfluxDBClient
from paho.mqtt import client as mqtt_client
from apscheduler.schedulers.background import BackgroundScheduler

# 讀取config
config = configparser.ConfigParser()
config.read('example.ini')

# MQTT參數
broker = config['MQTT']['broker']
port = config['MQTT']['port']
topic = config['MQTT']['topic']
client_id = config['MQTT']['client_id']
run_MQTT_seconds = config['MQTT']['run_MQTT_seconds']

# Termimal參數--TCP參數
connect_mode = config['Terminal']['connect_mode']
IP = config['Terminal']['IP']
TCP_port = config['Terminal']['TCP_port']

# Termimal參數--RTU參數
COM = config['Terminal']['COM']
Baudrate = config['Terminal']['Baudrate']
Bytesize = config['Terminal']['Bytesize']
Parity = config['Terminal']['Parity']
Stopbits = config['Terminal']['Stopbits']

# Termimal參數--讀取資訊參數
equipment_name = config['Terminal']['equipment_name']
Terminal_name = config['Terminal']['Terminal_name']
Terminal_information_01 = config['Terminal']["Terminal_information_01"]
Terminal_information_02 = config['Terminal']["Terminal_information_02"]
Terminal_information_03 = config['Terminal']["Terminal_information_03"]
Terminal_information_04 = config['Terminal']["Terminal_information_04"]
Terminal_information_05 = config['Terminal']["Terminal_information_05"]
Terminal_information_06 = config['Terminal']["Terminal_information_06"]
Terminal_information_07 = config['Terminal']["Terminal_information_07"]
Terminal_information_08 = config['Terminal']["Terminal_information_08"]
Terminal_information_09 = config['Terminal']["Terminal_information_09"]
Terminal_information_10 = config['Terminal']["Terminal_information_10"]
Terminal_information_11 = config['Terminal']["Terminal_information_11"]
Terminal_information_12 = config['Terminal']["Terminal_information_12"]
Terminal_information_13 = config['Terminal']["Terminal_information_13"]
Terminal_information_14 = config['Terminal']["Terminal_information_14"]
Terminal_inf_seconds = config['Terminal']["Terminal_inf_seconds"]

# local save參數
local_save_seconds = config['local_save']["local_save_seconds"]

# influxDB參數
InfluxDB_host = config['InfluxDB']["InfluxDB_host"]
InfluxDB_port = config['InfluxDB']["InfluxDB_port"]
username = config['InfluxDB']["username"]
password = config['InfluxDB']["password"]
dbname = config['InfluxDB']["dbname"]
influxdb_save_seconds = config['InfluxDB']["influxdb_save_seconds"]

# 開關程式顯示資訊
open_information = config["information"]["open_information"]


def connect_Terminal():  # (read,alarm)
    # 連線Terminal(TCP模式)
    if connect_mode == 'TCP':
        read = []
        alarm = ""
        try:
            # 連接MODBUS TCP從機
            logger = modbus_tk.utils.create_logger("console")
            master = modbus_tcp.TcpMaster(IP, int(TCP_port))
            master.set_timeout(5.0)
            logger.info("connected")
            read = master.execute(1, cst.READ_HOLDING_REGISTERS, 0, 21)
            alarm = "running"
            return list(read), alarm

        except modbus_tk.exceptions.ModbusInvalidResponseError:
            alarm = 'no power'
            return list(read), alarm  # 異常訊息返回

        except Exception as e:
            alarm = (str(e))
            return list(read), alarm  # 異常訊息返回

    # 連線Terminal(RTU模式)
    if connect_mode == 'RTU':
        read = []
        alarm = ""
        try:
            # 基本設定
            master = modbus_rtu.RtuMaster(serial.Serial(port=COM,
                                                        baudrate=int(Baudrate), bytesize=int(Bytesize),
                                                        parity=Parity,
                                                        stopbits=int(Stopbits)))
            master.set_timeout(1)
            master.set_verbose(True)

            # 讀取保持寄存器
            read = master.execute(1, cst.READ_HOLDING_REGISTERS, 0, 21)
            # 1—從設備地址 ,cst.READ_HOLDING_REGISTERS—讀保持寄存器 ,0—開始地址 ,3—讀三個字節
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


# 將讀取數值後放入data這個動作單獨拉出來
# 本機執行時同步顯示Terminal資料，open_information設定0不顯示，設定1顯示
def Terminal_inf():
    inf = connect_Terminal()
    localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if open_information == str(1):
        try:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            inf = connect_Terminal()
            print('當地時間：', now)
            print('連線狀態：', inf[1])
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
        except:
            pass
    try:
        data_for_local = \
            {
                "measurement": equipment_name,
                "fields":
                    {
                        "localtime": localtime,
                        Terminal_information_01: inf[0][0],
                        Terminal_information_02: inf[0][1],
                        Terminal_information_03: inf[0][2],
                        Terminal_information_04: inf[0][3],
                        Terminal_information_05: inf[0][4],
                        Terminal_information_06: inf[0][5],
                        Terminal_information_07: inf[0][6],
                        Terminal_information_08: inf[0][7],
                        Terminal_information_09: inf[0][8],
                        Terminal_information_10: inf[0][9],
                        Terminal_information_11: inf[0][10],
                        Terminal_information_12: inf[0][11],
                        Terminal_information_13: inf[0][12],
                        Terminal_information_14: inf[0][13]
                    }

            }
        data_for_influxdb = \
            [
                {
                    "measurement": equipment_name,
                    "tags": {"topic": Terminal_name},
                    "fields": {
                        Terminal_information_01: inf[0][0],
                        Terminal_information_02: inf[0][1],
                        Terminal_information_03: inf[0][2],
                        Terminal_information_04: inf[0][3],
                        Terminal_information_05: inf[0][4],
                        Terminal_information_06: inf[0][5],
                        Terminal_information_07: inf[0][6],
                        Terminal_information_08: inf[0][7],
                        Terminal_information_09: inf[0][8],
                        Terminal_information_10: inf[0][9],
                        Terminal_information_11: inf[0][10],
                        Terminal_information_12: inf[0][11],
                        Terminal_information_13: inf[0][12],
                        Terminal_information_14: inf[0][13],
                        "connect com": "normal"
                    }
                }
            ]

    except:
        data_for_local = \
            {
                "measurement": equipment_name,
                "tags": {"topic": Terminal_name},
                "fields": {
                    "localtime": localtime,
                    'status': "loss"
                }
            }
        data_for_influxdb = \
            [
                {
                    "measurement": equipment_name,
                    "tags": {"topic": Terminal_name},
                    "fields": {"connect com": "loss"}
                }
            ]

    global Terminal_inf_data
    Terminal_inf_data = [data_for_local, data_for_influxdb]


# 存取Terminal資料到本地端
def local_save():
    print("local_save   :", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    # 系統當前時間年份
    year = time.strftime('%Y', time.localtime(time.time()))
    # 月份
    month = time.strftime('%m', time.localtime(time.time()))
    # 具體時間
    md = time.strftime('%m%d', time.localtime(time.time()))
    path = os.getcwd() + '/FileSave'
    # 判斷目錄是否存在   存在：True   不存在：False
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    else:
        pass
    fileyear = os.getcwd() + '/FileSave/' + '/' + year
    filemonth = fileyear + '/' + month
    if not os.path.exists(fileyear):
        os.mkdir(fileyear)
        os.mkdir(filemonth)
    else:
        if not os.path.exists(filemonth):
            os.mkdir(filemonth)
    filedir = filemonth + '/test_' + md + '.json'

    data = Terminal_inf_data[0]
    with open(filedir, 'a', encoding='utf-8') as file:
        json.dump(data, file)
        file.write('\n')


# 存取Terminal資料到influxDB
def influxdb_save():
    print("influxdb_save:", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    # InfluxDBClient('host', port, 'your_username', 'yuor_password', 'your_dbname')
    client = InfluxDBClient(InfluxDB_host, int(InfluxDB_port), username, password, dbname)
    client.create_database(dbname)
    client = InfluxDBClient(InfluxDB_host, int(InfluxDB_port), username, password, dbname)
    client.write_points(Terminal_inf_data[1])


# 連線到MQTT
def connect_mqtt():
    client = mqtt_client.Client(client_id)
    client.connect(broker, int(port), 5)
    return client


# 堆播到MQTT
def publish(client):
    msg = Terminal_inf_data[0]
    # json.dumps(msg)將msg轉換為json格式
    result = client.publish(topic, json.dumps(msg))
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send msg to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")


# 執行MQTT連線並在推播之後中斷連線
def run_MQTT():
    print("run_MQTT     :", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    client = connect_mqtt()
    client.loop_start()
    publish(client)
    client.loop_stop()


def first_run():
    Terminal_inf()
    local_save()
    influxdb_save()
    run_MQTT()


if __name__ == '__main__':
    # 創建 schedulers
    scheduler = BackgroundScheduler()
    # 添加調度任務
    scheduler.add_job(first_run, 'date', next_run_time=datetime.datetime.now())
    scheduler.add_job(Terminal_inf, 'interval', seconds=float(Terminal_inf_seconds))
    scheduler.add_job(local_save, 'interval', seconds=float(local_save_seconds))
    scheduler.add_job(influxdb_save, 'interval', seconds=float(influxdb_save_seconds))
    scheduler.add_job(run_MQTT, 'interval', seconds=float(run_MQTT_seconds))
    # 啟動調度任務
    scheduler.start()

#     while True:
#         pass
