import serial
import time
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import queue
import threading
import time
import random
import datetime
import warnings
import RPi.GPIO as GPIO


warnings.filterwarnings('ignore')

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler
def get_file_handler():
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler
def get_logger(logger_name):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.INFO) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger

def test():
    hardwareSerial = serial.Serial('/dev/ttyS0', timeout=5)
    command = "AT+CGMI=?"
    hardwareSerial.write(command.encode())
    hardwareSerial.write(CR.encode('utf-8'))
    hardwareSerial.write(LF.encode('utf-8'))
    response = []
    for i in range(2):
        response.append(hardwareSerial.readline().decode())
    hardwareSerial.close()

    print(response)
    test = response[-1].strip()
    if test == "OK":
        simlogger.info("SIM communication TEST OK")
        return True
    else:
        simlogger.info("SIM communication TEST FAILED")
        return False
    
def send_sms(value):
    _id, cnumber, _name, _time, _room, _status = value
    message = f"""La Salle Greenhills\nNOTIFYI System\nID Number:{_id}\nName: {_name}\nRoom:{_room}\nTime: {_time}\nStatus: {_status}\n"""
    simlogger.info(f"Sending SMS to {cnumber}")
    for retry in range(MAX_RETRY):
        hardwareSerial = serial.Serial('/dev/ttyS0', timeout=5)
        cnumber = f'"{cnumber}"'
        command = 'AT+CMGS='
        command = command+cnumber
        response = []
        # print(command)
        hardwareSerial.write(command.encode())
        # hardwareSerial.write(CR.encode('utf-8'))
        hardwareSerial.write(LF.encode('utf-8'))
        for i in range(2):
            response.append(hardwareSerial.readline().decode())
        hardwareSerial.write(message.encode())
        hardwareSerial.write(CTRLZ.encode('utf-8'))
        hardwareSerial.write(LF.encode('utf-8'))
        for i in range(12):
            response.append(hardwareSerial.readline().decode())
        hardwareSerial.close()
        print("SIM808 Response: ", response)
        for response_status in response:
            if response_status.find("+CMGS:") != -1:
                simlogger.info(f"Sending Attempt {retry+1} >> Message sent.")
                return
            else:
                pass
        simlogger.error(f"Sending Attempt {retry+1} >> Message sending failed.")

def db_engine():
    USER = 'sa'
    PASS = 'ELIDtech1234'
    HOST = '192.168.0.105'
    PORT = 1433
    DATABASE = 'biostar_ac'
    DRIVER = '/home/elid/sim/sim808_integration/env/lib/python3.9/site-packages/pyodbc.cpython-39-arm-linux-gnueabihf.so'
    return create_engine(
            url="mssql+pymssql://{0}:{1}@{2}:{3}/{4}".format(
                USER, PASS, HOST, PORT, DATABASE
            ))

def monitor_que_obj():
    global empty_que_flag
    while True:
        try:
            if my_queue.empty():
                if empty_que_flag == False:
                    simlogger.info("Que empty. Waiting for new entry.")
                    empty_que_flag = True
            else:
                empty_que_flag = False
                qsize =  my_queue.qsize()
                simlogger.info(f"que_size: {qsize}")
                value = my_queue.get(block=False)
                ## execute Que activity here
                ## trigger sending sms here!
                simlogger.info("Processing que:")
                simlogger.info(value)
                send_sms(value)
        except Exception as e:
            simlogger.error("Que monitoring Thread Error.")
            simlogger.error(e)
            
def q_loop():
    while True:
        global masterDF
        engine = db_engine()
        target_table = "T_LG202206"
        T_LG_q = f"""SELECT USRGRUID, DATEADD(SECOND, DEVDT+28800, '19700101') AS DEVDT, 
                USRID, TNAKEY
                FROM {target_table}"""
        T_USG_q = """SELECT USRID, PH, NM FROM dbo.T_USR"""
        T_USRGR_q = """SELECT USRGRUID, NM FROM dbo.T_USRGR"""
        simlogger.info("Recording state of database.")
       
        try:
            while True:
                master_LG_df = pd.read_sql_query(T_LG_q, engine)
                rowCount = master_LG_df.shape[0]
                new_record_flag = True
                GPIO.output(RED, OFF) 
                GPIO.output(GREEN, ON)
                v = 0
                while True:
                    try:
                        with engine.connect() as con:
                            r = con.execute("""SELECT COUNT(*) FROM dbo.T_LG202206
                            """)
                            for row in r:
                                current_rowCount = row[0]
                                break
                        if current_rowCount > rowCount:
                            new_record_flag = True
                            diff = current_rowCount - rowCount
                            simlogger.info(f"{diff} new record detected. Adding to que.")
                            master_LG_df = pd.read_sql_query(T_LG_q, engine)
                            inserted_record = master_LG_df.iloc[-diff:, :]
                            inserted_record.loc[:, ["DEVDT"]] = inserted_record["DEVDT"].dt.strftime('%Y-%m-%d %H:%M:%S').values
                            master_USR_df = pd.read_sql_query(T_USG_q, engine)
                            room_df = pd.read_sql_query(T_USRGR_q, engine)
                            inserted_record_ = master_USR_df.merge(inserted_record, on=["USRID"], how="inner")
                            inserted_record_ = inserted_record_.merge(room_df, on=["USRGRUID"], how="inner")
                            inserted_record_ = inserted_record_.drop(["USRGRUID"], axis=1)
                            inserted_record_.loc[inserted_record_['TNAKEY'] == 1, 'status'] = 'IN'
                            inserted_record_.loc[inserted_record_['TNAKEY'] == 2, 'status'] = 'OUT'
                            inserted_record_ = inserted_record_.drop(["TNAKEY"], axis=1)
                            inserted_record_.columns = ["id", "cnumber", "name", "datetime","room", "status"]
                            print(inserted_record_)
                            to_queue = []
                            # add record to que
                            for v in inserted_record_.values:
                                to_queue.append(v)
                            
                            print(to_queue)
                            my_queue.put(v)
                            ##
                            
                            rowCount = current_rowCount
                        else:
                            if new_record_flag == True:
                                simlogger.info("No new record detected.")
                                new_record_flag = False
                        time.sleep(0.1)
                        # if v == 1:
                        #     print("Value Hit")
                        #     time.sleep(10)
                        #     v = random.randint(0, 9)
                        # else:
                        #     time.sleep(0.5)
                        #     v = random.randint(0, 9)
                        #     print("Random generated: ", v)
                        #     my_queue.put(v)
                    except Exception as e:
                        simlogger.error("Something went wrong!")
                        simlogger.error(e)
                        simlogger.warning("Retry connection in 10 sec.")
                        GPIO.output(GREEN, OFF)
                        for i in range(10):
                            GPIO.output(RED, ON) 
                            time.sleep(0.2)
                            GPIO.output(RED, OFF)
                            time.sleep(0.7) 
                            
        except Exception as e:
            simlogger.error("Server unavailable or does not exist.")
            simlogger.error(e)
            simlogger.warning("Retry connection in 10 sec.")
            GPIO.output(GREEN, OFF)
            for i in range(10):
                GPIO.output(RED, ON) 
                time.sleep(0.2)
                GPIO.output(RED, OFF)
                time.sleep(0.7) 
        
def start_que_trhreading():
    try:
        simlogger.info("Que monitoring Thread begin.")
        x = threading.Thread(target=monitor_que_obj, daemon=True)
        x.start()
    except Exception as e:
        simlogger.error("Que monitoring Thread Error.")
        simlogger.error(e)
        

def main():
    global my_queue
    while True:
        test_ = test()
        if test_:
            ## loop here for monitoring DB 
            start_que_trhreading()
            q_loop()   
        else:
            simlogger.error("Failed to connect with SIM808.")
            simlogger.warning("Retrying in 10 sec.")
            GPIO.output(GREEN, OFF)
            for i in range(10):
                GPIO.output(RED, ON) 
                time.sleep(0.2)
                GPIO.output(RED, OFF)
                time.sleep(0.7) 


GPIO.setmode(GPIO.BCM)



# GPIO.output(27, GPIO.LOW)

# GPIO.output(22, GPIO.HIGH)
# GPIO.output(27, GPIO.HIGH)
RED = 22
GREEN = 27
ON = GPIO.LOW
OFF = GPIO.HIGH
GPIO.setup(RED, GPIO.OUT)
GPIO.setup(GREEN, GPIO.OUT)
GPIO.output(RED, GPIO.LOW) 
GPIO.output(GREEN, GPIO.HIGH)

LF = chr(10)
CR = chr(13)
CTRLZ = chr(26)

MAX_RETRY = 3

BASE_DIR = os.getcwd()
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = f"{BASE_DIR}/logs/sim.log"
LOGS_RETAINED = 7

simlogger = get_logger("sim_logger")
console_handler = get_console_handler()
file_handler = get_file_handler()
simlogger.info("Start")
masterDF = pd.DataFrame()

my_queue = queue.Queue()
empty_que_flag = False

if __name__ == "__main__":
    main()
        
else:
    simlogger.info("main script used as import")
