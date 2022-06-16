import serial
import time
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd

LF = chr(10)
CR = chr(13)
CTRLZ = chr(26)

MAX_RETRY = 3

BASE_DIR = os.getcwd()
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = f"{BASE_DIR}/logs/sim.log"
LOGS_RETAINED = 7


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

simlogger = get_logger("sim_logger")
console_handler = get_console_handler()
file_handler = get_file_handler()

simlogger.info("Start")

masterDF = pd.DataFrame()

def test():
    hardwareSerial = serial.Serial('/dev/ttyS0', timeout=5)
    command = "AT+CGMI=?"
    hardwareSerial.write(command.encode())
    # hardwareSerial.write(CR.encode('utf-8'))
    hardwareSerial.write(LF.encode('utf-8'))
    response = []
    for i in range(2):
        response.append(hardwareSerial.readline().decode())
    hardwareSerial.close()

    # print(response)
    test = response[-1].strip()
    if test == "OK":
        simlogger.info("SIM communication TEST OK")
        return True
    else:
        simlogger.info("SIM communication TEST FAILED")
        return False
    

def send_sms(cnumber, message):
    simlogger.info(f"Sending SMS to {cnumber}")
    for retry in range(MAX_RETRY):
        hardwareSerial = serial.Serial('/dev/ttyS0', timeout=5)
        cnumber = f'"{cnumber}"'
        command = 'AT+CMGS='
        command = command+cnumber
        # print(command)
        hardwareSerial.write(command.encode())
        # hardwareSerial.write(CR.encode('utf-8'))
        hardwareSerial.write(LF.encode('utf-8'))
        hardwareSerial.write(message.encode())
        hardwareSerial.write(CTRLZ.encode('utf-8'))

        response = []
        for i in range(3):
            response.append(hardwareSerial.readline().decode())
        hardwareSerial.close()

        response_status = response[-1]
        if response_status.find("+CMGS:") != -1:
            simlogger.info(f"Sending Attempt {retry+1} >> Message sent.")
            break
        else:
            simlogger.error(f"Sending Attempt {retry+1} >> Message sending failed.")

def db_engine():
    USER = 'sa'
    PASS = 'ELIDtech1234'
    HOST = '202.128.57.226'
    PORT = 1433
    DATABASE = 'biostar_ac'
    DRIVER = '/home/elid/sim/sim808_integration/env/lib/python3.9/site-packages/pyodbc.cpython-39-arm-linux-gnueabihf.so'
    return create_engine(
            url="mssql+pymssql://{0}:{1}@{2}:{3}/{4}".format(
                USER, PASS, HOST, PORT, DATABASE
            ))

def disconnect_to_db(con, engine):
    con.close()
    engine.dispose()



def q_loop(engine):
    global masterDF
    target_table = "T_LG202206"
    T_LG_q = f"""SELECT EVTLGUID, DEVUID, DATEADD(SECOND, DEVDT+28800, '19700101') AS DEVDT, 
            USRID, TNAKEY, EVT
            FROM {target_table}"""

    T_USG_q = """SELECT * FROM dbo.T_USR"""
    simlogger.info("Recording state of database.")
    try:
        master_USR_df = pd.read_sql_query(T_USG_q, engine)
        master_LG_df = pd.read_sql_query(T_LG_q, engine)
        rowCount = master_LG_df.shape[0]
        new_record_flag = True
        while True:
            with engine.connect() as con:
                r = con.execute("""SELECT COUNT(*) FROM dbo.T_LG202206
                """)
                for row in r:
                    current_rowCount = row[0]
                    break
            if current_rowCount > rowCount:
                new_record_flag = True
                diff = current_rowCount - rowCount
                simlogger.info(f"{diff} new record detected.")
                master_LG_df = pd.read_sql_query(T_LG_q, engine)
                inserted_recprd = master_LG_df.iloc[-diff, :]
                print(inserted_recprd)
                rowCount = current_rowCount
            else:
                if new_record_flag == True:
                    simlogger.info("No new record detected.")
                    new_record_flag = False
            time.sleep(0.1)
    except Exception as e:
        simlogger.error("Server unavailable or does not exist.")
        simlogger.error(e)
        






def main():
    test_ = test()
    if test_:
        ## loop here for monitoring DB
        
        engine = db_engine()
        q_loop(engine)
       
        
    else:
        simlogger.error("Failed to connect with SIM808.")

if __name__ == "__main__":
    main()
else:
    simlogger.info("main script used as import")