import serial
import time
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os

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
        simlogger.info("TEST OK")
        return True
    else:
        simlogger.info("TEST FAILED")
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

    
        
        



# test()
send_sms("178150665", "hello baby Marcus!")


