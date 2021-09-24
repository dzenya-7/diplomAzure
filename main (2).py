import pyodbc
from datetime import *
import cv2
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from msrest.authentication import CognitiveServicesCredentials
import time
from threading import Timer, Thread
import serial
import os, uuid, sys
from azure.storage.blob import *

server = "***"
database = "***"
port = "***"
username = "***"
password = '***'
driver = '***'

cnxn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password};TDS_Version=8.0')
cursor = cnxn.cursor()
subscription_key = "***"
endpoint = "***"
computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))
#block_blob_service = BlobServiceClient(account_name='***',
#                                      account_key='***')
block_blob_service = BlobServiceClient.from_connection_string('***')

container_name = 'container'

ser = serial.Serial('/dev/ttyS0', 9600)
time.sleep(2)


def make_image(numder):
    start = time.time()
    print("Thread [" + str(numder) + "]")
    cap = cv2.VideoCapture(numder)
    for i in range(50):
        cap.read()
    ret, frame = cap.read()
    image_name = 'image' + str(numder-1) + '.png'
    cv2.imwrite(image_name, frame)
    rekognition(image_name)
    save_image_to_blob(image_name)
    cap.release()
    end = time.time()
    print("Thread" + str(numder) + "'s speed = " + str(end - start))


def run_threads():
    thread1 = Thread(target=make_image(1))
    #thread2 = Thread(target=make_image(2))
    thread1.start()
    #thread2.start()
    thread1.join()
    #thread2.join()


def delete_all_sens():
    cursor.execute("DELETE FROM sensors_data")


def insert_into_db_sens(temp, gas):
    cursor.execute("INSERT INTO sensors_data (temp,gas, date, time) VALUES (?,?, ?, ?);",
                   (temp, gas, date.today(), datetime.now().time()))
    cnxn.commit()
    print("inserted")


def select_from_db_sens():
    cursor.execute('SELECT * FROM sensors_data')
    for row in cursor.fetchall():
        print(row)

def delete_all_rec():
    cursor.execute("DELETE FROM recognition_data")

def insert_into_db_rec(data_line):
    cursor.execute("INSERT INTO recognition_data (data, date, time) VALUES (?, ?, ?);",
                   (data_line, date.today(), datetime.now().time()))
    cnxn.commit()
    print("inserted")


def select_from_db_rec():
    cursor.execute('SELECT * FROM recognition_data')
    for row in cursor.fetchall():
        print(row)


def rekognition(image_name):
    local_image = open(image_name, "rb")
    tags_result_local = computervision_client.tag_image_in_stream(local_image)
    string1 = ''
    print("Tags in the local image: ")
    if len(tags_result_local.tags) == 0:
        string1 += "No tags detected."
    else:
        for tag in tags_result_local.tags:
            string1 += "'{}' with confidence {:.2f}%;".format(tag.name, tag.confidence * 100)
    s = string1.replace(" with confidence ", "=")
    string1 = s.replace("'", "")
    s = string1.replace(" ", "")
    print(len(s))
    print(s)
    insert_into_db_rec(s)


def loop():
    get_data()
    t = Timer(3600.0, loop)
    t.start()


def get_data():
    ser.write(b'1')
    response1 = ser.readline().decode('UTF-8')
    print(response1)
    response2 = ser.readline().decode('UTF-8')
    print(response2)
    s1 = response1.replace("Temp: ", "")
    s1 = s1.replace("\n", "")
    s1 = s1.replace("\n", "")
    s2 = response2.replace("Gas %: ", "")
    s2 = s2.replace("\n", "")
    insert_into_db_sens(s1, s2)


def save_image_to_blob(local_file_name):
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    local_path = os.path.abspath(os.path.curdir)
    full_path_to_file = os.path.join(local_path, local_file_name)
    print("Temp file = " + full_path_to_file)
    print("\nUploading to Blob storage as blob" + local_file_name)
    block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)


def main():
    try:
        while 1:
            response = ser.readline()
            print(response)
            if response == b'make photo\r\n':
                get_data()
                run_threads()
    except KeyboardInterrupt:
        ser.close()

if __name__ == '__main__':
    #get_data()
    run_threads()
    #loop()

