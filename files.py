#Import required libraries 
import os
import time
from pathlib import Path
from threading import Timer
from sqlalchemy import create_engine, MetaData, Table, Column, Integer

#Create a special Timer sub-class that repeatedly executes the function after a certain time interval
class RepeatTimer(Timer):
  def run(self):
    while not self.finished.wait(self.interval):
      self.function(*self.args, **self.kwargs)

#Creates an engine (interface) to the given database
db = create_engine('mysql://<user>:<password>@<host>[:<port>]/files')
#Details can be filled as per the MySQL server being run

#Create a MetaData container object that binds features of the database being described
md_obj = MetaData()

#Define the table "status"
status = Table('status',md_obj,
  Column('update_id', Integer, primary_key=True,autoincrement=True),
  Column('update_status',Integer,nullable=False)
)

#Create the tables in the database if they don't exist
md_obj.create_all(db)

#Keeps count of the files being created
fc = 0

#Function to create files in "processing"
def file_generator():
  global fc
  f = open(os.path.join(Path("processing").absolute(),"{}.txt".format(fc)),"x")
  fc += 1

#Function to check if "queue" is empty and move all files in "processing" to the "queue"
def file_queue():
  if len(os.listdir(Path("queue").absolute())) == 0:
    for i in os.listdir(Path("processing").absolute()):
      os.replace(os.path.join(Path("processing").absolute(),i),os.path.join(Path("queue").absolute(),i))

#Function to move files from "queue" to "processed" and update the status table with 1 if files are moved and 0 if there are no files to move
def file_process():
  global status
  global db
  conn = db.connect()
  if len(os.listdir(Path("queue").absolute())) != 0:
    for i in os.listdir(Path("queue").absolute()):
      os.replace(os.path.join(Path("queue").absolute(),i),os.path.join(Path("processed").absolute(),i))
      update_status = 1
  else:
    update_status = 0
  ins = status.insert()
  conn.execute(ins,{"update_status":update_status})

#Create the directories
os.mkdir(os.path.join(Path().absolute(),"processing"))
os.mkdir(os.path.join(Path().absolute(),"queue"))
os.mkdir(os.path.join(Path().absolute(),"processed"))

#Create the threads
t1 = RepeatTimer(1.0,file_generator)
t2 = RepeatTimer(5.0,file_queue)
t3 = RepeatTimer(5.0,file_process)

#Start the threads
t1.start()
t2.start()
t3.start()

#Stop the execution of the main program for given number of seconds
time.sleep(31)

#Stop the execution of the threads
t1.cancel()
t2.cancel()
t3.cancel()