from airflow import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import datetime
from airflow.decorators import dag, task
from airflow.decorators import dag, task
import sys
import sys
import maadstml
import maadstml
import tsslogging
import tsslogging
import os
import os
import subprocess
import subprocess
import json 
import json 
import time
import time
import random 
import random 
import threading
import threading
from contextlib import contextmanager
from contextlib import contextmanager
from contextlib import ExitStack
from contextlib import ExitStack
import re
import re


sys.dont_write_bytecode = True
sys.dont_write_bytecode = True
######################################## USER CHOOSEN PARAMETERS ########################################
######################################## USER CHOOSEN PARAMETERS ########################################
default_args = {
default_args = {
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'microserviceid' : '', # <<< *** leave blank
  'microserviceid' : '', # <<< *** leave blank
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'inputfile' : '',#'/rawdatademo/cisco_network_data.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'inputfile' : '',#'/rawdatademo/cisco_network_data.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'topicid' : '-999', # <<< ********* do not modify  
  'topicid' : '-999', # <<< ********* do not modify  
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'docfolder' : '/rawdatademo/mylogsdemo,/rawdatademo/mylogs2demo', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'docfolder' : '/rawdatademo/mylogsdemo,/rawdatademo/mylogs2demo', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'doctopic' : 'rtms-stream-mylogs,rtms-stream-mylogs2',  # This is the topic that will contain the docfolder file data
  'doctopic' : 'rtms-stream-mylogs,rtms-stream-mylogs2',  # This is the topic that will contain the docfolder file data
  'chunks' :3000, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'chunks' :3000, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
}
}


######################################## DO NOT MODIFY BELOW #############################################
######################################## DO NOT MODIFY BELOW #############################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.decorators import dag, task
import sys
import maadstml
import tsslogging
import os
import subprocess
import json 
import time
import random 
import threading
from contextlib import contextmanager
from contextlib import ExitStack
import re

sys.dont_write_bytecode = True
######################################## USER CHOOSEN PARAMETERS ########################################
default_args = {
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'microserviceid' : '', # <<< *** leave blank
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'inputfile' : '',#'/rawdatademo/cisco_network_data.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'topicid' : '-999', # <<< ********* do not modify  
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'docfolder' : '/rawdatademo/mylogsdemo,/rawdatademo/mylogs2demo', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'doctopic' : 'rtms-stream-mylogs,rtms-stream-mylogs2',  # This is the topic that will contain the docfolder file data
  'chunks' :3000, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
}

######################################## DO NOT MODIFY BELOW #############################################

# This sets the lat/longs for the IoT devices so it can be map
VIPERTOKEN=""
VIPERHOST=""
VIPERPORT=""

def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        try:
          if chunk_size != 0:
            data = file_object.read(chunk_size).decode('utf-8')            
            if len(data)>0 and data[-1] != ' ':
                 ct=0
                 for c in reversed(data):
                   if c == ' ':
                        break
                   ct = ct +1
                 if ct < len(data):
                   file_object.seek(file_object.tell()-ct)
                   data = data[:len(data)-ct]
          else:
            data = file_object.readline().decode('utf-8')            
          data=data.replace('"','').replace("'","").replace("\\n"," ").replace('\n'," ").replace("\\r"," ").replace('\r'," ").replace(';'," ").replace('&'," ").strip()
          if not data:
               break
          yield data          
        except Exception as e:
           break

def readallfiles(fd,tr,cs=1024):
  args=default_args
  producerid='userfilestream'
  print("fd=",fd.name)
  for piece in read_in_chunks(fd,cs):
        piece=re.sub(' +', ' ', piece)
        pj='{"RTMSMessage":"' + piece + '"}'
        
        producetokafka(pj, "", "",producerid,tr,"",args)
  return []    

def ingestfiles():
    args = default_args
    buf = default_args['docfolder']
    chunks = int(default_args['chunks'])
    maintopic = default_args['doctopic']
    producerid='userfilestream'     
    interval=int(default_args['docingestinterval'])

    #gather files in the folders
    dirbuf = buf.split(",")
    # check if user wants to split folders to separate topics
    maintopicbuf = maintopic.split(",")
    if len(maintopicbuf) > 1:
      if len(dirbuf) != len(maintopicbuf):
        tsslogging.locallogs("ERROR", "STEP 3: Produce LOCALFILE in {} You specified multiple doctopics, then must match docfolder".format(os.path.basename(__file__)))
        return
    elif len(maintopicbuf) == 1 and len(dirbuf) > 0:
       for i in range(len(dirbuf)):
         maintopicbuf.append(maintopic)
    else:
       return
  
    while True:
       for dr,tr in zip(dirbuf,maintopicbuf):
         filenames = []
         if os.path.isdir("{}".format(dr)):
           a = [os.path.join("{}".format(dr), f) for f in os.listdir("{}".format(dr)) if 
           os.path.isfile(os.path.join("{}".format(dr), f))]
           filenames.extend(a)
           print("filename=",filenames)
           if len(filenames) > 0:
             with ExitStack() as stack:
               files = [stack.enter_context(open(i, "rb")) for i in filenames]
               contents = [readallfiles(file,tr,chunks) for file in files]
       if interval==0:
         break
       else:  
        time.sleep(interval)         
      
def startdirread():
  if 'docfolder' not in default_args and 'doctopic' not in default_args and 'chunks' not in default_args and 'docingestinterval' not in default_args:
     return
    
  if default_args['docfolder'] != '' and default_args['doctopic'] != '':
    print("INFO startdirread")  
    try:  
      t = threading.Thread(name='child procs', target=ingestfiles)
      t.start()
    except Exception as e:
      print(e)
  
def producetokafka(value, tmlid, identifier,producerid,maintopic,substream,args):
 inputbuf=value     
 topicid=int(args['topicid'])

 # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic 
 delay = int(args['delay'])
 enabletls = int(args['enabletls'])
 identifier = args['identifier']

 try:
    result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,producerid,enabletls,delay,'','', '',0,inputbuf,substream,
                                        topicid,identifier)
#    print("result=",result)
 except Exception as e:
    print("ERROR:",e)

def readdata():

  repo = tsslogging.getrepo()
  tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
  tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        

  args = default_args  
  inputfile=args['inputfile']

  # MAin Kafka topic to store the real-time data
  maintopic = args['topics']
  producerid = args['producerid']

  startdirread()
  
  if maintopic=='' or inputfile=='':
     return
  k=0
  try:
    file1 = open(inputfile, 'r')
    print("Data Producing to Kafka Started:",datetime.now())
  except Exception as e:
    tsslogging.locallogs("ERROR", "Localfile producing DAG in {} - {}".format(os.path.basename(__file__),e))     
    
    tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
    tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        
    return

  tsslogging.locallogs("INFO", "STEP 3: reading local file..successfully")   

  while True:
    line = file1.readline()
    line = line.replace(";", " ")
    print("line=",line)
    # add lat/long/identifier
    k = k + 1
    try:
      if line == "":
        #break
        file1.seek(0)
        k=0
        print("Reached End of File - Restarting")
        print("Read End:",datetime.now())
        continue
      producetokafka(line.strip(), "", "",producerid,maintopic,"",args)
      # change time to speed up or slow down data   
      time.sleep(args['sleep'])
    except Exception as e:
      print(e)  
      pass  

  file1.close()

def windowname(wtype,sname,dagname):
    randomNumber = random.randrange(10, 9999)
    wn = "python-{}-{}-{},{}".format(wtype,randomNumber,sname,dagname)
    with open("/tmux/pythonwindows_{}.txt".format(sname), 'a', encoding='utf-8') as file: 
      file.writelines("{}\n".format(wn))
    
    return wn

def startproducing(**context):

  tsslogging.locallogs("INFO", "STEP 3: producing data started")     
  
  sd = context['dag'].dag_id

  sname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_solutionname".format(sd))
  pname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_projectname".format(sd))
  VIPERTOKEN = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERTOKEN".format(sname))
  VIPERHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERHOSTPRODUCE".format(sname))
  VIPERPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERPORTPRODUCE".format(sname))
  HTTPADDR = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HTTPADDR".format(sname))

  VIPERHOSTFROM=tsslogging.getip(VIPERHOST)     
  ti = context['task_instance']
  ti.xcom_push(key="{}_PRODUCETYPE".format(sname),value='LOCALFILE')
  ti.xcom_push(key="{}_TOPIC".format(sname),value=default_args['topics'])
  ti.xcom_push(key="{}_CLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_IDENTIFIER".format(sname),value="{},{}".format(default_args['identifier'],default_args['inputfile']))

  ti.xcom_push(key="{}_FROMHOST".format(sname),value=VIPERHOSTFROM)
  ti.xcom_push(key="{}_TOHOST".format(sname),value=VIPERHOST)

  ti.xcom_push(key="{}_TSSCLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_TMLCLIENTPORT".format(sname),value="")
    
  ti.xcom_push(key="{}_PORT".format(sname),value="_{}".format(VIPERPORT))
  ti.xcom_push(key="{}_HTTPADDR".format(sname),value=HTTPADDR)

  inputfile=default_args['inputfile']
  if 'step3localfileinputfile' in os.environ:
       default_args['inputfile']=os.environ['step3localfileinputfile']
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
  else:
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
  
  docfolder=''
  if 'docfolder' in default_args and 'doctopic' in default_args:
    docfolder=default_args['docfolder']
    ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
    ti.xcom_push(key="{}_doctopic".format(sname),value=default_args['doctopic'])
    ti.xcom_push(key="{}_chunks".format(sname),value="_{}".format(default_args['chunks']))
    ti.xcom_push(key="{}_docingestinterval".format(sname),value="_{}".format(default_args['docingestinterval']))
  else:  
    ti.xcom_push(key="{}_docfolder".format(sname),value='')
    ti.xcom_push(key="{}_doctopic".format(sname),value='')
    ti.xcom_push(key="{}_chunks".format(sname),value='')
    ti.xcom_push(key="{}_docingestinterval".format(sname),value='')

  if 'step3localfiledocfolder' in os.environ:
       default_args['docfolder']=os.environ['step3localfiledocfolder']
       ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
        
  chip = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_chip".format(sname))   

  repo=tsslogging.getrepo() 

  if sname != '_mysolution_':
     fullpath="/{}/tml-airflow/dags/tml-solutions/{}/{}".format(repo,pname,os.path.basename(__file__))  
  else:
     fullpath="/{}/tml-airflow/dags/{}".format(repo,os.path.basename(__file__))  
    
  wn = windowname('produce',sname,sd)  
  subprocess.run(["tmux", "new", "-d", "-s", "{}".format(wn)])
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "cd /Viper-produce", "ENTER"])
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "python {} 1 {} {}{} {} \"{}\" \"{}\"".format(fullpath,VIPERTOKEN,HTTPADDR,VIPERHOST,VIPERPORT[1:],inputfile,docfolder), "ENTER"])        
        
if __name__ == '__main__':
    
    if len(sys.argv) > 1:
       if sys.argv[1] == "1":  
         VIPERTOKEN = sys.argv[2]
         VIPERHOST = sys.argv[3] 
         VIPERPORT = sys.argv[4]          
         inputfile = sys.argv[5]          
         default_args['inputfile']=inputfile
         docfolder = sys.argv[6]                   
         default_args['docfolder']=docfolder
         readdata()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.decorators import dag, task
import sys
import maadstml
import tsslogging
import os
import subprocess
import json 
import time
import random 
import threading
from contextlib import contextmanager
from contextlib import ExitStack
import re

sys.dont_write_bytecode = True
######################################## USER CHOOSEN PARAMETERS ########################################
default_args = {
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'microserviceid' : '', # <<< *** leave blank
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'inputfile' : '/rawdatademo/IoTData.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'topicid' : '-999', # <<< ********* do not modify  
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'docfolder' : '', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'doctopic' : '',  # This is the topic that will contain the docfolder file data
  'chunks' : 0, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
}

######################################## DO NOT MODIFY BELOW #############################################


from airflow import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import datetime
from airflow.decorators import dag, task
from airflow.decorators import dag, task
import sys
import sys
import maadstml
import maadstml
import tsslogging
import tsslogging
import os
import os
import subprocess
import subprocess
import json 
import json 
import time
import time
import random 
import random 
import threading
import threading
from contextlib import contextmanager
from contextlib import contextmanager
from contextlib import ExitStack
from contextlib import ExitStack
import re
import re


sys.dont_write_bytecode = True
sys.dont_write_bytecode = True
######################################## USER CHOOSEN PARAMETERS ########################################
######################################## USER CHOOSEN PARAMETERS ########################################
default_args = {
default_args = {
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'owner' : 'Sebastian Maurice', # <<< *** Change as needed   
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'enabletls': '1', # <<< *** 1=connection is encrypted, 0=no encryption
  'microserviceid' : '', # <<< *** leave blank
  'microserviceid' : '', # <<< *** leave blank
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'producerid' : 'iotsolution',   # <<< *** Change as needed   
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'topics' : 'iot-raw-data', # *************** This is one of the topic you created in SYSTEM STEP 2
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'identifier' : 'TML solution',   # <<< *** Change as needed   
  'inputfile' : '',#'/rawdatademo/cisco_network_data.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'inputfile' : '',#'/rawdatademo/cisco_network_data.txt',  # <<< ***** replace ?  to input file name to read. NOTE this data file should be JSON messages per line and stored in the HOST folder mapped to /rawdata folder 
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'delay' : '7000', # << ******* 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
  'topicid' : '-999', # <<< ********* do not modify  
  'topicid' : '-999', # <<< ********* do not modify  
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'sleep' : 0.15, # << Control how fast data streams - if 0 - the data will stream as fast as possible - BUT this may cause connecion reset by peer 
  'docfolder' : '/rawdatademo/mylogsdemo,/rawdatademo/mylogs2demo', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'docfolder' : '/rawdatademo/mylogsdemo,/rawdatademo/mylogs2demo', # You can read TEXT files or any file in these folders that are inside the volume mapped to /rawdata
  'doctopic' : 'rtms-stream-mylogs,rtms-stream-mylogs2',  # This is the topic that will contain the docfolder file data
  'doctopic' : 'rtms-stream-mylogs,rtms-stream-mylogs2',  # This is the topic that will contain the docfolder file data
  'chunks' :3000, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'chunks' :3000, # if 0 the files in docfolder are read line by line, otherwise they are read by chunks i.e. 512
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
  'docingestinterval' : 0, # specify the frequency in seconds to read files in docfolder - if 0 the files are read ONCE
}
}


######################################## DO NOT MODIFY BELOW #############################################
######################################## DO NOT MODIFY BELOW #############################################


# This sets the lat/longs for the IoT devices so it can be map
# This sets the lat/longs for the IoT devices so it can be map
VIPERTOKEN=""
VIPERTOKEN=""
VIPERHOST=""
VIPERHOST=""
VIPERPORT=""
VIPERPORT=""


def read_in_chunks(file_object, chunk_size=1024):
def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    Default chunk size: 1k."""
    while True:
    while True:
        try:
        try:
          if chunk_size != 0:
          if chunk_size != 0:
            data = file_object.read(chunk_size).decode('utf-8')            
            data = file_object.read(chunk_size).decode('utf-8')            
            if len(data)>0 and data[-1] != ' ':
            if len(data)>0 and data[-1] != ' ':
                 ct=0
                 ct=0
                 for c in reversed(data):
                 for c in reversed(data):
                   if c == ' ':
                   if c == ' ':
                        break
                        break
                   ct = ct +1
                   ct = ct +1
                 if ct < len(data):
                 if ct < len(data):
                   file_object.seek(file_object.tell()-ct)
                   file_object.seek(file_object.tell()-ct)
                   data = data[:len(data)-ct]
                   data = data[:len(data)-ct]
          else:
          else:
            data = file_object.readline().decode('utf-8')            
            data = file_object.readline().decode('utf-8')            
          data=data.replace('"','').replace("'","").replace("\\n"," ").replace('\n'," ").replace("\\r"," ").replace('\r'," ").replace(';'," ").replace('&'," ").strip()
          data=data.replace('"','').replace("'","").replace("\\n"," ").replace('\n'," ").replace("\\r"," ").replace('\r'," ").replace(';'," ").replace('&'," ").strip()
          if not data:
          if not data:
               break
               break
          yield data          
          yield data          
        except Exception as e:
        except Exception as e:
           break
           break


def readallfiles(fd,tr,cs=1024):
def readallfiles(fd,tr,cs=1024):
  args=default_args
  args=default_args
  producerid='userfilestream'
  producerid='userfilestream'
  print("fd=",fd.name)
  print("fd=",fd.name)
  for piece in read_in_chunks(fd,cs):
  for piece in read_in_chunks(fd,cs):
        piece=re.sub(' +', ' ', piece)
        piece=re.sub(' +', ' ', piece)
        pj='{"RTMSMessage":"' + piece + '"}'
        pj='{"RTMSMessage":"' + piece + '"}'
        
        
        producetokafka(pj, "", "",producerid,tr,"",args)
        producetokafka(pj, "", "",producerid,tr,"",args)
  return []    
  return []    


def ingestfiles():
def ingestfiles():
    args = default_args
    args = default_args
    buf = default_args['docfolder']
    buf = default_args['docfolder']
    chunks = int(default_args['chunks'])
    chunks = int(default_args['chunks'])
    maintopic = default_args['doctopic']
    maintopic = default_args['doctopic']
    producerid='userfilestream'     
    producerid='userfilestream'     
    interval=int(default_args['docingestinterval'])
    interval=int(default_args['docingestinterval'])


    #gather files in the folders
    #gather files in the folders
    dirbuf = buf.split(",")
    dirbuf = buf.split(",")
    # check if user wants to split folders to separate topics
    # check if user wants to split folders to separate topics
    maintopicbuf = maintopic.split(",")
    maintopicbuf = maintopic.split(",")
    if len(maintopicbuf) > 1:
    if len(maintopicbuf) > 1:
      if len(dirbuf) != len(maintopicbuf):
      if len(dirbuf) != len(maintopicbuf):
        tsslogging.locallogs("ERROR", "STEP 3: Produce LOCALFILE in {} You specified multiple doctopics, then must match docfolder".format(os.path.basename(__file__)))
        tsslogging.locallogs("ERROR", "STEP 3: Produce LOCALFILE in {} You specified multiple doctopics, then must match docfolder".format(os.path.basename(__file__)))
        return
        return
    elif len(maintopicbuf) == 1 and len(dirbuf) > 0:
    elif len(maintopicbuf) == 1 and len(dirbuf) > 0:
       for i in range(len(dirbuf)):
       for i in range(len(dirbuf)):
         maintopicbuf.append(maintopic)
         maintopicbuf.append(maintopic)
    else:
    else:
       return
       return
  
  
    while True:
    while True:
       for dr,tr in zip(dirbuf,maintopicbuf):
       for dr,tr in zip(dirbuf,maintopicbuf):
         filenames = []
         filenames = []
         if os.path.isdir("{}".format(dr)):
         if os.path.isdir("{}".format(dr)):
           a = [os.path.join("{}".format(dr), f) for f in os.listdir("{}".format(dr)) if 
           a = [os.path.join("{}".format(dr), f) for f in os.listdir("{}".format(dr)) if 
           os.path.isfile(os.path.join("{}".format(dr), f))]
           os.path.isfile(os.path.join("{}".format(dr), f))]
           filenames.extend(a)
           filenames.extend(a)
           print("filename=",filenames)
           print("filename=",filenames)
           if len(filenames) > 0:
           if len(filenames) > 0:
             with ExitStack() as stack:
             with ExitStack() as stack:
               files = [stack.enter_context(open(i, "rb")) for i in filenames]
               files = [stack.enter_context(open(i, "rb")) for i in filenames]
               contents = [readallfiles(file,tr,chunks) for file in files]
               contents = [readallfiles(file,tr,chunks) for file in files]
       if interval==0:
       if interval==0:
         break
         break
       else:  
       else:  
        time.sleep(interval)         
        time.sleep(interval)         
      
      
def startdirread():
def startdirread():
  if 'docfolder' not in default_args and 'doctopic' not in default_args and 'chunks' not in default_args and 'docingestinterval' not in default_args:
  if 'docfolder' not in default_args and 'doctopic' not in default_args and 'chunks' not in default_args and 'docingestinterval' not in default_args:
     return
     return
    
    
  if default_args['docfolder'] != '' and default_args['doctopic'] != '':
  if default_args['docfolder'] != '' and default_args['doctopic'] != '':
    print("INFO startdirread")  
    print("INFO startdirread")  
    try:  
    try:  
      t = threading.Thread(name='child procs', target=ingestfiles)
      t = threading.Thread(name='child procs', target=ingestfiles)
      t.start()
      t.start()
    except Exception as e:
    except Exception as e:
      print(e)
      print(e)
  
  
def producetokafka(value, tmlid, identifier,producerid,maintopic,substream,args):
def producetokafka(value, tmlid, identifier,producerid,maintopic,substream,args):
 inputbuf=value     
 inputbuf=value     
 topicid=int(args['topicid'])
 topicid=int(args['topicid'])


 # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic 
 # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic 
 delay = int(args['delay'])
 delay = int(args['delay'])
 enabletls = int(args['enabletls'])
 enabletls = int(args['enabletls'])
 identifier = args['identifier']
 identifier = args['identifier']


 try:
 try:
    result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,producerid,enabletls,delay,'','', '',0,inputbuf,substream,
    result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,producerid,enabletls,delay,'','', '',0,inputbuf,substream,
                                        topicid,identifier)
                                        topicid,identifier)
#    print("result=",result)
#    print("result=",result)
 except Exception as e:
 except Exception as e:
    print("ERROR:",e)
    print("ERROR:",e)


def readdata():
def readdata():


  repo = tsslogging.getrepo()
  repo = tsslogging.getrepo()
  tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
  tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
  tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        
  tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        


  args = default_args  
  args = default_args  
  inputfile=args['inputfile']
  inputfile=args['inputfile']


  # MAin Kafka topic to store the real-time data
  # MAin Kafka topic to store the real-time data
  maintopic = args['topics']
  maintopic = args['topics']
  producerid = args['producerid']
  producerid = args['producerid']


  startdirread()
  startdirread()
  
  
  if maintopic=='' or inputfile=='':
  if maintopic=='' or inputfile=='':
     return
     return
  k=0
  k=0
  try:
  try:
    file1 = open(inputfile, 'r')
    file1 = open(inputfile, 'r')
    print("Data Producing to Kafka Started:",datetime.now())
    print("Data Producing to Kafka Started:",datetime.now())
  except Exception as e:
  except Exception as e:
    tsslogging.locallogs("ERROR", "Localfile producing DAG in {} - {}".format(os.path.basename(__file__),e))     
    tsslogging.locallogs("ERROR", "Localfile producing DAG in {} - {}".format(os.path.basename(__file__),e))     
    
    
    tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
    tsslogging.tsslogit("Localfile producing DAG in {}".format(os.path.basename(__file__)), "INFO" )                     
    tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        
    tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")        
    return
    return


  tsslogging.locallogs("INFO", "STEP 3: reading local file..successfully")   
  tsslogging.locallogs("INFO", "STEP 3: reading local file..successfully")   


  while True:
  while True:
    line = file1.readline()
    line = file1.readline()
    line = line.replace(";", " ")
    line = line.replace(";", " ")
    print("line=",line)
    print("line=",line)
    # add lat/long/identifier
    # add lat/long/identifier
    k = k + 1
    k = k + 1
    try:
    try:
      if line == "":
      if line == "":
        #break
        #break
        file1.seek(0)
        file1.seek(0)
        k=0
        k=0
        print("Reached End of File - Restarting")
        print("Reached End of File - Restarting")
        print("Read End:",datetime.now())
        print("Read End:",datetime.now())
        continue
        continue
      producetokafka(line.strip(), "", "",producerid,maintopic,"",args)
      producetokafka(line.strip(), "", "",producerid,maintopic,"",args)
      # change time to speed up or slow down data   
      # change time to speed up or slow down data   
      time.sleep(args['sleep'])
      time.sleep(args['sleep'])
    except Exception as e:
    except Exception as e:
      print(e)  
      print(e)  
      pass  
      pass  


  file1.close()
  file1.close()


def windowname(wtype,sname,dagname):
def windowname(wtype,sname,dagname):
    randomNumber = random.randrange(10, 9999)
    randomNumber = random.randrange(10, 9999)
    wn = "python-{}-{}-{},{}".format(wtype,randomNumber,sname,dagname)
    wn = "python-{}-{}-{},{}".format(wtype,randomNumber,sname,dagname)
    with open("/tmux/pythonwindows_{}.txt".format(sname), 'a', encoding='utf-8') as file: 
    with open("/tmux/pythonwindows_{}.txt".format(sname), 'a', encoding='utf-8') as file: 
      file.writelines("{}\n".format(wn))
      file.writelines("{}\n".format(wn))
    
    
    return wn
    return wn


def startproducing(**context):
def startproducing(**context):


  tsslogging.locallogs("INFO", "STEP 3: producing data started")     
  tsslogging.locallogs("INFO", "STEP 3: producing data started")     
  
  
  sd = context['dag'].dag_id
  sd = context['dag'].dag_id


  sname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_solutionname".format(sd))
  sname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_solutionname".format(sd))
  pname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_projectname".format(sd))
  pname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_projectname".format(sd))
  VIPERTOKEN = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERTOKEN".format(sname))
  VIPERTOKEN = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERTOKEN".format(sname))
  VIPERHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERHOSTPRODUCE".format(sname))
  VIPERHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERHOSTPRODUCE".format(sname))
  VIPERPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERPORTPRODUCE".format(sname))
  VIPERPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERPORTPRODUCE".format(sname))
  HTTPADDR = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HTTPADDR".format(sname))
  HTTPADDR = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HTTPADDR".format(sname))


  VIPERHOSTFROM=tsslogging.getip(VIPERHOST)     
  VIPERHOSTFROM=tsslogging.getip(VIPERHOST)     
  ti = context['task_instance']
  ti = context['task_instance']
  ti.xcom_push(key="{}_PRODUCETYPE".format(sname),value='LOCALFILE')
  ti.xcom_push(key="{}_PRODUCETYPE".format(sname),value='LOCALFILE')
  ti.xcom_push(key="{}_TOPIC".format(sname),value=default_args['topics'])
  ti.xcom_push(key="{}_TOPIC".format(sname),value=default_args['topics'])
  ti.xcom_push(key="{}_CLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_CLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_IDENTIFIER".format(sname),value="{},{}".format(default_args['identifier'],default_args['inputfile']))
  ti.xcom_push(key="{}_IDENTIFIER".format(sname),value="{},{}".format(default_args['identifier'],default_args['inputfile']))


  ti.xcom_push(key="{}_FROMHOST".format(sname),value=VIPERHOSTFROM)
  ti.xcom_push(key="{}_FROMHOST".format(sname),value=VIPERHOSTFROM)
  ti.xcom_push(key="{}_TOHOST".format(sname),value=VIPERHOST)
  ti.xcom_push(key="{}_TOHOST".format(sname),value=VIPERHOST)


  ti.xcom_push(key="{}_TSSCLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_TSSCLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_TMLCLIENTPORT".format(sname),value="")
  ti.xcom_push(key="{}_TMLCLIENTPORT".format(sname),value="")
    
    
  ti.xcom_push(key="{}_PORT".format(sname),value="_{}".format(VIPERPORT))
  ti.xcom_push(key="{}_PORT".format(sname),value="_{}".format(VIPERPORT))
  ti.xcom_push(key="{}_HTTPADDR".format(sname),value=HTTPADDR)
  ti.xcom_push(key="{}_HTTPADDR".format(sname),value=HTTPADDR)


  inputfile=default_args['inputfile']
  inputfile=default_args['inputfile']
  if 'step3localfileinputfile' in os.environ:
  if 'step3localfileinputfile' in os.environ:
       default_args['inputfile']=os.environ['step3localfileinputfile']
       default_args['inputfile']=os.environ['step3localfileinputfile']
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
  else:
  else:
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
       ti.xcom_push(key="{}_inputfile".format(sname),value=default_args['inputfile'])
  
  
  docfolder=''
  docfolder=''
  if 'docfolder' in default_args and 'doctopic' in default_args:
  if 'docfolder' in default_args and 'doctopic' in default_args:
    docfolder=default_args['docfolder']
    docfolder=default_args['docfolder']
    ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
    ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
    ti.xcom_push(key="{}_doctopic".format(sname),value=default_args['doctopic'])
    ti.xcom_push(key="{}_doctopic".format(sname),value=default_args['doctopic'])
    ti.xcom_push(key="{}_chunks".format(sname),value="_{}".format(default_args['chunks']))
    ti.xcom_push(key="{}_chunks".format(sname),value="_{}".format(default_args['chunks']))
    ti.xcom_push(key="{}_docingestinterval".format(sname),value="_{}".format(default_args['docingestinterval']))
    ti.xcom_push(key="{}_docingestinterval".format(sname),value="_{}".format(default_args['docingestinterval']))
  else:  
  else:  
    ti.xcom_push(key="{}_docfolder".format(sname),value='')
    ti.xcom_push(key="{}_docfolder".format(sname),value='')
    ti.xcom_push(key="{}_doctopic".format(sname),value='')
    ti.xcom_push(key="{}_doctopic".format(sname),value='')
    ti.xcom_push(key="{}_chunks".format(sname),value='')
    ti.xcom_push(key="{}_chunks".format(sname),value='')
    ti.xcom_push(key="{}_docingestinterval".format(sname),value='')
    ti.xcom_push(key="{}_docingestinterval".format(sname),value='')


  if 'step3localfiledocfolder' in os.environ:
  if 'step3localfiledocfolder' in os.environ:
       default_args['docfolder']=os.environ['step3localfiledocfolder']
       default_args['docfolder']=os.environ['step3localfiledocfolder']
       ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
       ti.xcom_push(key="{}_docfolder".format(sname),value=default_args['docfolder'])
        
        
  chip = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_chip".format(sname))   
  chip = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_chip".format(sname))   


  repo=tsslogging.getrepo() 
  repo=tsslogging.getrepo() 


  if sname != '_mysolution_':
  if sname != '_mysolution_':
     fullpath="/{}/tml-airflow/dags/tml-solutions/{}/{}".format(repo,pname,os.path.basename(__file__))  
     fullpath="/{}/tml-airflow/dags/tml-solutions/{}/{}".format(repo,pname,os.path.basename(__file__))  
  else:
  else:
     fullpath="/{}/tml-airflow/dags/{}".format(repo,os.path.basename(__file__))  
     fullpath="/{}/tml-airflow/dags/{}".format(repo,os.path.basename(__file__))  
    
    
  wn = windowname('produce',sname,sd)  
  wn = windowname('produce',sname,sd)  
  subprocess.run(["tmux", "new", "-d", "-s", "{}".format(wn)])
  subprocess.run(["tmux", "new", "-d", "-s", "{}".format(wn)])
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "cd /Viper-produce", "ENTER"])
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "cd /Viper-produce", "ENTER"])
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "python {} 1 {} {}{} {} \"{}\" \"{}\"".format(fullpath,VIPERTOKEN,HTTPADDR,VIPERHOST,VIPERPORT[1:],inputfile,docfolder), "ENTER"])        
  subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "python {} 1 {} {}{} {} \"{}\" \"{}\"".format(fullpath,VIPERTOKEN,HTTPADDR,VIPERHOST,VIPERPORT[1:],inputfile,docfolder), "ENTER"])        
        
        
if __name__ == '__main__':
if __name__ == '__main__':
    
    
    if len(sys.argv) > 1:
    if len(sys.argv) > 1:
       if sys.argv[1] == "1":  
       if sys.argv[1] == "1":  
         VIPERTOKEN = sys.argv[2]
         VIPERTOKEN = sys.argv[2]
         VIPERHOST = sys.argv[3] 
         VIPERHOST = sys.argv[3] 
         VIPERPORT = sys.argv[4]          
         VIPERPORT = sys.argv[4]          
         inputfile = sys.argv[5]          
         inputfile = sys.argv[5]          
         default_args['inputfile']=inputfile
         default_args['inputfile']=inputfile
         docfolder = sys.argv[6]                   
         docfolder = sys.argv[6]                   
         default_args['docfolder']=docfolder
         default_args['docfolder']=docfolder
         readdata()
         readdata()
