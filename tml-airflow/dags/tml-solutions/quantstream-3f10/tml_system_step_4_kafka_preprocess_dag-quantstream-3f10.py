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
import time
import random

sys.dont_write_bytecode = True
######################################## USER CHOOSEN PARAMETERS ########################################
default_args = {
    'myname': 'Sebastian Maurice', # <<< *** Change as needed
    'enabletls': 1, # <<< *** 1=connection is encrypted, 0=no encryption
    'microserviceid': '', # <<< *** leave blank
    'producerid': 'iotsolution', # <<< *** Change as needed
    'preprocess_data_topic': 'iot-preprocess', # << *** topic/data to use for training datasets - You created this in STEP 2
    'ml_data_topic': 'ml-data', # topic to store the trained algorithms  - You created this in STEP 2
    'identifier': 'TML solution', # <<< *** Change as needed
    'companyname': 'Your company', # <<< *** Change as needed
    'myemail': 'Your email', # <<< *** Change as needed
    'mylocation': 'Your location', # <<< *** Change as needed
    'brokerhost': '', # <<< *** Leave as is
    'brokerport': -999, # <<< *** Leave as is
    'deploy': 1, # <<< *** do not modofy
    'modelruns': 50, # <<< *** Change as needed
    'offset': -1, # <<< *** Do not modify
    'islogistic': 2, # <<< *** Change as needed, 1=logistic, 0=not logistic, 2=multinomial logistic
    'networktimeout': 600, # <<< *** Change as needed
    'modelsearchtuner': 90, # <<< *This parameter will attempt to fine tune the model search space - A number close to 100 means you will have fewer models but their predictive quality will be higher.
    'dependentvariable': 'failure', # <<< *** Change as needed,
    'independentvariables': 'Power_preprocessed_AnomProb', # <<< *** Change as needed,
    'rollbackoffsets': 1000, # <<< *** Change as needed,
    'consumeridtrainingdata2': '', # leave blank
    'partition_training': '', # leave blank
    'consumefrom': '', # leave blank
    'topicid': -1, # leave as is
    'fullpathtotrainingdata': '/Viper-ml/viperlogs/iotlogistic', # # <<< *** Change as needed - add name for foldername that stores the training datasets
    'processlogic': '', # <<< *** Change as needed, i.e. classification_name=failure_prob:Voltage_preprocessed_AnomProb=55,n:Current_preprocessed_AnomProb=55,n
    'array': 0, # leave as is
    'transformtype': '', # Sets the model to: log-lin,lin-log,log-log
    'sendcoefto': '', # you can send coefficients to another topic for further processing -- MUST BE SET IN STEP 2
    'coeftoprocess': '', # indicate the index of the coefficients to process i.e. 0,1,2 For example, for a 3 estimated parameters 0=constant, 1,2 are the other estmated paramters
    'coefsubtopicnames': '', # Give the coefficients a name: constant,elasticity,elasticity2
    'viperconfigfile': '/Viper-ml/viper.env', # Do not modify
    'HPDEADDR': 'http://',
}

######################################## DO NOT MODIFY BELOW #############################################

VIPERTOKEN=""
VIPERHOST=""
VIPERPORT=""
HTTPADDR=""

def processtransactiondata():
 global VIPERTOKEN
 global VIPERHOST
 global VIPERPORT   
 global HTTPADDR
 preprocesstopic = default_args['preprocess_data_topic']
 maintopic =  default_args['raw_data_topic']  
 mainproducerid = default_args['producerid']     
  
#############################################################################################################
  #                                    PREPROCESS DATA STREAMS


  # Roll back each data stream by 10 percent - change this to a larger number if you want more data
  # For supervised machine learning you need a minimum of 30 data points in each stream
 maxrows=int(default_args['maxrows'])

  # Go to the last offset of each stream: If lastoffset=500, then this function will rollback the 
  # streams to offset=500-50=450
 offset=int(default_args['offset'])
  # Max wait time for Kafka to response on milliseconds - you can increase this number if
  #maintopic to produce the preprocess data to
 topic=maintopic
  # producerid of the topic
 producerid=mainproducerid
  # use the host in Viper.env file
 brokerhost=default_args['brokerhost']
  # use the port in Viper.env file
 brokerport=int(default_args['brokerport'])
  #if load balancing enter the microsericeid to route the HTTP to a specific machine
 microserviceid=default_args['microserviceid']


  # You can preprocess with the following functions: MAX, MIN, SUM, AVG, COUNT, DIFF,OUTLIERS
  # here we will take max values of the arcturus-humidity, we will Diff arcturus-temperature, and average arcturus-Light_Intensity
  # NOTE: The number of process logic functions MUST match the streams - the operations will be applied in the same order
#
 preprocessconditions=default_args['preprocessconditions']

 # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic 
 delay=int(default_args['delay'])
 # USE TLS encryption when sending to Kafka Cloud (GCP/AWS/Azure)
 enabletls=int(default_args['enabletls'])
 array=int(default_args['array'])
 saveasarray=int(default_args['saveasarray'])
 topicid=int(default_args['topicid'])

 rawdataoutput=int(default_args['rawdataoutput'])
 asynctimeout=int(default_args['asynctimeout'])
 timedelay=int(default_args['timedelay'])

 jsoncriteria = default_args['jsoncriteria']

 tmlfilepath=default_args['tmlfilepath']
 usemysql=int(default_args['usemysql'])

 streamstojoin=default_args['streamstojoin']
 identifier = default_args['identifier']

 # if dataage - use:dataage_utcoffset_timetype
 preprocesstypes=default_args['preprocesstypes']
 pathtotmlattrs=default_args['pathtotmlattrs']       
    
 try:
    result=maadstml.viperpreprocesscustomjson(VIPERTOKEN,VIPERHOST,VIPERPORT,topic,producerid,offset,jsoncriteria,rawdataoutput,maxrows,enabletls,delay,brokerhost,
                                      brokerport,microserviceid,topicid,streamstojoin,preprocesstypes,preprocessconditions,identifier,
                                      preprocesstopic,array,saveasarray,timedelay,asynctimeout,usemysql,tmlfilepath,pathtotmlattrs)
    #print(result)
    return result
 except Exception as e:
    print(e)
    return e

def windowname(wtype,sname,dagname):
    randomNumber = random.randrange(10, 9999)
    wn = "python-{}-{}-{},{}".format(wtype,randomNumber,sname,dagname)
    with open("/tmux/pythonwindows_{}.txt".format(sname), 'a', encoding='utf-8') as file: 
      file.writelines("{}\n".format(wn))
    
    return wn

def dopreprocessing(**context):
       tsslogging.locallogs("INFO", "STEP 4: Preprocessing started")
       sd = context['dag'].dag_id
       sname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_solutionname".format(sd))
       pname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_projectname".format(sd))

       VIPERTOKEN = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERTOKEN".format(sname))
       VIPERHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERHOSTPREPROCESS".format(sname))
       VIPERPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERPORTPREPROCESS".format(sname))
       HTTPADDR = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HTTPADDR".format(sname))

       chip = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_chip".format(sname)) 

       if 'step4raw_data_topic' in os.environ:
         default_args['raw_data_topic']=os.environ['step4raw_data_topic']
       if 'step4preprocesstypes' in os.environ:
           default_args['preprocesstypes']=os.environ['step4preprocesstypes']
       if 'step4jsoncriteria' in os.environ:
           default_args['jsoncriteria']=os.environ['step4jsoncriteria']
       if 'step4preprocess_data_topic'  in os.environ:
           default_args['preprocess_data_topic']=os.environ['step4preprocess_data_topic']
         
       ti = context['task_instance']    
       ti.xcom_push(key="{}_raw_data_topic".format(sname), value=default_args['raw_data_topic'])
       ti.xcom_push(key="{}_preprocess_data_topic".format(sname), value=default_args['preprocess_data_topic'])
       ti.xcom_push(key="{}_preprocessconditions".format(sname), value=default_args['preprocessconditions'])
       ti.xcom_push(key="{}_delay".format(sname), value="_{}".format(default_args['delay']))
       ti.xcom_push(key="{}_array".format(sname), value="_{}".format(default_args['array']))
       ti.xcom_push(key="{}_saveasarray".format(sname), value="_{}".format(default_args['saveasarray']))
       ti.xcom_push(key="{}_topicid".format(sname), value="_{}".format(default_args['topicid']))
       ti.xcom_push(key="{}_rawdataoutput".format(sname), value="_{}".format(default_args['rawdataoutput']))
       ti.xcom_push(key="{}_asynctimeout".format(sname), value="_{}".format(default_args['asynctimeout']))
       ti.xcom_push(key="{}_timedelay".format(sname), value="_{}".format(default_args['timedelay']))
       ti.xcom_push(key="{}_usemysql".format(sname), value="_{}".format(default_args['usemysql']))
       ti.xcom_push(key="{}_preprocesstypes".format(sname), value=default_args['preprocesstypes'])
       ti.xcom_push(key="{}_pathtotmlattrs".format(sname), value=default_args['pathtotmlattrs'])
       ti.xcom_push(key="{}_identifier".format(sname), value=default_args['identifier'])
       ti.xcom_push(key="{}_jsoncriteria".format(sname), value=default_args['jsoncriteria'])

       maxrows=default_args['maxrows']
       if 'step4maxrows' in os.environ:
         ti.xcom_push(key="{}_maxrows".format(sname), value="_{}".format(os.environ['step4maxrows']))                
         maxrows=os.environ['step4maxrows']
       else:  
         ti.xcom_push(key="{}_maxrows".format(sname), value="_{}".format(default_args['maxrows']))
         
        
       repo=tsslogging.getrepo() 
       if sname != '_mysolution_':
        fullpath="/{}/tml-airflow/dags/tml-solutions/{}/{}".format(repo,pname,os.path.basename(__file__))  
       else:
         fullpath="/{}/tml-airflow/dags/{}".format(repo,os.path.basename(__file__))  
            
       wn = windowname('preprocess',sname,sd)     
       subprocess.run(["tmux", "new", "-d", "-s", "{}".format(wn)])
       subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "cd /Viper-preprocess", "ENTER"])
       subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "python {} 1 {} {}{} {} {} \"{}\" \"{}\" \"{}\" \"{}\"".format(fullpath,VIPERTOKEN,HTTPADDR,VIPERHOST,VIPERPORT[1:],
                                                          maxrows,default_args['raw_data_topic'],default_args['preprocesstypes'],default_args['jsoncriteria'],default_args['preprocess_data_topic']), "ENTER"],capture_output=True, text=True)        
       pane_pid = subprocess.check_output([
          "tmux", "list-panes", "-t", wn, "-F", "#{pane_pid}"
       ]).decode().strip()

       with open("/tmux/step4_preprocess.txt", 'w', encoding='utf-8') as file: 
          file.write("{}\n".format(pane_pid))
          file.write("{}\n".format(wn))
          file.write("python {} 1 {} {}{} {} {} \"{}\" \"{}\" \"{}\" \"{}\"".format(fullpath,VIPERTOKEN,HTTPADDR,VIPERHOST,VIPERPORT[1:],
                                                          maxrows,default_args['raw_data_topic'],default_args['preprocesstypes'],default_args['jsoncriteria'],default_args['preprocess_data_topic']))         

if __name__ == '__main__':
    if len(sys.argv) > 1:
       if sys.argv[1] == "1": 
        repo=tsslogging.getrepo()
        
        VIPERTOKEN = sys.argv[2]
        VIPERHOST = sys.argv[3] 
        VIPERPORT = sys.argv[4]                  
        maxrows =  sys.argv[5]
        default_args['maxrows'] = maxrows
        default_args['raw_data_topic'] =  sys.argv[6]
        default_args['preprocesstypes'] =  sys.argv[7]
        default_args['jsoncriteria'] =  sys.argv[8]
        default_args['preprocess_data_topic'] =  sys.argv[9]
         
        tsslogging.locallogs("INFO", "STEP 4: Preprocessing started")
                     
        while True:
          try: 
            processtransactiondata()
            time.sleep(1)
          except Exception as e:    
           tsslogging.locallogs("ERROR", "STEP 4: Preprocessing DAG in {} {}".format(os.path.basename(__file__),e))
           tsslogging.tsslogit("Preprocessing DAG in {} {}".format(os.path.basename(__file__),e), "ERROR" )                     
           tsslogging.git_push("/{}".format(repo),"Entry from {}".format(os.path.basename(__file__)),"origin")    
           break