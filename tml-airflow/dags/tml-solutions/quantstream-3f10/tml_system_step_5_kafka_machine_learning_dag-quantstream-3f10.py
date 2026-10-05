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
    'owner': 'Sebastian Maurice', # <<< *** Change as needed
    'enabletls': 1, # <<< *** 1=connection is encrypted, 0=no encryption
    'microserviceid': '', # <<< *** leave blank
    'producerid': 'iotsolution', # <<< *** Change as needed
    'raw_data_topic': 'quantstream-raw-data', # *************** INCLUDE ONLY ONE TOPIC - This is one of the topic you created in SYSTEM STEP 2
    'preprocess_data_topic': 'iot-preprocess', # *************** INCLUDE ONLY ONE TOPIC - This is one of the topic you created in SYSTEM STEP 2
    'maxrows': 1, # <<< ********** Number of offsets to rollback the data stream -i.e. rollback stream by 500 offsets
    'offset': -1, # <<< Rollback from the end of the data streams
    'brokerhost': '', # <<< *** Leave as is
    'brokerport': -999, # <<< *** Leave as is
    'preprocessconditions': '', # # <<< Leave blank
    'delay': 70, # Add a 70 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic
    'array': 0, # do not modify
    'saveasarray': 1, # do not modify
    'topicid': -999, # do not modify
    'rawdataoutput': 0, # <<< 1 to output raw data used in the preprocessing, 0 do not output
    'asynctimeout': 120, # <<< 120 seconds for connection timeout
    'timedelay': 0, # <<< connection delay
    'tmlfilepath': '', # leave blank
    'usemysql': 1, # do not modify
    'streamstojoin': '', # leave blank
    'identifier': 'QuantStream AI Mid-Frequency Algorithmic trading', # <<< ** Change as needed
    'preprocesstypes': 'avg', # <<< **** MAIN PREPROCESS TYPES CHNAGE AS NEEDED refer to https://tml-readthedocs.readthedocs.io/en/latest/
    'pathtotmlattrs': 'oem=n/a,lat=n/a,long=n/a,location=n/a,identifier=n/a', # Change as needed
    'jsoncriteria': """uid=date,filter:allrecords~
subtopics=features_X.x1_tick_return_preprocessed_Avg,features_X.x2_session_return_preprocessed_Avg,features_X.x4_range_drift_preprocessed_Avg,features_X.x6_realized_volatility_preprocessed_Avg,features_X.x8_sma_distance_preprocessed_Avg,features_X.x9_acceleration_preprocessed_Avg~
values=features_X.x1_tick_return_preprocessed_Avg,features_X.x2_session_return_preprocessed_Avg,features_X.x4_range_drift_preprocessed_Avg,features_X.x6_realized_volatility_preprocessed_Avg,features_X.x8_sma_distance_preprocessed_Avg,features_X.x9_acceleration_preprocessed_Avg~
identifiers=price,price,price,price,price,price~
datetime=datetime_utc~
msgid=symbol,price~
latlong=lat:long""" # <<< **** Specify your json criteria. Here is an example of a multiline json --  refer to https://tml-readthedocs.readthedocs.io/en/latest/
}

######################################## DO NOT MODIFY BELOW #############################################


# This sets the lat/longs for the IoT devices so it can be map
VIPERTOKEN=""
VIPERHOST=""
VIPERPORT=""
HPDEHOST = ''    
HPDEPORT = ''
HTTPADDR=""
        
def performSupervisedMachineLearning():
      maintopic =  default_args['preprocess_data_topic']  
      mainproducerid = default_args['producerid']                     
            
      viperconfigfile = default_args['viperconfigfile']
      # Set personal data
      companyname=default_args['companyname']
      myname=default_args['myname']
      myemail=default_args['myemail']
      mylocation=default_args['mylocation']

      # Enable SSL/TLS communication with Kafka
      enabletls=int(default_args['enabletls'])
      # If brokerhost is empty then this function will use the brokerhost address in your
      # VIPER.ENV in the field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
      brokerhost=default_args['brokerhost']
      # If this is -999 then this function uses the port address for Kafka in VIPER.ENV in the
      # field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
      brokerport=int(default_args['brokerport'])
      # If you are using a reverse proxy to reach VIPER then you can put it here - otherwise if
      # empty then no reverse proxy is being used
      microserviceid=default_args['microserviceid']

      #############################################################################################################
      #                         VIPER CALLS HPDE TO PERFORM REAL_TIME MACHINE LEARNING ON TRAINING DATA 


      # deploy the algorithm to ./deploy folder - otherwise it will be in ./models folder
      deploy=int(default_args['deploy'])
      # number of models runs to find the best algorithm
      modelruns=int(default_args['modelruns'])
      # Go to the last offset of the partition in partition_training variable
      offset=int(default_args['offset'])
      # If 0, this is not a logistic model where dependent variable is discreet
      islogistic=int(default_args['islogistic'])
      # set network timeout for communication between VIPER and HPDE in seconds
      # increase this number if you timeout
      networktimeout=int(default_args['networktimeout'])

      # This parameter will attempt to fine tune the model search space - a number close to 0 means you will have lots of
      # models but their quality may be low.  A number close to 100 means you will have fewer models but their predictive
      # quality will be higher.
      modelsearchtuner=int(default_args['modelsearchtuner'])

      #this is the dependent variable
      dependentvariable=default_args['dependentvariable']
      # Assign the independentvariable streams
      independentvariables=default_args['independentvariables'] #"Voltage_preprocessed_AnomProb,Current_preprocessed_AnomProb"
            
      rollbackoffsets=int(default_args['rollbackoffsets'])
      consumeridtrainingdata2=default_args['consumeridtrainingdata2']
      partition_training=default_args['partition_training']
      producerid=default_args['producerid']
      consumefrom=default_args['consumefrom']

      topicid=int(default_args['topicid'])      
      fullpathtotrainingdata=default_args['fullpathtotrainingdata']

     # These are the conditions that sets the dependent variable to a 1 - if condition not met it will be 0
      processlogic=default_args['processlogic'] #'classification_name=failure_prob:Voltage_preprocessed_AnomProb=55,n:Current_preprocessed_AnomProb=55,n'
      
      identifier=default_args['identifier']

      producetotopic = default_args['ml_data_topic']
        
      array=int(default_args['array'])
      transformtype=default_args['transformtype'] # Sets the model to: log-lin,lin-log,log-log
      sendcoefto=default_args['sendcoefto']  # you can send coefficients to another topic for further processing
      coeftoprocess=default_args['coeftoprocess']  # indicate the index of the coefficients to process i.e. 0,1,2
      coefsubtopicnames=default_args['coefsubtopicnames']  # Give the coefficients a name: constant,elasticity,elasticity2

    
     # Call HPDE to train the model
      result=maadstml.viperhpdetraining(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                      companyname,consumeridtrainingdata2,producerid, HPDEHOST,
                                      viperconfigfile,enabletls,partition_training,
                                      deploy,modelruns,modelsearchtuner,HPDEPORT,offset,islogistic,
                                      brokerhost,brokerport,networktimeout,microserviceid,topicid,maintopic,
                                      independentvariables,dependentvariable,rollbackoffsets,fullpathtotrainingdata,processlogic,identifier)    
 

def windowname(wtype,sname,dagname):
    randomNumber = random.randrange(10, 9999)
    wn = "python-{}-{}-{},{}".format(wtype,randomNumber,sname,dagname)
    with open("/tmux/pythonwindows_{}.txt".format(sname), 'a', encoding='utf-8') as file: 
      file.writelines("{}\n".format(wn))
    
    return wn

def startml(**context):
       sd = context['dag'].dag_id
       sname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_solutionname".format(sd))
       pname=context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_projectname".format(sd))
       
       VIPERTOKEN = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERTOKEN".format(sname))
       VIPERHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERHOSTML".format(sname))
       VIPERPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_VIPERPORTML".format(sname))
       HTTPADDR = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HTTPADDR".format(sname))
       HPDEADDR = default_args['HPDEADDR']
    
       HPDEHOST = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HPDEHOST".format(sname))
       HPDEPORT = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_HPDEPORT".format(sname))
       chip = context['ti'].xcom_pull(task_ids='step_1_solution_task_getparams',key="{}_chip".format(sname)) 
        
       ti = context['task_instance']
       ti.xcom_push(key="{}_preprocess_data_topic".format(sname), value=default_args['preprocess_data_topic'])
       ti.xcom_push(key="{}_ml_data_topic".format(sname), value=default_args['ml_data_topic'])
       ti.xcom_push(key="{}_modelruns".format(sname), value="_{}".format(default_args['modelruns']))
       ti.xcom_push(key="{}_offset".format(sname), value="_{}".format(default_args['offset']))
       ti.xcom_push(key="{}_islogistic".format(sname), value="_{}".format(default_args['islogistic']))
       ti.xcom_push(key="{}_networktimeout".format(sname), value="_{}".format(default_args['networktimeout']))
       ti.xcom_push(key="{}_modelsearchtuner".format(sname), value="_{}".format(default_args['modelsearchtuner']))
       ti.xcom_push(key="{}_dependentvariable".format(sname), value=default_args['dependentvariable'])
       ti.xcom_push(key="{}_independentvariables".format(sname), value=default_args['independentvariables'])

       rollback=default_args['rollbackoffsets']
       if 'step5rollbackoffsets' in os.environ:
         ti.xcom_push(key="{}_rollbackoffsets".format(sname), value="_{}".format(os.environ['step5rollbackoffsets']))
         rollback=os.environ['step5rollbackoffsets']
       else:  
         ti.xcom_push(key="{}_rollbackoffsets".format(sname), value="_{}".format(default_args['rollbackoffsets']))

       processlogic=default_args['processlogic']
       if 'step5processlogic' in os.environ:
         ti.xcom_push(key="{}_processlogic".format(sname), value="{}".format(os.environ['step5processlogic']))
         processlogic=os.environ['step5processlogic']
       else:  
         ti.xcom_push(key="{}_processlogic".format(sname), value="{}".format(default_args['processlogic']))

       independentvariables=default_args['independentvariables']
       if 'step5independentvariables' in os.environ:
         ti.xcom_push(key="{}_independentvariables".format(sname), value="{}".format(os.environ['step5independentvariables']))
         independentvariables=os.environ['step5independentvariables']
       else:  
         ti.xcom_push(key="{}_independentvariables".format(sname), value="{}".format(default_args['independentvariables']))

  
       ti.xcom_push(key="{}_topicid".format(sname), value="_{}".format(default_args['topicid']))
       ti.xcom_push(key="{}_consumefrom".format(sname), value=default_args['consumefrom'])
       ti.xcom_push(key="{}_fullpathtotrainingdata".format(sname), value=default_args['fullpathtotrainingdata'])
       ti.xcom_push(key="{}_transformtype".format(sname), value=default_args['transformtype'])
       ti.xcom_push(key="{}_sendcoefto".format(sname), value=default_args['sendcoefto'])
       ti.xcom_push(key="{}_coeftoprocess".format(sname), value=default_args['coeftoprocess'])
       ti.xcom_push(key="{}_coefsubtopicnames".format(sname), value=default_args['coefsubtopicnames'])
       ti.xcom_push(key="{}_HPDEADDR".format(sname), value=HPDEADDR)

       dependentvariable=default_args['dependentvariable']
       rollbackoffsets=default_args['rollbackoffsets'] 
       islogistic=default_args['islogistic']
       preprocess_data_topic=default_args['preprocess_data_topic']
       ml_data_topic=default_args['ml_data_topic']
       fullpathtotrainingdata=default_args['fullpathtotrainingdata']

       repo=tsslogging.getrepo() 
       if sname != '_mysolution_':
        fullpath="/{}/tml-airflow/dags/tml-solutions/{}/{}".format(repo,pname,os.path.basename(__file__))  
       else:
         fullpath="/{}/tml-airflow/dags/{}".format(repo,os.path.basename(__file__))  
            
       wn = windowname('ml',sname,sd)     
       subprocess.run(["tmux", "new", "-d", "-s", "{}".format(wn)])
       subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "cd /Viper-ml", "ENTER"])
       subprocess.run(["tmux", "send-keys", "-t", "{}".format(wn), "python {} 1 {} {}{} {} {}{} {} {} \"{}\" \"{}\" \"{}\" {} \"{}\" \"{}\" \"{}\"".format(fullpath,VIPERTOKEN, HTTPADDR, VIPERHOST, VIPERPORT[1:], 
                                             HPDEADDR, HPDEHOST, HPDEPORT[1:],rollbackoffsets,processlogic,independentvariables,dependentvariable, islogistic,
                                             preprocess_data_topic, ml_data_topic,fullpathtotrainingdata), "ENTER"],capture_output=True, text=True)        
       pane_pid = subprocess.check_output([
          "tmux", "list-panes", "-t", wn, "-F", "#{pane_pid}"
       ]).decode().strip()

       with open("/tmux/step5_ml.txt", 'w', encoding='utf-8') as file: 
          file.write("{}\n".format(pane_pid))
          file.write("{}\n".format(wn))
          file.write("python {} 1 {} {}{} {} {}{} {} {} \"{}\" \"{}\" \"{}\" {} \"{}\" \"{}\" \"{}\"".format(fullpath,VIPERTOKEN, HTTPADDR, VIPERHOST, VIPERPORT[1:], 
                                             HPDEADDR, HPDEHOST, HPDEPORT[1:],rollbackoffsets,processlogic,independentvariables,dependentvariable, islogistic,
                                             preprocess_data_topic, ml_data_topic,fullpathtotrainingdata))         

if __name__ == '__main__':
    if len(sys.argv) > 1:
       if sys.argv[1] == "1":          
        repo=tsslogging.getrepo()
            
        VIPERTOKEN = sys.argv[2]
        VIPERHOST = sys.argv[3]
        VIPERPORT = sys.argv[4]
        HPDEHOST = sys.argv[5]
        HPDEPORT = sys.argv[6]
        rollbackoffsets =  sys.argv[7]
        default_args['rollbackoffsets'] = rollbackoffsets
        processlogic =  sys.argv[8]
        default_args['processlogic'] = processlogic
        independentvariables =  sys.argv[9]
        default_args['independentvariables'] = independentvariables

         #-------------------
        default_args['dependentvariable'] =  sys.argv[10]
        default_args['islogistic'] =  sys.argv[11]
        default_args['preprocess_data_topic'] =  sys.argv[12]
        default_args['ml_data_topic'] =  sys.argv[13]

        args = sys.argv[14]  
        if args.startswith("/"):
           default_args['fullpathtotrainingdata'] = sys.argv[14]
        else:
           default_args['fullpathtotrainingdata'] =  f"/rawdata/ml/{args}"
          
         #------------------------
         
      #  subprocess.run("rm -rf {}".format(default_args['fullpathtotrainingdata']), shell=True)

         
        tsslogging.locallogs("INFO", "STEP 5: Machine learning started")
        try: 
          f = open("/tmux/step5.txt", "w")
          f.write(default_args['fullpathtotrainingdata'])
          f.close()
        except Exception as e:
          pass

        while True:
         try:     
          performSupervisedMachineLearning()
#          time.sleep(10)
         except Exception as e:
          tsslogging.locallogs("ERROR", "STEP 5: Machine Learning DAG in {} {}".format(os.path.basename(__file__),e))
          time.sleep(10)
          continue