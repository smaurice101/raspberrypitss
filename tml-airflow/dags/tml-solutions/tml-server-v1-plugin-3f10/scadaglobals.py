import threading

active_sim_threads = []
active_read_threads = []

# Initialize the STOP SIGNALS (Events)
stop_sim = threading.Event()
stop_read = threading.Event()

read_job = None
read_thread = None
mqtt_job = None
mqtt_thread = None
mqtt_client = None
sim_thread = None
sim_job = None
mainmode = ""
mainmodes = []
