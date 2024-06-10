# Purpose of this module is to manage the config files that has values for common and dag variables 
# Function read_json takes in the config file path and returns configs from json files in map variable format based on the  environment.
# Function print config prints both the map variables that are being passed from python operator
import json
from airflow.utils.log.logging_mixin import LoggingMixin

def read_json(json_file_path):
    map_variables={}
    LoggingMixin().log.info("json_file_path: {}".format(json_file_path))
    """Reads a JSON file and returns a map variable."""
    with open(json_file_path, "r") as f:
        json_data = f.read()
        
    json_dict = json.loads(json_data)
    if "/Prod/" in json_file_path:
        configs=json_dict["Prod"]
    elif "/Qa/" in json_file_path:
        configs=json_dict["Qa"]
    elif "/Dev/" in json_file_path:
        configs=json_dict["Dev"]
    else:
         configs={}

    if configs:
        map_variables = {key: value for key, value in configs.items()}
    return map_variables

# to check the arguments and pass to the dictionary 
# declare the argumnet as None if it is empty 
def printconfigs(arg1, arg2=None):
    """Prints the configuration variables in the given arguments."""
    if arg1 is not None:
        for key, value in arg1.items():
            if "SECRET" not in key.upper():
                LoggingMixin().log.info("{}: {}".format(key, value))
    else:
        LoggingMixin().log.info("arg1 is None")
      
    if arg2 is not None:
        for key, value in arg2.items():
          if "SECRET" not in key.upper():
            LoggingMixin().log.info("{}: {}".format(key, value))
    else:
        LoggingMixin().log.info("arg2 is None")
            
    return "Printed config variable"