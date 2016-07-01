#The first draft of the enveloping script that runs the pipeline.
import logging
import yaml
#Import external modules

import . from datasets

#Import the internal modules

def main(config_file_name):
  #Set up the Logger
  # define logging
  logging.basicConfig(format="%(asctime)s %(message)s",
                 filename="default.log", level=logging.INFO)
    log = logging.getLogger("Police EIS")

    screenlog = logging.StreamHandler(sys.stdout)
    screenlog.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
    screenlog.setFormatter(formatter)
    log.addHandler(screenlog)
      #Load the yaml file.
    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
        log.info("Loaded experiment file")
    except:
        log.exception("Failed to get experiment configuration file!")
  # Brian will work on the following 
  grab_from_dataset(config) #this creates df features and labels in the postgres
  [train,test]=splits(features, labels) # this 
  
  
  
  

