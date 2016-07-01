#The first draft of the enveloping script that runs the pipeline.

#Import external modules

#Import the internal modules

def main(config_file_name):
  #Set up the Logger
  
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
  
  

