import yaml
import itertools

class Experiment(object):
    """
    A class to encapsulate one experiment. An experiment should correspond as closely as possible to an actual use scenario by the project partner.
    Hence, it will include the model, the model hyperparameters, the prediction window, the prediction length...

    Each Experiment object encapsulates:
    * config (dict of dicts)
    * model (string)
    * parameters (dict)
    """

    def __init__(self, config, model, parameters):
        self.config = config.copy()
        self.model = model
        self.parameters = parameters


    def __hash__(self):
        """
        Adding the hashing and equals functions, so that we can use the Experiment objects as dictionary keys.
        """
        return hash((self.model, hash(frozenset(self.parameters.items())) ) )

    def __eq__(self, other):
        models_are_the_same = (self.model == other.model)
        items_are_the_same =  (len(set(self.parameters.items()) ^ set(other.parameters.items())) == 0) #Take the symmetric difference of the sets of two items and check that the symmetric difference is empty.
        return (   models_are_the_same & items_are_the_same )

def generate_experiments(yaml_config):
    """
    Parse the yaml file to generate runnable experiments as a direct product of the lists of different settings

    Returns: A list of Experiment objects.
    """
    experiments = []

    #Create the implementation cross-section
    #print(yaml_config)
    implementation_settings_product = list(itertools.product(*yaml_config['implementation'].values()))
    implementation_crosssections = [dict(zip(yaml_config['implementation'].keys(),setting)) for setting in implementation_settings_product]

    #Iterate over models, for each model, create a parameters cross-section,
    # then loop over parameters and cross-sections and create an experiment for each
    for model in yaml_config['model']:
        model_parameters_specific = yaml_config['parameters'][model]
        model_parameters_product = list(itertools.product(*model_parameters_specific.values()))
        parameters_crosssections = [dict(zip(yaml_config['parameters'][model].keys(),setting)) for setting in model_parameters_product]
        for implementation_crosssection in implementation_crosssections:
            for parameters_crosssection in parameters_crosssections:
                merged_crosssection = parameters_crosssection.copy(); merged_crosssection.update(implementation_crosssection)
                exp = Experiment(yaml_config,model,merged_crosssection)
                experiments.append(exp)

    return(experiments)
