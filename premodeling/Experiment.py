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


def main():
    """
    Testing code.
    """
    with open("default.yaml", 'r') as f:
            config = yaml.load(f)
    #Random testing code
    experiments = generate_experiments(config)
    print(len(experiments))
    print(experiments[23].parameters)


if __name__ == '__main__':
    main()
