import os
import yaml

def load_config_as_env(path="config.yaml"):
    with open(path, "r") as file:
        config = yaml.safe_load(file)
        for key, value in config.items():
            os.environ[key] = str(value)