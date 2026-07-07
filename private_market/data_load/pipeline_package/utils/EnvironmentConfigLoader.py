import json
import os
from pipeline_package.utils.ConfigFilePath import config_base_path

class EnvironmentConfigLoader:
    @staticmethod
    def get_config(env: str, layer_defaults: dict, base_path: str = config_base_path):
        """
        Loads environment-specific config from a JSON file and merges it with layer defaults.
        :param env: Environment name (e.g., 'dev', 'prod')
        :param layer_defaults: Dictionary of layer-specific default config
        :param base_path: Base path to environment config files
        :return: Merged config dictionary
        """
        config_path = os.path.join(base_path, f"{env}_config.json")
        if not os.path.exists(config_path):
            raise ValueError(f"Config file for environment '{env}' not found: {config_path}")
        with open(config_path, "r") as f:
            env_config = json.load(f)
        # Merge: env_config overrides layer_defaults
        merged_config = {**layer_defaults, **env_config}
        return merged_config
