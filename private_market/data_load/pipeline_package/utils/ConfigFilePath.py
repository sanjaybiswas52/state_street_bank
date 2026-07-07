import os
import logging

# Directory path for resources folder (not a specific file)
config_base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources')
logging.info(config_base_path)
