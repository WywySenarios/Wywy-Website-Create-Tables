import logging
import yaml
from Wywy_Website_Types import MainConfig

logger = logging.getLogger()

# peak at config
with open("/home/create_tables/config.yml", "r") as file:
    CONFIG: MainConfig = yaml.safe_load(file)
    logger.debug(f"Loaded config: {CONFIG}")
