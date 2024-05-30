import json
import logging
import sys
from pathlib import Path

import yaml

# from common.py_libs.configs import load_config_file
# from common.py_libs.dag_generator import generate_file_from_template

_THIS_DIR = Path(__file__).resolve().parent


"""Library for Cortex Config related functions."""

import json
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def load_config_file(config_file) -> Dict[str, Any]:
    """Loads a json config file to a dictionary.

    Args:
        config_file: Path of config json file.

    Returns:
        Config as a dictionary.
    """
    logger.debug("Input file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise Exception(e_msg) from None

        logger.info("Using the following config:\n %s",
                    json.dumps(config, indent=4))

    return config


from string import Template
from pathlib import Path


def generate_file_from_template(template_file_path: Path,
                                output_file_path: Path, **subs: str):
    """Creates fully resolved file from template using substitutions.

    Args:
        template_file_path: Full template file path.
        output_file_path: Full output file path where resolved file will be
            created.
        subs: Substitutes to be applied to the template.
    """
    with open(template_file_path, mode="r", encoding="utf-8") as template_file:
        dag_template = Template(template_file.read())
    generated_code = dag_template.substitute(**subs)

    output_file_path.parent.mkdir(exist_ok=True, parents=True)
    with output_file_path.open(mode="w+", encoding="utf-8") as generated_file:
        generated_file.write(generated_code)


_CONFIG_FILE = Path(_THIS_DIR, "../config/config.json")

_GENERATED_FILE_DIR = Path(_THIS_DIR,"../")
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")

generated_file_prefix = "variables"

# Python file generation
#########################
python_template_file = Path(_TEMPLATE_DIR, "variables.tfvars")
output_py_file_name = generated_file_prefix + ".tfvars"
output_py_file = Path(_GENERATED_FILE_DIR, output_py_file_name)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating config files...")

    # Load configs to get various parameters needed for the DAG generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Read params from the config
    project_id = config_dict.get("projectId")
    composer_location=config_dict.get("location")
    project_number = config_dict.get("projectNumber")
    composer_env_name = config_dict.get("composerEnvName")
    custom_service_account = config_dict.get("serviceAccountName")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  project = %s \n"
        "  project_number = %s \n"
        "  composer_location = %s \n"
        "  composer_env_name = %s \n"
        "  custom_service_account = %s \n"
        "---------------------------------------\n", project_id, project_number,composer_location, composer_env_name,custom_service_account)

    Path(_GENERATED_FILE_DIR).mkdir(exist_ok=True, parents=True)


    # Pass parameters to the py_subs dictionary
    py_subs = {
        "project_id": project_id,
        "projectNumber": project_number,
        "location" : composer_location,
        "composerEnvName" : composer_env_name,
        "serviceAccountName" : custom_service_account
    }

    # Generate Python file from template
    generate_file_from_template(python_template_file, output_py_file, **py_subs)

    logging.info("Generated variable files ")

if __name__ == "__main__":
    main()