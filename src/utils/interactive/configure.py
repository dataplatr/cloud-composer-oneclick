"""Data Foundation Deployment UI"""


import logging
import os
import subprocess
import typing

from prompt_toolkit import PromptSession
from prompt_toolkit.cursor_shapes import CursorShape, to_cursor_shape_config
from prompt_toolkit.shortcuts import checkboxlist_dialog, radiolist_dialog
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML
from google.cloud import resourcemanager_v3
import googleapiclient.discovery
from googleapiclient.errors import HttpError
from google.api_core.exceptions import Unauthorized, Forbidden, Conflict
from google.cloud import bigquery, storage
from google.cloud.bigquery.enums import EntityTypes

import google.auth

from completers import (GCPProjectCompleter,
                        RegionsCompleter,
                        StorageBucketCompleter)
from prompt import (get_value, yes_no, print_formatted, print_formatted_json)
from constants import DF_TITLE


def get_cloud_build_account(project_id: str) -> str:
        """Retrieves GCP project Cloud Build account principal by project name/id.

        Args:
            project_id (str): project id

        Returns:
            str: Cloud Build account principal
        """
        crm = googleapiclient.discovery.build("cloudresourcemanager", "v1",
                                            cache_discovery=False)
        project = crm.projects().get(projectId=project_id).execute()
        project_number = project["projectNumber"]
        return project_number

def configure(in_cloud_shell: bool,
              default_config: typing.Dict[str, typing.Any],
              existing_config: typing.Dict[str, typing.Any],
              default_project: str) -> typing.Optional[
                                            typing.Dict[str,
                                                typing.Any]]:
    """UI driver for Data Foundation configurator.

    Args:
        in_cloud_shell (bool): True if running in Cloud Shell.
        default_config (typing.Dict[str, typing.Any]): default configuration
                                                       dictionary
        existing_config (typing.Dict[str, typing.Any]): loaded configuration
                                                        dictionary
        default_project (str): default project id to use

    Returns:
        typing.Optional[ typing.Dict[str, typing.Any]]: configured configuration
                                                        dictionary
    """

    config = default_config
    went_with_existing = False
    # If source project, target project, location and target bucket
    # are specified in config.json,
    # we assume it's initialized, and use existing config.
    if (existing_config.get("projectId", "") != "" and
        existing_config.get("projectNumber", "") != "" and
         existing_config.get("location", "") != "" and
          existing_config.get("composerEnvName", "") != ""):
        if yes_no(
            f"{DF_TITLE} Configuration",
            HTML("There is an existing configuration "
            "in <b>config/config.json:</b>\n"
            f"   Project: <b>{existing_config['projectId']}</b>\n"
            f"   Location: <b>{existing_config['location']}</b>"
            "\n\nWould you like to load it?"),
            full_screen=True
            ):
            print_formatted(
                "\n\n🦄 Using existing configuration in config.json:",
                bold=True)
            print_formatted_json(existing_config)
            config = existing_config
            went_with_existing = True

    if not default_project:
        default_project = os.environ.get("GOOGLE_CLOUD_PROJECT",
                                         None)  # type: ignore
    else:
        os.environ["GOOGLE_CLOUD_PROJECT"] = default_project

    print_formatted("\nInitializing...", italic=True, end="")
    try:
        logging.disable(logging.WARNING)
        credentials, _ = google.auth.default()
    finally:
        logging.disable(logging.NOTSET)
    project_completer = GCPProjectCompleter(credentials)
    if not default_project:
        default_project = ""

    session = PromptSession()
    session.output.erase_screen()
    print("\r", end="")
    print_formatted(f"{DF_TITLE}\n")

    defaults = ["CreateComposer"]

    while True:
        dialog = checkboxlist_dialog(
            title=HTML(DF_TITLE),
            text=HTML(
                f"{DF_TITLE}.\n\n"
                "Please confirm the deployment."),
            values=[
                ("CreateComposer", "NewCloudComposer"),
            ],
            default_values=defaults,
            style=Style.from_dict({
                "checkbox-selected": "bg:lightgrey",
                "checkbox-checked": "bold",
            }))
        dialog.cursor = to_cursor_shape_config(CursorShape.BLINKING_UNDERLINE)
        results = dialog.run()

        if not results:
            print_formatted("See you next time! 🦄")
            return None

        config["CreateComposer"] = "CreateComposer" in results

        if (config["CreateComposer"] is False):
            if yes_no(
                    f"{DF_TITLE} Configuration",
                    "Please select the option to proceed.",
                    yes_text="Try again",
                    no_text="Cancel"
            ):
                continue
            else:
                print_formatted("See you next time! 🦄")
                return None
        break

    project = config.get("projectId", default_project)
    if project == "":
        project = default_project

    composer_location = config.get("location", "").lower()
    config["location"] = composer_location

    composerEnvName = config.get("composerEnvName", "")
    config["composerEnvName"] = composerEnvName

    service_account_name = config.get("serviceAccountName","")
    config["serviceAccountName"] = service_account_name

    if project == "":
        project = None
    if composer_location == "":
        composer_location = "us-central1"
    if composerEnvName == "":
        composerEnvName = "Example-Environment"
        
    
    print_formatted("\nEnter or confirm configuration values:",
                    bold=True)

    project = get_value(
        session,
        "GCP Project",
        project_completer,
        project or "",
        description="Specify the Project (existing).",
        allow_arbitrary=False,
    )
    os.environ["GOOGLE_CLOUD_PROJECT"] = project

    config["projectId"] = project

    project_number = get_cloud_build_account(project)

    project_number = get_value(
        session,
        "GCP Project Number",
        project_completer,
        project_number or "",
        description="Specify the Project Number (existing).",
        allow_arbitrary=False,
    )

    config["projectNumber"] = project_number

    print_formatted("Retrieving regions...", italic=True, end="")
    regions_completer = RegionsCompleter()
    print("\r", end="")

    composer_location = get_value(
            session,
            "Cloud Composer Location",
            regions_completer,
            default_value=composer_location.lower(),
            description="Specify GCP Location for  Cloud Composer.",
        )
    composer_location = composer_location.lower()
    config["location"] = composer_location


    composer_env_name = get_value(
            session,
            "Cloud Composer Environment Name",
            default_value=composerEnvName.lower(),
            description="Specify Cloud Composer Name.",
        )
    composer_env_name = composer_env_name.lower()
    config["composerEnvName"] = composer_env_name

    service_account_name = get_value(
            session,
            "service Account Name",
            default_value=service_account_name.lower(),
            description="Specify Service Account Name.",
        )
    service_account_name = service_account_name.lower() 
    config["serviceAccountName"] = service_account_name
    
    if not config:
        print_formatted(
            "Please check the documentation. "
            "See you next time! 🦄")
        return None

    return config
