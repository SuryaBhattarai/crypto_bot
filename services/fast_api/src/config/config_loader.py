"""
Module for loading configuration data from a YAML file.

This module provides the `load_config` function, which reads configuration data from a YAML file
and returns it as a dictionary. This is useful for loading application settings, API keys, and
other configuration details stored in an external YAML file.

Dependencies:
    - pathlib
    - typing
    - yaml

Example:
    Load configuration settings from a YAML file:

    ```python
    from pathlib import Path
    from src.config.config_loader import load_config

    config_path = Path("config.yml")
    config_data = load_config(config_path)
    print(config_data)
    ```
"""

from pathlib import Path
from typing import Any, Dict
import yaml

def load_config(file_path: Path) -> Dict[str, Any]:
    """
    Loads configuration data from a YAML file and returns it as a dictionary.

    This function reads a YAML file from the specified file path, parses its contents, and returns
    a dictionary containing the configuration data. If the file does not exist or cannot be read,
    an exception will be raised.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the YAML configuration file.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the configuration data.

    Notes
    -----
    - The YAML file should be structured in a key-value format.
    - This function uses `yaml.safe_load` to parse the file, ensuring safe loading of the data.

    Examples
    --------
    Load configuration settings from a YAML file named `config.yml`:

    ```python
    from pathlib import Path
    from src.config.config_loader import load_config

    config_path = Path("config.yml")
    config_data = load_config(config_path)
    print(config_data)
    ```
    """
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config
