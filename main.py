"""The main file of the robot which will install all requirements in
a virtual environment and then start the actual process.
"""

import subprocess
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Change to the script's directory
script_directory = os.path.dirname(os.path.realpath(__file__))
logger.debug(f"Changing current working directory to the script's directory: {script_directory}")
os.chdir(script_directory)

# Install 'uv' if not already installed
try:
    logger.debug("Checking if 'uv' is already installed...")
    subprocess.run(["uv", "--version"], check=True)
    logger.info("'uv' is already installed.")
except subprocess.CalledProcessError:
    logger.info("'uv' not found. Installing 'uv'...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "uv"], check=True)
        logger.info("'uv' installed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install 'uv': {e}")
        sys.exit(1)

# Create the virtual environment
venv_path = f".venv_{os.getpid()}"
logger.debug(f"Creating a virtual environment at: {venv_path}")
try:
    subprocess.run(["uv", "venv", "--path", venv_path], check=True)
    logger.info(f"Virtual environment created successfully at: {venv_path}")
except subprocess.CalledProcessError as e:
    logger.error(f"Failed to create virtual environment: {e}")
    sys.exit(1)

# Install packages in the virtual environment
logger.debug("Installing packages in the virtual environment...")
try:
    subprocess.run(["uv", "pip", "install", "."], check=True, timeout=300)
    logger.info("Packages installed successfully.")
except subprocess.CalledProcessError as e:
    logger.error(f"Failed to install packages: {e}")
    sys.exit(1)
except subprocess.TimeoutExpired as e:
    logger.error("Package installation timed out.")
    sys.exit(1)

# Run the main process
command_args = [r".venv\Scripts\python", "-m", "robot_framework"] + sys.argv[1:]
logger.debug(f"Executing the main process with command: {' '.join(command_args)}")
try:
    subprocess.run(command_args, check=True)
    logger.info("Main process executed successfully.")
except subprocess.CalledProcessError as e:
    logger.error(f"Main process failed: {e}")
    sys.exit(1)
