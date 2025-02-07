"""The main file of the robot which will install all requirements in
a virtual environment and then start the actual process.
"""

import subprocess
import os
import sys
import secrets


script_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_directory)

try:
    import uv
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "uv"], check=True)

unique_id = secrets.token_hex(8)
VENV_NAME = f".venv_{unique_id}"

subprocess.run(["uv", "venv", VENV_NAME], check=True)

cache_dir = os.path.join(script_directory, "pip_cache")
os.makedirs(cache_dir, exist_ok=True)

subprocess.run([
    "uv",
    "--venv", VENV_NAME,
    "pip", "install", ".",
    "--cache-dir", cache_dir
], check=True)

python_exe = os.path.join(script_directory, VENV_NAME, "Scripts", "python")
command_args = [python_exe, "-m", "robot_framework"] + sys.argv[1:]
subprocess.run(command_args, check=True)
