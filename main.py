"""The main file of the robot which will install all requirements in
a virtual environment and then start the actual process.
"""

import subprocess
import os
import sys
import pathlib

# Check if virtual environment already exists to avoid re-creation
script_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_directory)

venv_dir = os.path.join(script_directory, ".venv")

# Install 'uv' if not already installed (avoid redundant installation)
def install_uv():
    """Install 'uv' if not already installed (avoid redundant installation)."""
    try:
        subprocess.run([sys.executable, "-m", "uv", "--version"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        subprocess.run([sys.executable, "-m", "pip", "install", "uv"], check=True)

# Create virtual environment only if it doesn't exist
if not os.path.exists(venv_dir):
    install_uv()
    subprocess.run(["uv", "venv"], check=True)

# Install packages in the virtual environment (skip if already installed)
try:
    packages_file = pathlib.Path(script_directory) / ".venv_installed_marker"
    if not packages_file.exists():
        subprocess.run(["uv", "pip", "install", "."], check=True, timeout=120)
        packages_file.touch()
except subprocess.CalledProcessError as e:
    print(f"Package installation failed: {e}", file=sys.stderr)
    sys.exit(1)

# Execute the command
command_args = [os.path.join(".venv", "Scripts", "python"), "-m", "robot_framework"] + sys.argv[1:]
subprocess.run(command_args, check=True)
