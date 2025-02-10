"""The main file of the robot which will install all requirements in
a virtual environment and then start the actual process.
"""

import subprocess
import os
import sys
import pathlib
import fcntl


# Check if virtual environment already exists to avoid re-creation
script_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_directory)

venv_dir = os.path.join(script_directory, ".venv")
lock_file_path = os.path.join(script_directory, ".venv.lock")


# Install 'uv' if not already installed (avoid redundant installation)
def install_uv():
    """Install 'uv' if not already installed (avoid redundant installation)."""
    try:
        subprocess.run(
            [sys.executable, "-m", "uv", "--version"], check=True, capture_output=True
        )
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
        with open(lock_file_path, "w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                subprocess.run(["uv", "pip", "install", "."], check=True, timeout=120)
                packages_file.touch()
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
except subprocess.CalledProcessError as e:
    print(f"Package installation failed: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:  # pylint: disable=broad-except
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)

# Execute the command
command_args = [
    os.path.join(venv_dir, "Scripts", "python"),
    "-m",
    "robot_framework",
] + sys.argv[1:]
try:
    subprocess.run(command_args, check=True)
except subprocess.CalledProcessError as e:
    print(f"Command execution failed: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:  # pylint: disable=broad-except
    print(
        f"An unexpected error occurred during command execution: {e}", file=sys.stderr
    )
    sys.exit(1)
