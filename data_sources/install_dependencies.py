import subprocess
import sys
import json

def install_system_packages(packages):
    try:
        subprocess.run(["sudo", "apt", "update"])
        subprocess.run(["sudo", "apt", "upgrade"])
        subprocess.run(["sudo", "apt", "install", "-y"] + packages)
        print("System packages installed successfully.")
    except subprocess.CalledProcessError as e:
        print("Failed to install system packages.")
        sys.exit(1)

def install_python_dependencies(dependencies):
    try:
        subprocess.run([sys.executable, "-m", "pip", "install"] + dependencies)
        print("Python dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print("Failed to install Python dependencies.")
        sys.exit(1)

if __name__ == '__main__':
    # Read requirements.json file
    with open('kafka_requirements.json', 'r') as requirements_file:
        requirements = json.load(requirements_file)

    # Install Ubuntu system packages
    if "system_packages" in requirements:
        system_packages = requirements["system_packages"]
        install_system_packages(system_packages)

    # Install Python dependencies
    if "python_dependencies" in requirements:
        python_dependencies = requirements["python_dependencies"]
        install_python_dependencies(python_dependencies)