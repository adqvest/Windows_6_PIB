#!/usr/bin/env python3
#!/usr/bin/env python3
import subprocess
import sys
import os
from pathlib import Path

# Define your virtual environment Python path
# VENV_PYTHON = r"D:\Work\AMC_Crawlee_code\.venv\Scripts\python.exe"
VENV_PYTHON = r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler\crawlee_env\Scripts\python.exe"
SCRIPT_PATH = r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler\AMC_CRAWLER\CRAWLEE_PW_VS_CODE.py"

def run_crawler():
    """Run the crawler using the specified virtual environment"""
    
    # Verify virtual environment exists
    if not os.path.exists(VENV_PYTHON):
        print(f"❌ Virtual environment not found: {VENV_PYTHON}")
        sys.exit(1)
    
    # Verify script exists
    if not os.path.exists(SCRIPT_PATH):
        print(f"❌ Script not found: {SCRIPT_PATH}")
        sys.exit(1)
    
    # Get command line arguments (config file path)
    config_file = "amc_config.json"  # Default config file
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    
    # Verify config file exists
    if not os.path.exists(config_file):
        print(f"❌ Config file not found: {config_file}")
        print("Please create amc_config.json or specify a valid config file path")
        sys.exit(1)
    
    # Construct the command
    cmd = [VENV_PYTHON, SCRIPT_PATH, config_file]
    
    print(f"🚀 Running crawler with virtual environment:")
    print(f"   Python: {VENV_PYTHON}")
    print(f"   Script: {SCRIPT_PATH}")
    print(f"   Config: {config_file}")
    
    try:
        # Run the subprocess
        result = subprocess.run(cmd, check=True)
        print("✅ Crawler completed successfully!")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"❌ Crawler failed with exit code: {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print(f"❌ Script not found: {SCRIPT_PATH}")
        return 1

if __name__ == "__main__":
    exit_code = run_crawler()
    sys.exit(exit_code)
