# -*- coding: utf-8 -*-
"""
Created on Sun Aug 17 11:52:54 2025

@author: Santonu
"""
import time
import os
import sys
import subprocess

from datetime import timedelta  
from  datetime import datetime
from pytz import timezone

india_time = timezone('Asia/Kolkata')
today      = datetime.now()
days       = timedelta(1)
yesterday = today - days

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')

# working_dir = r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler"

os.chdir(working_dir)
from site_config import SiteConfigLoader


site_configs = SiteConfigLoader()._load_configs()
all_sites = list(site_configs.keys())
all_sites=SiteConfigLoader()._load_configs(info_type="site_groups")["InputBasedStrategy"]
not_req=SiteConfigLoader()._load_configs(info_type="site_groups")["InputBasedStrategy_Long"]
all_sites=list(set(all_sites)-set(not_req))

# print(f'TOTAL SITES::{len(all_sites)}\n{all_sites}')
print(all_sites)

all_sites=["Pantaloons"]

# venv_python = r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler\crawlee_env\Scripts\python.exe"
# script_path = r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler\data_collection_framework_V2.py"

# venv_python = r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler\crawlee_env\Scripts\python.exe"
# script_path = r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler\data_collection_framework_V2.py"

venv_python = r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler\crawlee_env\Scripts\python.exe"
script_path = r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler\data_collection_framework_V2.py"

# venv_python = r"C:\Users\Santonu\Adqvest_Crawler\crawlee_env\Scripts\python.exe"
# script_path = r"C:\Users\Santonu\Adqvest_Crawler\data_collection_framework_V2.py"

def print_dashboard(running_processes):
    """Print live status of all running processes."""
    os.system("cls" if os.name == "nt" else "clear")  # clear screen
    print("Crawler Dashboard\n")
    print(f"{'Site':<20}{'PID':<10}{'Status'}")
    print("-" * 50)

    for s, p in running_processes:
        status = "Running"
        if p.poll() is not None:
            status = f"Done (exit {p.returncode})"
        print(f"{s:<20}{p.pid:<10}{status}")

    print("-" * 50)

def run_program():
    MAX_CONCURRENT = 1
    running_processes = []

    for site in all_sites:
        # Build command
        command = [venv_python, script_path, "--site", site]
        print(f"\nStarting process for: {site}")
        # print(command)

        # Start process
        process = subprocess.Popen(command,shell=True)
        running_processes.append((site, process))

        # If too many running, wait until one finishes
        while len(running_processes) >= MAX_CONCURRENT:
            # print_dashboard(running_processes)
            for s, p in running_processes:
                ret = p.poll()
                if ret is not None:  # process finished
                    print(f"Finished {s} (exit code {ret})")
                    running_processes.remove((s, p))
            time.sleep(60)

    # Wait for remaining processes to finish
    for s, p in running_processes:
        p.wait()
        print(f"Finished {s} (exit code {p.returncode})")

    print("\n---------------- ALL DONE ----------------")


if __name__ == "__main__":
    run_program()