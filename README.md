This repo is for downloading award transaction data from the USASpending API, looking for a specific TAS code and/or award ids.

The relevant code is in run.py, awards.py, and usa_types.py. The other files access other USASpending APIs.

Quick start for coding newbies:
1. Download a code editor such as [VSCode](https://code.visualstudio.com/download)
2. [Install python](https://code.visualstudio.com/docs/python/python-tutorial#_install-a-python-interpreter)
3. Open a new VSCode window. Click on "Clone Git Repository". Enter the url for this git repo (https://github.com/TBFighterTofu/usaspending.git). 
3. [Create a virtual environment](https://code.visualstudio.com/docs/python/python-tutorial#_create-a-virtual-environment)
4. Open a terminal (Terminal -> New Terminal)
5. Activate your virtual environment using `source venv/bin/activate` on macOS/Linux or `.\venv\Scripts\activate` on Windows.
5. In the terminal, run: `pip install -r requirements.txt`

Now your environment is set up! You don't need to rerun those installation steps, just activate your virtual environment before you start running python.
    
Now that your environment is set up:
1. Modify run.py with your desired tas codes, award IDs, and export file name
2. In the terminal, run `python run.py`

Status messages will start printing in your terminal, and files will start appearing in the data folder in this repo. If you want to change the folder where your files are exported, go to usa_types.py and change DATA_FOLDER.