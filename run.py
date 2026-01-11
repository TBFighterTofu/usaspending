"""To download data, fill out the requested fields, and run this script using the command:

python run.py

"""

from src.awards import AwardSearchDownload
import pandas as pd
from datetime import datetime

#------------------
#------------------
#### FILL THESE OUT

# the tas number I care about
tas_code = "072-019-2024/2025-1031-000"  

# if I'm looking for a few specific awards:
award_ids = ["72061521CA00007", "72061521CA00008"]  

# or if I have a spreadsheet in this folder with all the award ids, and no header, called awards_list.csv:
awards_table = pd.read_csv("awards_list.csv", header = None, names = ["award_id"])
award_ids = list(awards_table["award_id"])

# a name for my export files
summary_name = "my_awards"  

# re-download award data if it was last downloaded before this date
critical_download_date = datetime(year = 2026, month = 1, day = 1)

#### THANKS FOR FILLING THOSE OUT
#------------------
#------------------

downloader = AwardSearchDownload(
    tas_code = tas_code,
    award_ids = award_ids,
    summary_name = summary_name,
    critical_download_date = critical_download_date
)  # an object that can download the data you want from usaspending

downloader.search_awards()  # get summary data for all the awards, and look up their long award IDs, which we'll need for downloading the awards

downloader.download_awards()  # download the zip file for each award

downloader.combine_awards()  # combine and filter the data into a single csv
