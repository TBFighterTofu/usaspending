import json
from pathlib import Path
import pandas as pd
import time
import zipfile
import io
import requests
from datetime import datetime

from .usa_types import AwardType, DATA_FOLDER, SPENDING_BY_AWARD, AWARD_DOWNLOAD, usaid_tas


class AwardSearchDownload:
    """Finds the list of awards for a specific TAS code, downloads the zipfiles for all of the awards, and combines those zip files into a single CSV.
    
    For example, if you want to find all awards for 2024/2025 USAID, download their data, and combine the transations for that TAS into one csv file, run this:
        awards = USASpendingAwards("072-019-2024/2025-1031-000")
        awards.run_all()

    If you want to re-download the list of awards:
        awards.search_awards(overwrite=True)
    
    """

    valid_file_tags = ["FederalAccountFunding", "TransactionHistory", "Sub-Awards"]
    award_types = [AwardType.CONTRACT, AwardType.IDV, AwardType.LOAN, AwardType.GRANT, AwardType.DIRECT_PAYMENTS, AwardType.OTHER]

    def __init__(
            self,
            tas_code: str,
            award_ids: list[str] | None = None,
            summary_name: str | None = None,
            critical_download_date: datetime | None = None):
        self.tas_code = tas_code
        self.award_ids = award_ids    

        if award_ids is not None:
            if summary_name is None:
                raise ValueError("Please provide a summary_name for this set of awards.")
            self.summary_name = summary_name
        else:
            self.summary_name = self.tas_code.replace(r"/", "_")

        if critical_download_date is not None:
            self.critical_download_date = critical_download_date
        else:
            self.critical_download_date = None
        self.create_folders()

    def run_all(self):
        self.search_awards()
        self.download_awards()
        self.combine_awards()

    # -- File names

    def create_folders(self):
        # create the folders where we are going to save our data
        for folder in [self.summary_data_folder(), self.summary_folder(), self.downloads_folder(), self.pending_downloads_folder()]:
            if not folder.exists():
                folder.mkdir()

    def summary_data_folder(self) -> Path:
        return DATA_FOLDER / "summaries"

    def summary_folder(self) -> Path:
        """The folder where we will save summary tables for this set of awards."""
        return self.summary_data_folder() / self.summary_name
    
    def downloads_folder(self) -> Path:
        """Folder that holds the downloaded and unzipped data folders for all awards."""
        return DATA_FOLDER / "downloads"
    
    def pending_downloads_folder(self) -> Path:
        """Folder that holds info about award folder downloads that we've requested, that the API is still working on."""
        return DATA_FOLDER / "pending_downloads"

    def award_json(self) -> Path:
        """File where we will save the list of awards relevant to this TAS code."""
        return self.summary_folder() / f"awards_{self.summary_name}.json"

    def combined_csv(self, tag: str) -> Path:
        """CSV that combines one csv from each award download folder into one file. tag must be FederalAccountFunding, Sub-Awards, or TransactionHistory."""
        if tag not in self.valid_file_tags:
            raise ValueError(f"No file found named {tag}")
        return self.summary_folder() / f"combined_{tag}_{self.summary_name}.csv"
    
    # -- Searching for awards
     
    def _award_type_codes(self, award_type: AwardType) -> list[str]:
        """Get the eligible award type codes for the given award type. We have to provide these to the USASpending API to tell them what kinds of awards we're looking for."""
        if award_type == AwardType.CONTRACT:
            return ["A", "B", "C", "D"]
        elif award_type == AwardType.IDV:
            return ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
        elif award_type == AwardType.LOAN:
            return ["07", "08"]
        elif award_type == AwardType.GRANT:
            return ["02", "03", "04", "05"]
        elif award_type == AwardType.DIRECT_PAYMENTS:
            return ["06", "10"]
        elif award_type == AwardType.OTHER:
            return ["09", "11", "-1"]
        else:
            return []
        
    def _award_search_fields(self, award_type: AwardType) -> list[str]:
        """A list of column names to include in the table of awards that we're requesting from the API."""
        base_fields = [
            "generated_internal_id",
            "prime_award_recipient_id",
            "def_codes",
            "Award ID",
            "Recipient Name",
            "Recipient DUNS Number",
            "recipient_id",
            "Base Obligation Date",
            "Recipient Location",
            "Awarding Agency",
            "Awarding Agency Code",
            "Awarding Sub Agency",
            "Awarding Sub Agency Code",
            "Contract Award Type",
            "Award Type",
            "Funding Agency",
            "Funding Agency Code",
            "Funding Sub Agency",
            "Funding Sub Agency Code",
            "Description",
        ]
        if award_type == AwardType.CONTRACT:
            fields = ["Start Date", "End Date", "Award Amount", "Total Outlays", "Contract Award Type"]
        elif award_type == AwardType.IDV:
            fields = ["Start Date", "Award Amount", "Total Outlays", "Contract Award Type", "Last Date to Order", "NAICS", "PSC"]
        elif award_type == AwardType.LOAN:
            fields = ["Issued Date", "Loan Value", "Subsidy Cost", "SAI Number", "CFDA Number", "Assistance Listings", "primary_assistance_listing"]
        elif award_type in [AwardType.GRANT, AwardType.DIRECT_PAYMENTS, AwardType.OTHER]:
            fields =  ["Start Date", "End Date", "Award Amount", "Total Outlays", "Award Type", "SAI Number", "CFDA Number", "Assistance Listings", "primary_assistance_listing"]
        else:
            fields = []
        return base_fields + fields
    
    def _tas_filter(self) -> dict:
        """A dictionary that we pass in as an argument to the USASpending API to look for this TAS code."""
        return  {"require": [[self.tas_code]]}

    def _award_search_filter(self, page: int, award_type: AwardType) -> dict:
        """Filter that we're going to use in the awards search, specifying that we should look for this TAS code, to return the columns we defined in _award_search_fields, to get 100 results back, and which page to look on.
        
        Args:
            page: where in the list to look. i.e. if we're looking at page 2, then we're actually asking for row 101-200 in the awards table
            
        Output: a dictionary that we're going to send to the USASpending API."""
        query = {
            "filters": {
                "tas_codes": self._tas_filter(),
                "award_type_codes": self._award_type_codes(award_type)
            },
            "fields": self._award_search_fields(award_type),
            "limit": 100, 
            "page": page  
        }
        if self.award_ids is not None:
            # search for a specific list of awards. put the awards in double quotes so we're searching for exact matches, not fuzzy matches
            query["filters"]["award_ids"] = self.award_ids
            query["filters"].pop("tas_codes")
        return query

    def _search_award_type_page(self, page: int, award_type: AwardType) -> tuple[list[dict], bool]:
        """Get a list of awards for a given search page, and determine whether there are more pages.
        
        Args:
            page: see _award_search_filter
            
        Output:
            results: a list of dictionaries, where each dictionary represents one row in the awards table
            has_next: true if there are more pages to view    
        """
        # get the arguments to pass into the API
        kwargs = self._award_search_filter(page = page, award_type = award_type)
        print("Looking up awards for ", kwargs, SPENDING_BY_AWARD)

        # ask the API for the information
        retval = requests.post(SPENDING_BY_AWARD, json = kwargs)

        # raise an error if the lookup failed
        retval.raise_for_status()

        # the lookup succeeded. get the requested data out
        res = retval.json()

        # determine if there are more pages left
        has_next = res["page_metadata"]["hasNext"]

        # pull out the list of table rows
        results: list[dict] = res["results"]
        return results, has_next

    def _search_award_type(self, award_type: AwardType) -> dict:
        """Get a dict of awards for TAS codes and add them to the dict. If there are a lot of awards, the API won't let us ask for a huge table all at once. Instead, we have to break up that table into groups of 100, and ask it for results 1-100, then 101-200, then 201-300, etc. Each time we ask for those results, it will tell us if there are more pages of results.
            
        Output: a dictionary where the keys are long award IDs, and the values are dictionaries that represent rows in the awards table."""
        out = {}
        page = 1
        has_next = True
        while has_next:
            # get up to 100 rows from the API
            results, has_next = self._search_award_type_page(page, award_type = award_type)

            # add those rows to our running list
            for row in results:
                # store awards by their long ID, not their short ID (ASST_NON_7200... instead of 7200...). we'll need that long ID for downloading the data folder
                award_id = row["generated_internal_id"]
                out[award_id] = row

            page += 1
            if has_next:
                # wait 1 second before querying the API again, so we don't get locked out for making too many requests in a row
                time.sleep(1)
        return out
    
    def search_awards(self, overwrite: bool = False):
        """Find all of the awards for the TAS code and award type that we care about, and export the table of awards to a json.
        
        Args:
            overwrite: set this to true if you want to overwrite your existing table of awards.

        Output: exports a json, where the keys are the long award ID, and the values are a dictionary of summary data for that award.
        """
        file_name = self.award_json()
        if not overwrite and file_name.exists():
            return

        out = {}
        for award_type in self.award_types:
            # get the awards for each award type
            awards = self._search_award_type(award_type)

            # add the new awards to the dictionary
            out = {**out, **awards}

        print(f"Found {len(out)} awards.")

        # export all awards to a json
        with open(file_name, "w") as f:
            json.dump(out, f, indent = 4, sort_keys=True)

    # -- Downloading awards

    def pending_file(self, generated_award_id: str) -> Path:
        """The file that stores info about where to check the status of the pending download."""
        return self.pending_downloads_folder() / f"{generated_award_id}.json"
    
    def downloaded_award_folder(self, generated_award_id: str) -> Path:
        """The folder to export the downloaded data."""
        return self.downloads_folder() / generated_award_id
    
    def request_download(self, generated_award_id: str) -> bool:
        """Send the request to the API to download the data for a single award.
        
        Args:
            generated_award_id: The name of the award to request.
            
        Output: true if we have a status link to watch, false if something went wrong
        """
        # check if we already have a pending download for this award
        pending_file = self.pending_file(generated_award_id)
        if pending_file.exists():
            # we already have a pending download. don't need to make another request, just watch that download
            return True

        url = AWARD_DOWNLOAD
        kwargs = { "award_id": generated_award_id }

        # make the request
        response = requests.post(url, json = kwargs)

        # get the result from the request. if the request succeeded and USASpending is preparing the file, the response will have a status_url and a file_url in it. The status_url tells us where to look for the status of the download being prepared. file_url is where the file will be when it's ready to download.
        res = response.json()
        if "status_url" not in res:
            if "detail" in res:
                # something went wrong. print the message and quit
                print(res["detail"], kwargs)
            else:
                print(f"Something went wrong with the download request. Inputs: {kwargs}. Response: {res}")
            return False

        # save the status url and file url so we can check the status and download the file later
        with open(pending_file, "w") as f:
            json.dump(res, f)

        return True

    def downloaded_file(self, generated_award_id: str) -> Path:
        """The name of the file that tells us when we downloaded the data."""
        return self.downloaded_award_folder(generated_award_id) / "downloaded.txt"
    
    def downloaded_time(self, generated_award_id: str) -> datetime:
        """Get the time when the files were downloaded for this award."""
        filename = self.downloaded_file(generated_award_id)
        if not filename.exists():
            # no download time. assume one from the federalaccountfunding
            for file in self.downloaded_award_folder(generated_award_id).rglob("*FederalAccountFunding_1.csv"):
                # import the csv as a pandas dataframe
                df = pd.read_csv(file)

                # get the last date in the award_latest_action_date column
                last_funded: datetime = pd.to_datetime(df["award_latest_action_date"]).max()

                # save that to the download date file
                with open(filename, "w") as f:
                    f.write(last_funded.isoformat())

                return last_funded
            # we failed to find any files. return january 1, 1970
            return datetime(year = 1970, month = 1, day = 1)
        with open(filename) as f:
            return datetime.fromisoformat(f.read())

    def _download_files(self, file_url: str, generated_award_id: str) -> None:
        """Download the zip file from the file url, and save it to the award's download folder.
        
        Args:
            file_url: the url where usaspending is going to put the data to download
            generated_award_id: the long name of the award

        Output: downloads and unzips the folder to the data/downloads folder
        """
        # determine where to save the downloaded files
        award_folder = self.downloaded_award_folder(generated_award_id)

        # download the zip file
        res3 = requests.get(file_url)

        # read the zip file
        z = zipfile.ZipFile(io.BytesIO(res3.content))

        # extract all the files in the zip folder to the award folder
        z.extractall(award_folder)

        # drop a file into the award folder noting down when we downloaded it
        with open(self.downloaded_file(generated_award_id), "w") as f:
            now = datetime.now()
            f.write(now.isoformat())

        # print status update
        print(f"Data extracted to {award_folder}")

    def check_download_status(self, generated_award_id: str) -> bool:
        """Check if the download that we previously requested is ready, and download it if it is ready.
        
        Args:
            generated_award_id: the name of the award we want to download
            
        Output: true if the result is still pending."""

        # import the file that tells us where to request the status
        pending_file = self.pending_file(generated_award_id)
        with open(pending_file) as f:
            request_dict = json.load(f)

        if "status_url" in request_dict and "file_url" in request_dict:
            # check the status url
            status_dict: dict = requests.get(request_dict["status_url"]).json()
            status = status_dict.get("status", "unknown")
            print(f"{generated_award_id}    {status}, {status_dict.get('seconds_elapsed', 'unknown')} seconds elapsed")
            if status == "finished":
                self._download_files(request_dict["file_url"], generated_award_id)
                pending_file.unlink()  # delete the pending file so we know we already downloaded this data
                return False
            elif status in ["running", "ready"]:
                # keep the pending file so we know we should still check it
                return True
            else:
                print(status_dict)
                pending_file.unlink()  # delete the pending file so we know there was an error with this download, and we need to file a new request
                return False
        else:
            # the pending file is missing information. delete the file so we'll have to make another request to the API instead of trying to use this one again. this should not happen
            print(f"Something is wrong with the pending download file. Try downloading {generated_award_id} again.")
            pending_file.unlink()
            return False

    def download_award_data(self, generated_award_id: str, overwrite: bool = False, tries: int = 10) -> bool:
        """Download the data zip folder for a single award. This zip folder contains transaction-level information for all time for the award.
        
        Args:
            generated_award_id: the long internal award id (ASST_NON_7200... instead of 7200...)
            overwrite: true to overwrite any existing data we've already downloaded
            tries: how many times we should check the download status before we move onto another award and come back

        Output: saves a folder of csvs to data/downloads, if the download is ready. returns true if there is still a download pending        
        """
        # check if we've already downloaded this data
        award_folder = self.downloaded_award_folder(generated_award_id)
        if not overwrite and award_folder.exists():
            if self.critical_download_date is not None:
                # overwrite if this file was downloaded before the critical download date
                download_date = self.downloaded_time(generated_award_id)
                if download_date >= self.critical_download_date:
                    return False
            else:
                # no critical download date, don't overwrite
                return False
        
        # check if we've already requested this data
        request_succeeded = self.request_download(generated_award_id)
        if not request_succeeded:
            return False
        
        attempt = 0
        while attempt < tries:
            pending = self.check_download_status(generated_award_id)
            if pending:
                # there is still a download pending. wait 1 second and ask again
                attempt += 1
                time.sleep(1)
            else:
                return pending
        return pending

    def download_awards(self, stop_on_errors: bool = True) -> None:
        """Download the award data zip for all of the awards in the awards json, and extract it to a data folder.
        
        Args:
            stop_on_errors:
                true: if we hit an error on any award, abort
                false: if we hit an error on any award, move onto the next award

        Output: saves a bunch of files to the data folder
        """
        new_pending_list = self.generated_award_ids() 
        orig_num_rows = len(new_pending_list)

        # iterate through awards and try to download all the awards
        while len(new_pending_list) > 0:
            i = 0
            num_rows = len(new_pending_list)
            # reset the list of pending awards
            pending_list = new_pending_list
            new_pending_list = []
            for award_id in pending_list:
                try:
                    print(f"{i + 1} / {num_rows} ({orig_num_rows}) {award_id}")
                    award_pending = self.download_award_data(award_id)
                    if award_pending:
                        # if the award still has a download pending, put it back in the list of pending awards
                        new_pending_list.append(award_id)
                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except Exception as e:
                    print(e)
                    if stop_on_errors:
                        raise e
                    else:
                        pass
                i += 1

    def generated_award_ids(self) -> list[str]:
        """A list of all of the long award IDs that we're looking for, saved in our award json."""
        with open(self.award_json()) as f:
            award_dict: dict = json.load(f)
        return list(award_dict.keys())

    # -- Combining awards

    def combine_tag_awards(self, tag: str, overwrite: bool):
        """Combine all files for a specific tag into one file."""
        downloads_folder = self.downloads_folder()
        # get the file name to export to
        file_name = self.combined_csv(tag)
        if file_name.exists() and not overwrite:
            return
        print(f"Combining files for {tag}")

        # find all csvs in the award folder with the given tag
        df_list = []

        # look specifically for the requested awards
        for generated_award_id in self.generated_award_ids():
            award_folder = downloads_folder / generated_award_id
            for file in award_folder.rglob(f"*{tag}_1.csv"):
                # import the csv as a pandas dataframe and filter it to only rows with this tas
                df = pd.read_csv(file)
                if tag == "FederalAccountFunding":
                    # in the FederalAccountFunding file, the tas code is stored under the treasury_account_symbol column
                    filtered = df[df["treasury_account_symbol"] == self.tas_code]
                elif tag == "TransactionHistory":
                    # in the TransactionHistory file, all relevant tas codes are combined into the column treasury_accounts_funding_this_award
                    filtered = df[df["treasury_accounts_funding_this_award"].str.contains(self.tas_code)]
                elif tag == "Sub-Awards":
                    # in the TransactionHistory file, all relevant tas codes are combined into the column prime_award_treasury_accounts_funding_this_award
                    filtered = df[df["prime_award_treasury_accounts_funding_this_award"].str.contains(self.tas_code)]

                # add the pandas dataframe to the list of dataframes
                df_list.append(filtered)

        if len(df_list) > 0:
            # combine all dataframes into one big dataframe
            combined = pd.concat(df_list)

            # remove duplicate rows
            combined.drop_duplicates(inplace=True)

            # export to csv
            combined.to_csv(file_name, index = False)


    def combine_awards(self, overwrite: bool = False):
        """For each tag, combine all of the data from the data folders we downloaded, filter down to just the TAS codes we care about, and export the combined data to a csv."""
        downloads_folder = self.downloads_folder()
        if not downloads_folder.exists():
            raise FileNotFoundError("No downloaded files found.")

        # iterate through FederalAccountFunding, TransactionHistory, and Sub-Awards
        for tag in self.valid_file_tags:
            self.combine_tag_awards(tag, overwrite)

if __name__ == "__main__":
    awards = AwardSearchDownload(usaid_tas(2024), award_ids = ["72061521CA00007"], summary_name = "72061521CA00007")
    awards.search_awards()
    awards.download_awards()
    awards.combine_awards()