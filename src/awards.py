import json
from pathlib import Path
import pandas as pd
import time
import zipfile
import io
import requests
from datetime import datetime

from .usa_types import AwardType, DATA_FOLDER, SPENDING_BY_AWARD, AWARD_DOWNLOAD, award_type_codes, program_activity_codes


class AwardSearchDownload:
    """Finds the list of awards for a specific TAS code, downloads the zipfiles for all of the awards, and combines those zip files into a single CSV.
    
    For example, if you want to find program activity and all awards for 2024/2025 USAID, download their data, combine the transations for that TAS into one csv file, and compare the results from program activity and individual transations, run this:
        awards = USASpendingAwards("072-019-2024/2025-1031-000")
        awards.run_all()
    
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
            self.summary_name = summary_name.replace(r"/", "_")
        else:
            self.summary_name = self.tas_code.replace(r"/", "_")

        if critical_download_date is not None:
            self.critical_download_date = critical_download_date
        else:
            self.critical_download_date = None
        self.create_folders()

    def run_all(self):
        print(f"Downloading all for {self.summary_name}")
        self.download_program_activity()  # download the program activity summary
        self.search_awards()              # download the list of awards
        self.download_awards()            # download data for each award
        self.combine_awards()             # combine data into one spreadsheet
        self.check_summaries()            # check that totals make sense

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

    def program_activity_file(self):
        return self.summary_folder() / f"program_activity_{self.summary_name}.json"
    
    def summary_check_file(self):
        return self.summary_folder() / f"summary_check_{self.summary_name}.txt"
    
    def summary_downloaded_file(self):
        return self.summary_folder() / "downloaded.json"

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
    
    def check_overwrite(self, filename: Path) -> bool:
        """Get the time when the requested file was downloaded, and return true if we should overwrite the file.
        
        Args:
            filename: the name of the exported file
        
        """
        if not filename.exists():
            # file doesn't exist. overwrite it
            return True

        time_file = self.summary_downloaded_file()
        if not time_file.exists():
            # we didn't write down when we made any file. overwrite it.
            return True
        if self.critical_download_date is None:
            # we have no critical download date. don't overwrite it
            return False
        with open(time_file) as f:
            time_dict = json.load(f)
        file_str = filename.name
        if file_str not in time_dict:
            # we didn't write down when we made the file. overwrite it
            return True
        downloaded = datetime.fromisoformat(time_dict[file_str])
        if downloaded >= self.critical_download_date:
            # we downloaded this after the critical download date. don't overwrite it
            return False
        else:
            # we downloaded this before the critical download date. overwrite it.
            return True
    
    def export_downloaded_time(self, file: Path):
        """Export the current time as the time when we downloaded this file."""
        time_file = self.summary_downloaded_file()
        if not time_file.exists():
            time_dict = {}
        else:
            with open(time_file) as f:
                time_dict = json.load(f)
        time_dict[file.name] = datetime.now().isoformat()
        with open(time_file, "w") as f:
            json.dump(time_dict, f, indent = 4)

    # -- Downloading summary

    def fiscal_year_range(self):
        return range(2017, datetime.now().year + 1)

    def download_program_activity(self):
        """Download the summary of program activity and export it as a json."""
        # don't overwrite if we already downloaded it
        filename = self.program_activity_file()
        overwrite = self.check_overwrite(filename)
        if not overwrite:
            return

        out = {}
        for fiscal_year in self.fiscal_year_range():
            url = f"https://api.usaspending.gov/api/v2/agency/treasury_account/{self.tas_code}/program_activity?fiscal_year={fiscal_year}"
            response = requests.get(url).json()
            out[fiscal_year] = response.get("results", {})

        # export the results and save the time when we exported them
        with open(filename, "w") as f:
            json.dump(out, f, indent = 4)
        self.export_downloaded_time(filename)

    # -- Searching for awards
     
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
                "award_type_codes": award_type_codes(award_type)
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
        print(f"Looking up awards for {award_type.name}, page {page}")

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
    
    def search_awards(self):
        """Find all of the awards for the TAS code and award type that we care about, and export the table of awards to a json.
        
        Args:
            overwrite: set this to true if you want to overwrite your existing table of awards.

        Output: exports a json, where the keys are the long award ID, and the values are a dictionary of summary data for that award.
        """
        file_name = self.award_json()
        # check if we already made this summary
        overwrite = self.check_overwrite(file_name)
        if not overwrite:
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
        self.export_downloaded_time(file_name)

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
            json.dump(res, f, indent = 4)

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
            if status == "ready":
                print(f"{generated_award_id}    Queued")
            else:
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
        
        # wait 5 seconds for the download to start
        time.sleep(5)
        
        attempt = 0
        while attempt < tries:
            pending = self.check_download_status(generated_award_id)
            if pending:
                # there is still a download pending. wait and ask again
                attempt += 1
                time.sleep(5)
            else:
                return pending
        return pending

    def download_awards_chunk(self, awards: list[str], orig_num_rows: int, stop_on_errors: bool, starting_num: int) -> None:
        # iterate through awards and try to download all the awards
        new_pending_list = awards.copy()
        while len(new_pending_list) > 0:
            i = 0
            # reset the list of pending awards
            pending_list = new_pending_list
            new_pending_list = []
            for award_id in pending_list:
                try:
                    print(f"{starting_num + i + 1} / {orig_num_rows} {award_id}")
                    award_pending = self.download_award_data(award_id)
                    if award_pending:
                        # if the award still has a download pending, put it back in the list of pending awards
                        new_pending_list.append(award_id)
                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except Exception as e:
                    if "Connection aborted" in str(e):
                        print("The server aborted the connection because we've made too many requests. Waiting 5 minutes...")
                        new_pending_list.append(award_id)
                        time.sleep(60*5)
                    elif "Max retries exceeded with url" in str(e):
                        print("Connection problem. Waiting 5 minutes...")
                        new_pending_list.append(award_id)
                        time.sleep(60*5)
                    else:
                        print(e)
                        if stop_on_errors:
                            raise e
                        else:
                            pass
                i += 1

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

        
        chunk_size = 10
        j0 = 0
        jf = j0 + chunk_size

        while j0 <= len(new_pending_list):
            print(f"Downloading award {j0} to {jf}")
            self.download_awards_chunk(
                awards = new_pending_list[j0:min(len(new_pending_list),jf)],
                orig_num_rows = orig_num_rows,
                stop_on_errors = stop_on_errors,
                starting_num = j0
            )
            j0 = j0 + chunk_size
            jf = jf + chunk_size

    def generated_award_ids(self) -> list[str]:
        """A list of all of the long award IDs that we're looking for, saved in our award json."""
        file = self.award_json()
        if not file.exists():
            return []
        with open(self.award_json()) as f:
            award_dict: dict = json.load(f)
        return list(award_dict.keys())

    # -- Combining awards

    def _calculate_outlays_for_indices(self, df: pd.DataFrame, class_indices: pd.Series) -> None:
        """For a given list of indices, calculate the per-period outlays from the gross outlays.
        
        Args:
            df: the dataframe for all years and categories
            class_indices: a true/false list that indicates which rows are grouped together

        Output: updates the dataframe in the specified rows
        
        """
        gross_outlays = df.loc[class_indices, "gross_outlay_amount_FYB_to_period_end"]   
            # get the cumulative outlays
        df.loc[class_indices, "transaction_outlay_amount"] = gross_outlays.diff().fillna(gross_outlays)   
            # calculate as this row - prev row, or this row if there is no previous row

    def _check_class_codes(self, df: pd.DataFrame, indices: pd.Series, fy: int) -> list[float]:
        class_codes = df.loc[indices, "object_class_code"].unique()
        if 0 in class_codes and len(class_codes) > 1:
            # there are unknown classes. determine if they should be combined with the next class or not
            if len(df.loc[indices & (df["submission_period"]==f"FY{fy}P12") & (df["object_class_code"]==0)]) == 0:
                # there is no 12th period marked, which means this unknown category was combined with other categories in the last period. combine all categories as the same time series
                class_codes = [code for code in class_codes if code > 0]
                replacement_code = class_codes[0]
                for period in range(1, 13):
                    period_indices = (df["submission_period"]==f"FY{fy}P{period}")
                    unknown_gross = df.loc[indices & period_indices & (df["object_class_code"]==0), "gross_outlay_amount_FYB_to_period_end"].sum()
                    catchers = indices & period_indices & (df["object_class_code"]==replacement_code)
                    df.loc[catchers, "gross_outlay_amount_FYB_to_period_end"] += unknown_gross
                return class_codes
            else:
                # the unknowns have their own 12th period. treat them as their own thing
                return class_codes
        else:
            # no unknowns. proceed as normal.
            return class_codes

    def _calculate_outlays_for_fiscal_year(self, df: pd.DataFrame, fy: int, outlays: pd.Series) -> None:
        """For a given fiscal year, break up the rows by category and calculate the per-period outlays from the gross outlays.
        
        Args:
            df: the dataframe for all years, categories in the TAS code
            fy: fiscal year
            outlays: a True/False list that indicates which rows we're looking at

        Output: nothing. we're passing around a pointer to a dataframe, so the changes will be applied without us returning it
        """
        indices = (df["fiscal_year"] == fy)&outlays
        if len(df[indices]) == 0:
            return
        class_codes = self._check_class_codes(df, indices, fy)
                
        for class_code in class_codes:
            class_indices = indices & (df["object_class_code"] == class_code)
            self._calculate_outlays_for_indices(df, class_indices)

    def _import_award_federal_account_funding(self, file: Path) -> pd.DataFrame:
        """Given a FederalAccountFunding.csv file, import the table, add useful columns, and return a dataframe."""
        df = pd.read_csv(file)
        if len(df) == 0:
            return df
        pac = program_activity_codes()
        # in the FederalAccountFunding file, the tas code is stored under the treasury_account_symbol column
        df = df[df["treasury_account_symbol"] == self.tas_code]
        df.sort_values(by="submission_period", inplace=True)
        # add columns for fiscal year (2024), fiscal period (P3), program activity code (1,2,3,4), program activity title (Grants and fixed charges...)
        df["fiscal_year"] = [per[2:6] for per in df["submission_period"]]
        df["fiscal_period"] = [per[6:] for per in df["submission_period"]]
        df["pa_code"] = [int(s//10) for s in df["object_class_code"]]
        df["pa_title"] = [pac[s] for s in df["pa_code"]]
        
        df.sort_values(by = "submission_period", inplace = True)
        # calculate outlay amounts. gross_outlay is cumulative per fiscal year and category
        outlays = (~pd.isna(df["gross_outlay_amount_FYB_to_period_end"]))&(df["gross_outlay_amount_FYB_to_period_end"]!=0)
        for fy in df.loc[outlays, "fiscal_year"].unique():
            self._calculate_outlays_for_fiscal_year(df, fy, outlays)
        return df

    def _import_award_transaction_history(self, file: Path) -> pd.DataFrame:
        """In the TransactionHistory file, all relevant tas codes are combined into the column treasury_accounts_funding_this_award"""
        df = pd.read_csv(file)
        if len(df) == 0:
            return df
        tas_list = df["treasury_accounts_funding_this_award"].fillna("")
        filtered = df[tas_list.str.contains(self.tas_code)]
        return filtered
    
    def _import_award_subawards(self, file: Path) -> pd.DataFrame:
        # in the TransactionHistory file, all relevant tas codes are combined into the column prime_award_treasury_accounts_funding_this_award
        df = pd.read_csv(file)
        if len(df) == 0:
            return df
        tas_list = df["prime_award_treasury_accounts_funding_this_award"].fillna("")
        filtered = df[tas_list.str.contains(self.tas_code)]
        return filtered

    def combine_tag_awards(self, tag: str, overwrite: bool = False):
        """Combine all files for a specific tag into one file."""
        downloads_folder = self.downloads_folder()
        # get the file name to export to

        file_name = self.combined_csv(tag)
        if not overwrite:
            overwrite = self.check_overwrite(file_name)
            if not overwrite:
                return
        print(f"Combining files for {tag}")

        # find all csvs in the award folder with the given tag
        df_list = []
        awards = self.generated_award_ids()
        if len(awards) == 0:
            # no awards found. don't export this summary yet.
            return

        # look specifically for the requested awards
        for generated_award_id in awards:
            award_folder = downloads_folder / generated_award_id
            for file in award_folder.rglob(f"*{tag}_1.csv"):
                # import the csv as a pandas dataframe and filter it to only rows with this tas
                if tag == "FederalAccountFunding":
                    df = self._import_award_federal_account_funding(file)
                elif tag == "TransactionHistory":
                    df = self._import_award_transaction_history(file)
                elif tag == "Sub-Awards":
                    df = self._import_award_subawards(file)
                # add the pandas dataframe to the list of dataframes
                if len(df) > 0:
                    df_list.append(df)

        if len(df_list) > 0:
            # combine all dataframes into one big dataframe
            combined: pd.DataFrame = pd.concat(df_list)

            # remove duplicate rows
            combined.drop_duplicates(inplace=True)

            # export to csv
            combined.to_csv(file_name, index = False)

            self.export_downloaded_time(file_name)

    def combine_awards(self):
        """For each tag, combine all of the data from the data folders we downloaded, filter down to just the TAS codes we care about, and export the combined data to a csv."""
        downloads_folder = self.downloads_folder()
        if not downloads_folder.exists():
            raise FileNotFoundError("No downloaded files found.")

        # iterate through FederalAccountFunding, TransactionHistory, and Sub-Awards
        for tag in self.valid_file_tags:
            self.combine_tag_awards(tag)
 
    # -- Checking sums

    def import_program_activity(self) -> dict:
        """Import the program activity summary as a dictionary."""
        paf = self.program_activity_file()
        if paf.exists():
            with open(paf) as f:
                return json.load(f)
        else:
            return {}

    def import_federal_account_funding_df(self) -> pd.DataFrame | None:
        """Import the FederalAccountFunding csv as a pandas dataframe."""
        faf_file = self.combined_csv("FederalAccountFunding")
        if faf_file.exists():
            df = pd.read_csv(faf_file)
            return df
        else:
            return None

    def _make_tabbed_line(self, title: str, pa: float, faf: float):
        diff = pa - faf
        if pa != 0:
            diff_pct = str(int(diff / pa * 100)).rjust(2)
        else:
            diff_pct = "--"
        return "".join([f"    {title}:".ljust(18), " PA:", f" ${pa:,}".rjust(15)," / FA: ", f"${faf:,}".rjust(15), " / Missing:", f" ${(diff):,}".rjust(15), f" ({diff_pct}%)" ])

    def _compare_pa_to_faf(self, activity: dict, funding: pd.DataFrame, title: str) -> list[str]:
        """Compare a program activity year to a FederalAccountFunding year, and return a list of strings to print out or write to file."""
        lines = []
        if len(activity) == 0 and len(funding) == 0:
            return lines
        pa_obligated = int(activity.get("obligated_amount", 0))
        pa_gross_outlay = int(activity.get("gross_outlay_amount", 0))
        faf_obligated = int(funding["transaction_obligated_amount"].sum())
        faf_gross_outlay = int(funding["transaction_outlay_amount"].sum())
        lines.append(f"  {title}")
        lines.append(self._make_tabbed_line("Obligated", pa_obligated, faf_obligated))
        lines.append(self._make_tabbed_line("Gross Outlay", pa_gross_outlay, faf_gross_outlay))
        return lines

    def _find_child(self, activity: dict, child: str):
        children = activity.get("children", [])
        for chil in children:
            if chil["name"] == child:
                return chil
        return {}

    def _export_summary_lines(self, lines: list[str]):
        """Export lines of text to the summary check file."""
        filename = self.summary_check_file()
        output = "\n".join(lines)
        with open(filename, "w") as f:
            f.write(output)
        self.export_downloaded_time(filename)

    def check_summaries(self, overwrite: bool = False):
        """Compare the downloaded program activity summary to the downloaded federal account funding table, and export the result to a text file."""
        # check if we should overwrite
        filename = self.summary_check_file()
        if not overwrite:
            overwrite = self.check_overwrite(filename)
            if not overwrite:
                return
        
        lines = [f"TAS code: {self.tas_code}"]
        # import the program activity file and the federal account funding summary
        program_activity = self.import_program_activity()
        df = self.import_federal_account_funding_df()

        # don't export anything if we're missing FAF or program activity
        if len(program_activity) == 0 or df is None:
            return

        # iterate through fiscal years and sum data
        for fiscal_year in self.fiscal_year_range():
            # get activity summary and FAF rows for this fiscal year
            activity_list: list[dict] = program_activity.get(str(fiscal_year))
            funding = df[df["fiscal_year"] == fiscal_year]

            if len(activity_list) > 0 or len(funding) > 0:
                if len(activity_list) > 0:
                    activity = activity_list[0]
                else:
                    activity = {}

                # compare the totals
                lines.append(f"\nFY{fiscal_year}")
                lines = lines + self._compare_pa_to_faf(activity=activity, funding = funding, title = "Total")

                # compare the subgroups
                for code, child in program_activity_codes().items():
                    subdf = funding[funding["pa_code"]==code]
                    subchil = self._find_child(activity, child)
                    if len(subdf) > 0 or len(subchil) > 0:
                        lines = lines + self._compare_pa_to_faf(subchil, subdf, title = f"{code}X: {child}")

        # write the result to file
        self._export_summary_lines(lines)
