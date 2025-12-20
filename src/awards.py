import json
from pathlib import Path
import pandas as pd
import time
import zipfile
import io
import requests

from usaspending import USASpending, AwardType, DATA_FOLDER, SPENDING_BY_AWARD, AWARD_DOWNLOAD

class USASpendingAwards(USASpending):

    def __init__(self, award_type: AwardType, min_year: int = 2024, max_year: int = 2025):
        self.min_year = min_year
        self.max_year = max_year
        self.award_type = award_type
        summary_folder = self.summary_folder()
        download_folder = self.downloads_folder()
        for folder in [summary_folder, download_folder]:
            if not folder.exists():
                folder.mkdir()

    # -- File names

    def summary_folder(self) -> Path:
        return DATA_FOLDER / f"{self.min_year}_{self.max_year}"

    def award_csv(self):
        """File to save the list of awards."""
        return self.summary_folder() / f"awards_{self.award_type.name}_{self.min_year}_{self.max_year}.csv"
    
    def downloads_folder(self):
        """Folder that holds the raw downloaded data."""
        return DATA_FOLDER / self.award_type.name
    
    def combined_csv(self, tag: str):
        """CSV that combines all the raw data into one file."""
        return self.summary_folder() / f"combined_{self.award_type.name}_{tag}_{self.min_year}_{self.max_year}.csv"
    
    # -- Scrapers
    
    def search_awards(self):
        """Find all of the awards for the TAS codes and award type that we care about, and export the result to a json."""
        out = {}
        file_name = self.award_csv()
        if file_name.exists():
            return
        for year in range(self.min_year, self.max_year):
            out = self._search_award_year(year, out)
            time.sleep(1)
        with open(file_name, "w") as f:
            json.dump(out, f, indent = 4, sort_keys=True)

    def download_awards(self):
        """Download the award data zip for all of the awards in the award list, and extract it to a data folder."""
        with open(self.award_csv()) as f:
            d = f.readlines()
        num_rows = len(d)
        d = [row.replace("\n", "") for row in d]
        i = 0
        for award_id in d:
            try:
                print(f"{i + 1} / {num_rows} {award_id}")
                self._download_award_data(award_id)
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except Exception as e:
                print(e)
                pass
            i += 1

    def combine_awards(self, overwrite: bool = False):
        """For each tag, combine all of the data from the data folders we downloaded, filter down to just the TAS codes we care about, and export the combined data to a csv."""
        award_folder = self.downloads_folder()
        if not award_folder.exists():
            award_folder.mkdir()
        tas_list = [self._tas(year) for year in range(self.min_year, self.max_year)]
        print(tas_list)
        for tag in ["FederalAccountFunding", "TransactionHistory", "Sub-Awards"]:
            file_name = self.combined_csv(tag)
            if file_name.exists() and not overwrite:
                return
            print(f"Combining files for {tag}")
            df_list = []
            for file in award_folder.rglob(f"*{tag}_1.csv"):
                df = pd.read_csv(file)
                if tag == "FederalAccountFunding":
                    filtered = [df[df["treasury_account_symbol"] == tas_i] for tas_i in tas_list]
                elif tag == "TransactionHistory":
                    filtered = [df[df["treasury_accounts_funding_this_award"].str.contains(tas_i)] for tas_i in tas_list]
                elif tag == "Sub-Awards":
                    filtered = [df[df["prime_award_treasury_accounts_funding_this_award"].str.contains(tas_i)] for tas_i in tas_list]
                df_list = df_list + filtered
            if len(df_list) > 0:
                combined = pd.concat(df_list)
                combined.drop_duplicates(inplace=True)
                combined.to_csv(file_name, index = False)

    def run_all(self):
        self.search_awards()
        self.download_awards()
        self.combine_awards()

    # -- Private methods

    def _tas_filter(self, year: int) -> dict:
        return  {"require": [[self._tas(year)]]}
    
    def _award_type_codes(self) -> list[str]:
        """Get the eligible award type codes for the given award type."""
        if self.award_type == AwardType.CONTRACT:
            return ["A", "B", "C", "D"]
        elif self.award_type == AwardType.IDV:
            return ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
        elif self.award_type == AwardType.LOAN:
            return ["07", "08"]
        elif self.award_type == AwardType.GRANT:
            return ["02", "03", "04", "05"]
        elif self.award_type == AwardType.DIRECT_PAYMENTS:
            return ["06", "10"]
        elif self.award_type == AwardType.OTHER:
            return ["09", "11", "-1"]
        else:
            return []
        
    def _award_search_fields(self) -> list[str]:
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
        if self.award_type == AwardType.CONTRACT:
            fields = ["Start Date", "End Date", "Award Amount", "Total Outlays", "Contract Award Type"]
        elif self.award_type == AwardType.IDV:
            fields = ["Start Date", "Award Amount", "Total Outlays", "Contract Award Type", "Last Date to Order", "NAICS", "PSC"]
        elif self.award_type == AwardType.LOAN:
            fields = ["Issued Date", "Loan Value", "Subsidy Cost", "SAI Number", "CFDA Number", "Assistance Listings", "primary_assistance_listing"]
        elif self.award_type in [AwardType.GRANT, AwardType.DIRECT_PAYMENTS, AwardType.OTHER]:
            fields =  ["Start Date", "End Date", "Award Amount", "Total Outlays", "Award Type", "SAI Number", "CFDA Number", "Assistance Listings", "primary_assistance_listing"]
        else:
            fields = []
        return base_fields + fields

    def _award_search_filter(self, year: int, page: int = 1) -> dict:
        """Filter to apply to the awards search."""
        return {
            "filters": {
                "tas_codes": self._tas_filter(year),
                "award_type_codes": self._award_type_codes()
            },
            "fields": self._award_search_fields(),
            "limit": 100, 
            "page": page  
        }

    def _search_award_year_page(self, year: int, page: int) -> tuple[list[dict], bool]:
        """Get a list of awards for a given year and search page, and determine whether there are more pages."""
        kwargs = self._award_search_filter(year = year, page = page)
        print("Looking up awards for ", kwargs)
        res = requests.post(SPENDING_BY_AWARD, json = kwargs).json()
        has_next = res["page_metadata"]["hasNext"]
        results: list[dict] = res["results"]
        return results, has_next

    def _search_award_year(self, year: int, out: dict) -> dict:
        """Get a dict of awards for TAS codes from the given year and add them to the dict."""
        page = 1
        has_next = True
        while has_next:
            results, has_next = self._search_award_year_page(year, page)
            for row in results:
                award_id = row["generated_internal_id"]
                out[award_id] = row
            page += 1
            if has_next:
                time.sleep(1)
        return out

    def _download_award_data(self, generated_award_id: str):
        """Download the data zip folder for a single award, using the long internal award id (ASST_NON_7200... instead of 7200...)."""
        award_folder = self.downloads_folder() / generated_award_id
        if award_folder.exists():
            return
        
        pending_file = self.downloads_folder() / f"pending_{generated_award_id}.json"
        
        if pending_file.exists():
            with open(pending_file) as f:
                res2 = json.load(f)
            file_url = res2["file_url"]
        else:
            url = AWARD_DOWNLOAD
            kwargs = { "award_id": generated_award_id }
            res = {}

            # wait for the request to go through
            while "status_url" not in res:
                response = requests.post(url, json = kwargs)
                res = response.json()
                if "status_url" not in res:
                    if "detail" in res:
                        print(res["detail"], kwargs)
                        return
                    print("waiting for status...", res)
                    time.sleep(1)

            # wait for the download to be ready
            status_url = res["status_url"]
            file_url = res["file_url"]
            print(file_url)
            status_str = "running"
            while status_str == "running":
                res2 = requests.get(status_url).json()
                if "status" in res2:
                    status_str = res2["status"]
                print(f"    {status_str}, {res2['seconds_elapsed']} seconds elapsed")
                if status_str == "running":
                    time.sleep(1)

        # download the file
        if status_str == "finished":
            res3 = requests.get(file_url)
            z = zipfile.ZipFile(io.BytesIO(res3.content))
            z.extractall(award_folder)
            print(f"Data extracted to {award_folder}")
        else:
            if not pending_file.exists():
                with open(pending_file, "w") as f:
                    json.dump(res2, f)

if __name__ == "__main__":
    awards = USASpendingAwards(AwardType.ALL, 2023, 2024)
    # awards.download_awards()
    awards.combine_awards(overwrite = True)