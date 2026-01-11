from pathlib import Path
import pandas as pd
import time
import requests

from usa_types import DATA_FOLDER, SPENDING_BY_TRANSACTION, usaid_tas

class USASpendingTransactions:
    
    def __init__(self, min_year: int, max_year: int):
        self.min_year = min_year
        self.max_year = max_year
        summary_folder = self.summary_folder()
        download_folder = self.downloads_folder()
        for folder in [summary_folder, download_folder]:
            if not folder.exists():
                folder.mkdir()

    # -- File names

    def summary_folder(self) -> Path:
        return DATA_FOLDER / f"{self.min_year}_{self.max_year}"

    def downloads_folder(self) -> Path:
        return DATA_FOLDER / "transactions"
    
    def combined_csv(self) -> Path:
        return self.summary_folder() / f"combined_transactions_{self.min_year}_{self.max_year}.csv"

    # -- Scrapers

    def search_transactions(self):
        """Look up all of the transactions for the given time range and TAS."""
        out = {}
        for year in range(self.min_year, self.max_year):
            page = 1
            has_next = True
            while has_next:
                print(year, "page",  page)
                has_next, rows = self._year_page_transactions(year, page)
                page += 1
                for row in rows:
                    out[row["internal_id"]] = row
            time.sleep(1)
        df = pd.DataFrame(out.values())
        df.sort_values(by = "Action Date", inplace = True)
        df.to_csv(self.combined_csv(), index = False)

    def combine_transactions(self):
        """Combine all of the transactions in the data folder."""
        transaction_folder = self.downloads_folder()
        out = {}
        for file in transaction_folder.rglob("*.csv"):
            print(file)
            df = pd.read_csv(file, index_col = None)
            for i, row in df.iterrows():
                out[row["internal_id"]] = row
        df = pd.DataFrame(out.values())
        df.sort_values(by = "Action Date", inplace = True)
        df.to_csv(self.combined_csv(), index=False)

    def run_all(self):
        self.search_transactions()
        self.combine_transactions()

    # -- Private methods

    def _transaction_params(self, year: int, page: int) -> dict:
        return {
            "filters": {
                "tas_codes": {"require": [[usaid_tas(year)]]},
                "award_type_codes": ["A", "B", "C", "D", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
            },
            "fields": ["internal_id", "Action Date", "Action Type", "Assistance Listing", "Award ID", "Award Type", "Awarding Agency", "Awarding Sub Agency", "Funding Agency", "Funding Sub Agency", "Issued Date", "Last Date to Order", "Loan Value", "Mod", "NAICS", "PSC", "Primary Place of Performance", "Recipient Location", "Recipient Name", "Recipient UEI", "Subsidy Cost", "Transaction Amount", "Transaction Description"],
            "limit": 100,
            "page": page,
            "sort": "Action Date",
            "order": "desc"
        }
        
    def _year_page_transactions(self, year: int, page: int):
        url = SPENDING_BY_TRANSACTION
        kwargs = self._transaction_params(year, page)
        res = requests.post(url, json = kwargs).json()
        out = []
        for row in res["results"]:
            assistance_listing = row.pop("Assistance Listing")
            row["cfda_number"] = assistance_listing["cfda_number"]
            row["cfda_title"] = assistance_listing["cfda_title"]
            naics = row.pop("NAICS")
            row["NAICS code"] = naics["code"]
            row["NAICS description"] = naics["description"]
            psc = row.pop("PSC")
            row["PSC code"] = psc["code"]
            row["PSC description"] = psc["description"]
            row["country_name"] = row.pop("Primary Place of Performance")["country_name"]
            row["recipient_country"] = row.pop("Recipient Location")["country_name"]
            out.append(row)
        
        pd.DataFrame(out).to_csv(self.downloads_folder() / f"{year}_p_{page}.csv", index = False)
        return res["page_metadata"]["hasNext"], out
