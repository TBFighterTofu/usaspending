from pathlib import Path
import pandas as pd
from enum import Enum, auto

DATA_FOLDER = Path(__file__).parent.parent / "data"
SPENDING_BY_AWARD = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
SPENDING_OVER_TIME = "https://api.usaspending.gov/api/v2/search/spending_over_time/"
SPENDING_BY_TRANSACTION = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
AWARD_DOWNLOAD = "https://api.usaspending.gov/api/v2/download/contract"

class AwardType(Enum):
    CONTRACT = auto()
    IDV = auto()
    LOAN = auto()
    GRANT = auto()
    DIRECT_PAYMENTS = auto()
    OTHER = auto()
    ALL = auto()

class USASpending:
    
    def _tas(self, year: int) -> str:
        return f"072-019-{year}/{year+1}-1031-000"
    
    def category_folder(self):
        return DATA_FOLDER / "categories"

    def kff_csv(self):
        return self.category_folder() / "award_categories.csv"
    
    # -- Private methods

    def _health_categories(self):
        return ["Health?","FPRH","HIV-AIDS","Health - General","Malaria","MCH","Nutrition","Other Public Health Threats","PIOET","TB"]

    def _blank_categories(self) -> dict:
        return {key: False for key in self._health_categories()}

    def _load_kff_categories(self) -> dict[str, dict]:
        file = self.kff_csv()
        df = pd.read_csv(file)
        out = {}
        for _, row in df.iterrows():
            newrow = row.to_dict()
            newrow["categories"] = []
            newrow["category"] = ""
            for key in self._health_categories():
                if row[key]:
                    newrow["categories"].append(key)
                    newrow["category"] = key
            newrow["categories"] = ", ".join(newrow["categories"])
            out[newrow["Award ID"]] = newrow
        return out
    
    def _guess_category(self, description: str, newrow: dict | None = None) -> dict:
        """Guess the categories based on transaction descriptions."""
        if newrow is None:
            newrow = self._blank_categories()
        val = str(description).lower()
        if "tuberculosis" in val:
            newrow["TB"] = True
        if "malaria" in val:
            newrow["Malaria"] = True
        if "hiv" in val:
            newrow["HIV-AIDS"] = True
        if "nutrition" in val:
            newrow["Nutrition"] = True
        if "health" in val:
            newrow["Health?"] = True
        if "maternal" in val:
            newrow["MCH"] = True
        if "reprod" in val:
            newrow["FPRH"] = True
        return newrow


