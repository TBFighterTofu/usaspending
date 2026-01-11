import pandas as pd

from usa_types import DATA_FOLDER

class SpendingCategories:
    """Class for categorizing awards."""

    def __init__(self, tas_code: str):
        self.tas_code = tas_code
    
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


