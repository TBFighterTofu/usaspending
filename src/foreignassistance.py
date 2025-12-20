import requests
import json
from pathlib import Path

DATA_FOLDER = Path(__file__).parent.parent / "data" / "usaspending"

def lookup_foreign_budget():
    url = "https://foreignassistance.gov/data-api/complete-data.json"
    sectors = ["Other Public Health Threats", "Nutrition", "Tuberculosis", "Maternal and Child Health", "Family Planning and Reproductive Health", "HIV/AIDS", "Malaria", "Pandemic Influenza and Other Emerging Threats (PIOET)", "Water Supply and Sanitation"]
    
    out = {}
    for sector in sectors:
        params = {"funding_account_id": "19x1031", "per_page": 1000, "usg_sector_name": sector}
        new_out = {}
        res = requests.get(url, params = params)
        for row in res.json()["data"]:
            fiscal_year = int(row["fiscal_year"])
            if fiscal_year not in new_out:
                new_out[fiscal_year] = {}
                for transaction_types in ["Appropriated and Planned", "Obligations", "Disbursements", "President's Budget Requests"]:
                    for p in ["Current", "Constant"]:
                        new_out[fiscal_year][f"{p} {transaction_types}"] = 0
            transaction_type = row["transaction_type_name"]
            current_amount = row["current_amount"]
            constant_amount = row["constant_amount"]
            new_out[fiscal_year]["Current "+transaction_type] += current_amount
            new_out[fiscal_year]["Constant "+transaction_type] += constant_amount   
        out[sector] = new_out    

    with open(DATA_FOLDER / f"foreign_aid.json", "w") as f:
        json.dump(out, f, indent = 4, sort_keys = True)

if __name__=="__main__":
    lookup_foreign_budget()