import json
import pandas as pd
from dataclasses import dataclass

from usa_types import DATA_FOLDER, AwardType, usaid_tas
from awards import AwardSearchDownload

@dataclass
class SpendingSummary:
    name: str
    obligated_amount: int = 0
    gross_outlay_amount: int = 0
    children: list["SpendingSummary"] | None = None

    def append(self, name: str, total_obligated: int, total_outlay: int):
        self.obligated_amount += total_obligated
        self.gross_outlay_amount += total_outlay
        if self.children is None:
            self.children = []
        self.children.append(SpendingSummary(name = name, obligated_amount=total_obligated, gross_outlay_amount=total_outlay))

    def tabs(self, tabs: int = 0) -> str:
        if tabs == 0:
            return ""
        else:
            return "".join(["  " for _ in range(tabs)])

    def print_sums(self, tabs: int = 0):
        tab = self.tabs(tabs)
        if tabs == 0:
            print(" -------- ")
        print(f"{tab}{self.name} / Obligated amount: ${self.obligated_amount:,} / Gross outlay amount: ${self.gross_outlay_amount:,}")
        if self.children is not None:
            print("Children")
            for child in self.children:
                child.print_sums(tabs + 1)

class USASpendingAnalysis:
    
    # -- Analysis

    def program_activity(self) -> SpendingSummary:
        file = DATA_FOLDER / "fy25_program_activity.json"
        with open(file) as f:
            d = json.load(f)
        summary = SpendingSummary("Program Activity")
        for child in d["children"]:
            summary.append(child["name"], child["obligated_amount"], child["gross_outlay_amount"])
        return summary

    def federal_account_funding(self) -> SpendingSummary:
        summary = SpendingSummary("Federal Account Funding")
        for award_type in AwardType:
            awards = AwardSearchDownload(usaid_tas(2024))
            file_name = awards.combined_csv("FederalAccountFunding")
            if file_name.exists():
                contract_fa = pd.read_csv(file_name, index_col = False)
                total_obligated = int(contract_fa["transaction_obligated_amount"].sum())
                total_outlay = int(contract_fa["gross_outlay_amount_FYB_to_period_end"].sum())
                summary.append(award_type.name, total_obligated, total_outlay)
        return summary

if __name__=="__main__":
    # for award_type in AwardType:
    #     awards = USASpendingAwards(award_type, min_year = 2020, max_year = 2025)
    #     awards.run_all()
    analysis = USASpendingAnalysis()
    analysis.program_activity().print_sums()
    analysis.federal_account_funding().print_sums()