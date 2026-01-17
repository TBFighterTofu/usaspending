from pathlib import Path
from enum import Enum, auto

DATA_FOLDER = Path(__file__).parent.parent / "data"
SPENDING_BY_AWARD = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
SPENDING_OVER_TIME = "https://api.usaspending.gov/api/v2/search/spending_over_time/"
SPENDING_BY_TRANSACTION = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
AWARD_DOWNLOAD = "https://api.usaspending.gov/api/v2/download/contract"


class AwardType(Enum):
    """USASpending divides their data by award type, and they format the data for each award slightly differently when searching for awards. Use this class as an input to specify what kind of award you're looking for."""
    CONTRACT = auto()
    IDV = auto()
    LOAN = auto()
    GRANT = auto()
    DIRECT_PAYMENTS = auto()
    OTHER = auto()

def award_type_codes(award_type: AwardType) -> list[str]:
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
    
def program_activity_codes() -> dict:
    return {
        0: "Unknown",
        1: "Personnel compensation and benefits",
        2: "Contractual services and supplies",
        3: "Acquisition of assets",
        4: "Grants and fixed charges",
    }
                            
def usaid_tas(year: int):
    """Get the TAS code for a single-year USAID award."""
    return f"072-019-{year}/{year+1}-1031-000"