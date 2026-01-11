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

def usaid_tas(year: int):
    """Get the TAS code for a single-year USAID award."""
    return f"072-019-{year}/{year+1}-1031-000"