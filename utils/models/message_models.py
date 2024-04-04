from pydantic import BaseModel
from typing import List
from snapshotter.utils.models.message_models import TxLogsModel

class TrackingWalletInteractionSnapshot(BaseModel):
    wallet_address: str
    contract_address: str
    logs: List[TxLogsModel]
