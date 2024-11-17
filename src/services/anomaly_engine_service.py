from datetime import datetime
import json
from typing import List
from flask import g
from src.dto.response_dto import ResponseDto
from src.infra.db_repo import DatabaseManager
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=1)


class AnomalyEngine:
    def __init__(self):
        self.db: DatabaseManager = g.db_manager
    
    def save_record(self, dataRequest:dict):
        try:
            userId = dataRequest['user_id']
            alert_type = dataRequest['alert_type']
            timestamp = dataRequest['timestamp']
            risk_score = dataRequest['risk_score']

            insert_query = """
                insert into kd_hk_anomalies (user_id, alert_type, timestamp, risk_score)
                values(?, ?, ?, ?)
            """

            self.db.single_inserts(insert_query, (userId, alert_type, timestamp, risk_score))

            return ResponseDto(True, 'success', None, 200)
        
        except Exception as e:
            logger.error(f"Error while trying to save: {e}")
            return ResponseDto(False, 'An error occurred while fetching save record', None, 500)