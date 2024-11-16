from flask import g
from src.infra.db_repo import DatabaseManager


class RuleEngine:
    def __init__(self):
        self.db = g.db_manager

    def get_data_points(self):
        table_columns = self.db.get_columns_of_table('kd_hk_transactions')
        if table_columns:
            columns = [row[0] for row in table_columns]
            print(columns)
            return columns, 200
        else:
            return None, 400