import re

import pandas as pd
from clickhouse_driver import Client


class PyCH:
    """python connect clickhouse"""

    def __init__(
        self, host, dbname, username, password, port, send_receive_timeout=2000
    ):

        self.host = host
        self.dbname = dbname
        self.username = username
        self.password = password
        self.port = port
        self.send_receive_timeout = send_receive_timeout

        self.client = Client(
            host=self.host,
            database=self.dbname,
            user=self.username,
            password=self.password,
            send_receive_timeout=self.send_receive_timeout,
            port=self.port,
        )

    def get_pandasDataFrame(self, sql: str) -> pd.DataFrame:

        data, columns = self.client.execute(sql, columnar=True, with_column_types=True)
        pandasDataFrame = pd.DataFrame(
            {re.sub(r"\W", "_", col[0]): d for d, col in zip(data, columns)}
        )

        return pandasDataFrame
