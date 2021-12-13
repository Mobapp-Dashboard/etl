#!/usr/bin/env ipython
import logging

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class Extract:
    def __init__(self, path, dataset, **kwargs):
        self.df = None

        if dataset == "dublin-2013":
            self.df = self.extract_dublin2013(path, **kwargs)
            logger.info(f"DataFrame with {len(self.df)} rows")

    def get_df(self):
        return self.df

    def extract_dublin2013(self, path, **kwargs):

        self.features = [
            "timestamp",
            "line_id",
            "direction",
            "journey_id",
            "time_frame",
            "vehicle_journey_id",
            "operator",
            "congestion",
            "lng",
            "lat",
            "delay",
            "block_id",
            "vehicle_id",
            "stop_id",
            "stop",
        ]

        days = kwargs["days"] if ("days" in kwargs) else range(32)
        if isinstance(days, int):
            days = [days]
        logger.info(days)
        file_template = "siri.201301{}.csv.gz"
        full_path = path + file_template
        logger.info(full_path)

        dfs = []
        for day in days:
            logger.info(f"Extracting day {day}")
            day = "0" + str(day) if (day < 10) else str(day)
            df = pd.read_csv(full_path.format(day), names=self.features)
            dfs.append(df)

        logger.info("Concatenate")
        df = pd.concat(dfs)
        limit = kwargs.get("limit", -1)
        if limit >= 0:
            df = df.iloc[: int(limit)]
        return df
