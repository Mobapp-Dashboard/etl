#!/usr/bin/env ipython
from etl.transform.feature_enginnering import Features_Engineering
from etl.transform.preprocess import Preprocess

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class Transform:
    def __init__(self, raw):
        self.df, self.df_meta = self.process_dublin(raw.get_df())

    def get_df(self):
        return self.df

    def get_df_metadata(self):
        return self df_meta

    def process_dublin(self, df):
        trajectory_features = [
            "line_id",
            "journey_id",
            "time_frame",
            "vehicle_journey_id",
            "operator",
            "vehicle_id",
        ]

        list_ints = ["line_id"]

        preprocess = Preprocess(
            trajectory_features,
            ["lat", "lng"],
            ints=list_ints,
            timestamp="timestamp",
            timestamp_unit="us",
        )
        df, df_meta = preprocess(df)

        feat_eng = Features_Engineering("index", "lat", "lng", timestamp="timestamp")
        df = feat_eng(df)

        return df, df_meta
