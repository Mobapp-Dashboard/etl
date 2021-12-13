#!/usr/bin/env ipython
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class Preprocess:
    def __init__(
        self,
        traj_ID: list,
        categories=[],
        booleans=[],
        ints=[],
        drops=[],
        timestamp="",
        timestamp_unit="",
        min_points=50,
    ):
        self.traj_ID_list = traj_ID
        self.min_points = min_points
        self.categories = categories
        self.booleans = booleans
        self.ints = ints
        self.timestamp = timestamp
        self.timestamp_unit = timestamp_unit
        self.drops = drops

    def __call__(self, data):
        logger.info("Cleaning Data")
        data = self.clean_data(data)
        logger.info("Setting types")
        data = self.set_types(data)
        logger.info("Generating Trajectories IDs")
        data, df_meta = self.generate_traj_id(data, self.traj_ID_list)
        logger.info("Preprocessing Finished!")
        return data, df_meta

    def clean_data(self, data):
        data.drop(self.drops, axis=1, inplace=True)
        ## Retirar linhas com NAN nos campos da chave
        data.dropna(subset=self.traj_ID_list, inplace=True)
        ### Retira conjuntos de pontos (trajetórias) com menos de 50 pontos
        data = [
            x for _, x in data.groupby(self.traj_ID_list) if (len(x) > self.min_points)
        ]
        data = pd.concat(data)
        return data

    def set_types(self, data):
        data[self.categories] = data[self.categories].astype("category")
        data[self.booleans] = data[self.booleans].astype("bool")
        data[self.ints] = data[self.ints].astype("int")
        data[f"_{self.timestamp}"] = pd.to_datetime(
            data.timestamp, unit=self.timestamp_unit
        )
        return data

    def generate_traj_id(self, df, trajectory_features):
        df_meta = (
            df.groupby(trajectory_features).size().reset_index()[trajectory_features]
        )
        df_meta = df_meta.reset_index()
        df = pd.merge(df, df_meta, on=trajectory_features, how="inner")
        return (df, df_meta)
