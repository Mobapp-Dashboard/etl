#!/usr/bin/env ipython
from etl.extract import Extract
from etl.load import Load
from etl.transform import Transform


class Etl:
    def __init__(self, path, dataset="dublin-2013", **kwargs):
        self.raw_data = Extract(path, dataset, **kwargs)
        self.transformed_data = Transform(self.raw_data)
        Load(
            self.transformed_data.get_df(), self.transformed_data.get_df(), target="csv"
        )

    def get_df(self, data_stage="transformed"):
        if data_stage == "raw":
            data_container = self.raw_data
        elif data_stage == "transformed":
            data_container = self.transformed_data
        return data_container.get_df()

    def get_df_metadata(self):
        return self.transformed_data.get_df_metadata()
