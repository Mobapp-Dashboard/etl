#!/usr/bin/env ipython

import pandas as pd

from etl import Etl


def get_df(day):
    res = Etl("data/", "dublin-2013", limit=5000, days=day)
    df = res.get_df(data_stage="transformed")
    return df


def test_day_31():
    df = get_df(31)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 378
