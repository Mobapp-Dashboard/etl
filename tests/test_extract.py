#!/usr/bin/env ipython
import pandas as pd

from etl.extract import Extract


def get_df(day):
    res = Extract("data/", "dublin-2013", limit=5000, days=day)
    df = res.get_df()
    return df


def test_incorrct_path_dataset():
    res = Extract("SOME_PATH", "SOME_DATASET", days=31)
    df = res.get_df()
    assert not isinstance(df, pd.DataFrame)


def test_day_31():
    df = get_df(31)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5000
