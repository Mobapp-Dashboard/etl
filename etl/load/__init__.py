#!/usr/bin/env ipython
from etl.secrets import psql_pass


class Load:
    def __init__(self, df, df_meta, target="csv", **kwargs):

        if (target == "sql"):
            table_df = kwargs["sql"]["table_df"]
            table_df_meta = kwargs["sql"]["table_df_meta"]

            ip = kwargs["sql"]["ip"]
            user = kwargs["sql"]["user"]
            database = kwargs["sql"]["database"]
            engine = create_engine( f"postgresql://{user}:{psql_pass}@{ip}/{database}")

            save_in_db(df, table_df, engine)
            save_in_db(df_meta, table_df_meta, engine)

        else (target == "csv"):
            file_df = kwargs.get("file_df", "unamed_dataframe")
            file_df_meta = kwargs.get("file_df_meta", "unamed_metadata_dataframe")
            df.to_csv(file_df)
            df.to_csv(file_df_meta)

    def save_in_db(self, data, name_table, engine):
        return data.to_sql(
            name_table,
            engine,
            if_exists='append',
            index=False,
        )
