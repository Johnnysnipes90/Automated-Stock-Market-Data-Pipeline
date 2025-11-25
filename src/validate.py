# src/validate.py
import great_expectations as ge
import pandas as pd

def validate_df(df):
    gdf = ge.from_pandas(df)
    suite = {
        "expectations": [
            {"expectation_type":"expect_column_to_exist","kwargs":{"column":"date"}},
            {"expectation_type":"expect_column_values_to_not_be_null","kwargs":{"column":"close"}},
            {"expectation_type":"expect_column_values_to_be_between","kwargs":{"column":"close","min_value":0,"max_value":100000}}
        ]
    }
    # run validations
    res = gdf.validate(expectation_suite=suite, only_return_failures=False)
    return res

# used by DAG or local tests