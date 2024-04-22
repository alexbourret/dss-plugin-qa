import dataiku

from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
from datetime import datetime


input_A_names = get_input_names_for_role('input_A_role')
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

input_B_names = get_input_names_for_role('input_B_role')
input_B_datasets = [dataiku.Dataset(name) for name in input_B_names]

output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# -*- coding: utf-8 -*-
import dataiku
import math
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
data = dataiku.Dataset(input_A_names[0])
data_df = data.get_dataframe()

previous_dataset_df = None
if input_B_names and len(input_B_names)>0:
    previous_dataset = dataiku.Dataset(input_B_names[0])
    previous_dataset_df = previous_dataset.get_dataframe()

config = get_recipe_config()
check_max_variation = config.get("check_max_variation", True)
max_variation = None
if check_max_variation:
    max_variation = config.get("max_variation", 33)

major_currencies = ["JPY", "USD", "CHF", "DKK", "SEK", "GBP", "HKD", "SGD", "NOK"]

previous_line = None
previous_dataset_line = None
previous_dataset_latest_date = None
if previous_dataset_df is not None:
    print("ALX:There is a reference dataset")
    previous_dataset_iterator = previous_dataset_df.iterrows()
    _, previous_dataset_line = next(previous_dataset_iterator)
    print("ALX:previous_dataset_line={}".format(previous_dataset_line))
    date = previous_dataset_line["Date"]
    print("ALX:date={}".format(date))
    previous_dataset_latest_date = previous_dataset_line["Date"]
    print("ALX:The reference dataset earliest date is {}".format(previous_dataset_latest_date))

reference_dataset_synched = False

for row_number, input_parameters_row in data_df.iterrows():
    column_number = 0
    if previous_dataset_latest_date and previous_dataset_latest_date == input_parameters_row["Date"]:
        # from this point on, the current and reference datasets are supposed to remained synched
        reference_dataset_synched = True
    if reference_dataset_synched:
        for reference_value, reference_column_name in zip(previous_dataset_line, previous_dataset_df.columns):
            if type(input_parameters_row[reference_column_name])==float and math.isnan(input_parameters_row[reference_column_name]) and type(reference_value)==float and math.isnan(reference_value):
                pass
            elif input_parameters_row[reference_column_name] != reference_value:
                raise Exception("Mismatch between reference and new dataset on row {} ({}). On column {}, {}!={}".format(
                    row_number,
                    input_parameters_row["Date"],
                    reference_column_name,
                    input_parameters_row[reference_column_name],
                    reference_value
                ))
        try:
            _, previous_dataset_line = next(previous_dataset_iterator)
        except StopIteration as error_message:
            pass
        
    for actual_value, column_name in zip(input_parameters_row, data_df.columns):
        if column_name == "Date":
            try:
                new_date = datetime.strptime(actual_value, "%Y-%m-%d")
            except Exception as error_message:
                raise Exception("On row {}, date is wrong format ({})".format(row_number, actual_value))
            if row_number == 0:
                previous_date = new_date
            delta = previous_date - new_date
            if delta.days != 1 and row_number > 0:
                raise Exception("On row {}, {} days difference between {} and {}".format(row_number, delta.days, previous_date, new_date))
            previous_date = new_date
        if column_name in major_currencies:
            if math.isnan(actual_value):
                raise Exception("On row {} ({}), major currency {} is {}".format(row_number, new_date, column_name, actual_value))
        if column_name != "Date":
            if type(actual_value) != float:
                raise Exception("On row {} ({}), currency {}, value is {} (type {})".format(row_number, new_date, column_name, actual_value, type(actual_value)))
            if actual_value == 0 or actual_value == 0.0:
                raise Exception("On row {} ({}), currency {}, value is {}".format(row_number, new_date, column_name, actual_value))
        if (check_max_variation) and (column_name != "Date") and (previous_line is not None):
            daily_variation = (float(actual_value) - float(previous_line[column_name])) / float(actual_value) * 100.
            if daily_variation > max_variation:
                raise Exception("On row {} ({}), currency {} varied by {}%".format(row_number, new_date, column_name, daily_variation))
    previous_line = input_parameters_row

if (previous_dataset_df is not None) and (not reference_dataset_synched):
    raise Exception("Could not sync reference and latest datasets")

errors_df = data_df # For this sample code, simply copy input to output


# Write recipe outputs
errors = dataiku.Dataset(output_A_names[0])
errors.write_with_schema(errors_df)
