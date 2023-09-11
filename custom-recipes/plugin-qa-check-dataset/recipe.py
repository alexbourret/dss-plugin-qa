# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role
import pandas as pd
from plugin_qa_commons import chaos_monkey, build_value, build_column_name

input_dataset = get_input_names_for_role('input_dataset')
output_names_stats = get_output_names_for_role('api_output')
config = get_recipe_config()
dku_flow_variables = dataiku.get_flow_variables()


input_parameters_dataset = dataiku.Dataset(input_dataset[0])
input_parameters_dataframe = input_parameters_dataset.get_dataframe()

nb_rows_to_process = input_parameters_dataframe.shape[0]
type_mismatch_errors = 0
value_mismatch_errors = 0
column_name_mismatch_errors = 0
number_of_columns = 0
should_raise_on_error = config.get("should_raise_on_error", False)

for row_number, input_parameters_row in input_parameters_dataframe.iterrows():
    column_number = 0
    for actual_value in input_parameters_row:
        predicted_value = chaos_monkey(build_value(row_number, column_number))
        if type(predicted_value) != type(actual_value):
            error_message = "Error: type mismatch on [{},{}], {} expected, got {}. Value is {}".format(
                row_number, column_number, type(predicted_value), type(actual_value), actual_value)
            print(error_message)
            if should_raise_on_error:
                raise Exception(error_message)
            type_mismatch_errors = type_mismatch_errors + 1
        if predicted_value != actual_value:
            if isinstance(predicted_value, float):
                if (pd.isna(predicted_value) or pd.isna(actual_value)):
                    error_message = "Error: value mismatch on [{},{}], {} expected, got {}.".format(
                        row_number, column_number, predicted_value, actual_value)
                    print(error_message)
                    if should_raise_on_error:
                        raise Exception(error_message)
                    value_mismatch_errors = value_mismatch_errors + 1
                else:
                    error = actual_value - predicted_value
                    if error > 0.01:
                        error_message = "Error: value mismatch on [{},{}], {} expected, got {}.".format(
                            row_number, column_number, predicted_value, actual_value)
                        print(error_message)
                        if should_raise_on_error:
                            raise Exception(error_message)
                        value_mismatch_errors = value_mismatch_errors + 1
            else:
                error_message = "Error: value mismatch on [{},{}], {} expected, got {}.".format(
                    row_number, column_number, predicted_value, actual_value)
                print(error_message)
                if should_raise_on_error:
                    raise Exception(error_message)
                value_mismatch_errors = value_mismatch_errors + 1
        column_number = column_number + 1
    number_of_columns = column_number

use_cjk_in_columns_names = config.get("use_cjk_in_columns_names", True)
use_emojis_in_columns_names = config.get("use_emojis_in_columns_names", True)
column_number = 0
for column_name in input_parameters_dataframe.columns:
    predictred_column_name = build_column_name(column_number, use_cjk=use_cjk_in_columns_names, use_emoji=use_emojis_in_columns_names)
    if predictred_column_name != column_name:
        error_message = "Error: value mismatch on column {}, {} expected, got {}.".format(
            column_number, predictred_column_name, column_name)
        print(error_message)
        if should_raise_on_error:
            raise Exception(error_message)
        column_name_mismatch_errors = column_name_mismatch_errors + 1
    column_number += 1

output_dataset = dataiku.Dataset(output_names_stats[0])
output_row = {
    "Nb type mismatch": type_mismatch_errors,
    "Nb Value mismatch": value_mismatch_errors,
    "Nb column mismatch": column_name_mismatch_errors
}
unnested_items_rows = [output_row]

output_df = pd.DataFrame([], columns=["Nb type mismatch", "Nb Value mismatch", "Nb column mismatch"])
output_df = output_df.append(output_row, ignore_index=True)
plugin_installation_result_v2 = dataiku.Dataset(output_names_stats[0])
plugin_installation_result_v2.write_with_schema(output_df)
