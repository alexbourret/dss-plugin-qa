import dataiku
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config


config = get_recipe_config()
mode = config.get("mode", "use_all_dataset")
if mode not in ("use_all_dataset", "duplicate_first_line"):
    raise ValueError("Unknown Chaos Monkey mode: {}".format(mode))

input_dataset = dataiku.Dataset(get_input_names_for_role("input_dataset")[0])
output_dataset = dataiku.Dataset(get_output_names_for_role("output_dataset")[0])
input_schema = input_dataset.read_schema()
output_dataset.write_schema(input_schema)
column_count = len(input_schema)

with output_dataset.get_writer() as writer:
    if mode == "use_all_dataset":
        for row_number, input_row in enumerate(input_dataset.iter_tuples()):
            output_row = list(input_row)
            if 1 <= row_number <= column_count:
                output_row[row_number - 1] = None
            writer.write_row_array(output_row)
    else:
        for input_row in input_dataset.iter_tuples(limit=1):
            writer.write_row_array(input_row)
            for column_number in range(column_count):
                output_row = list(input_row)
                output_row[column_number] = None
                writer.write_row_array(output_row)
            writer.write_row_array(input_row)
