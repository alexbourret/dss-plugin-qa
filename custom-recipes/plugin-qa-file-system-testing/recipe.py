import dataiku
import uuid
import json
import time
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
from pandas import json_normalize

input_A_names = get_input_names_for_role('input_A_role')
output_A_names = get_output_names_for_role('main_output')

config = get_recipe_config()
shoud_raise_on_error = config.get("shoud_raise_on_error", False)
shoud_test_last_modified = config.get("shoud_test_last_modified", False)
last_modified_tolerance = config.get("last_modified_tolerance", 1000)
shoud_test_file_size = config.get("shoud_test_file_size", False)
shoud_test_on_root = config.get("shoud_test_on_root", True)
shoud_test_on_folder = config.get("shoud_test_on_folder", True)

Local = dataiku.Folder(input_A_names[0])
Local_info = Local.get_info()

result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
results = []
random_file_name = uuid.uuid4().hex + ".txt"

data_as_string = json.dumps(Local_info)
data_size = len(data_as_string)

if shoud_test_on_root:
    result["Test name"] = "Upload file on root"
    try:
        write_time = int(time.time()) * 1000
        Local.write_json(random_file_name, Local_info)
        result["Success"] = "OK"
        results.append(result)
    except Exception as error:
        result["Error"] = "{}".format(error)
        results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Download and compare file on root"
    try:
        with Local.get_download_stream(random_file_name) as file_handle:
            data_back = file_handle.read()
            data_back = data_back.decode('utf-8')
            data_back = json.loads(data_back)
            if data_back == Local_info:
                result["Success"] = "OK"
            else:
                result["Success"] = "KO"
                result["Error"] = "File not identical"
            results.append(result)
    except Exception as error:
        result["Error"] = "{}".format(error)
        results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Checking file properties"
    result["Success"] = "KO"
    result["Error"] = "File not found"
    try:
        path_details = Local.get_path_details()
        children = path_details.get("children", [])
        for child in children:
            child_name = child.get("name")
            if child_name == random_file_name:
                child_size = child.get("size")
                if shoud_test_file_size and data_size != child_size:
                    result["Success"] = "KO"
                    result["Error"] = "Size mismatch"
                else:
                    child_lastModified = child.get("lastModified")
                    if shoud_test_last_modified and abs(write_time - child_lastModified) > last_modified_tolerance:
                        result["Success"] = "KO"
                        result["Error"] = "Time mismatch: {} / {}".format(write_time, child_lastModified)
                    else:
                        result["Success"] = "OK"
                        result["Error"] = ""
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "Could not check the path details: {}".format(error)
    results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Delete file"
    try:
        Local.delete_path(random_file_name)
        result["Success"] = "OK"
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "{}".format(error)
    results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Checking file was deleted"
    result["Success"] = "OK"
    try:
        path_details = Local.get_path_details()
        children = path_details.get("children", [])
        for child in children:
            child_name = child.get("name")
            if child_name == random_file_name:
                result["Success"] = "KO"
                result["Error"] = "File {} was not deleted".format(random_file_name)
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "Could not check the path details: {}".format(error)
    results.append(result)

if shoud_test_on_folder:
    random_file_name = uuid.uuid4().hex + ".txt"
    random_folder_name = uuid.uuid4().hex

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Upload file on folder"
    try:
        write_time = int(time.time()) * 1000
        Local.write_json(random_folder_name + "/" + random_file_name, Local_info)
        result["Success"] = "OK"
        results.append(result)
    except Exception as error:
        result["Error"] = "{}".format(error)
        results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Checking file properties inside folder"
    result["Success"] = "KO"
    result["Error"] = "File not found"
    try:
        path_details = Local.get_path_details(random_folder_name)
        children = path_details.get("children", [])
        for child in children:
            child_name = child.get("name")
            if child_name == random_file_name:
                child_size = child.get("size")
                if shoud_test_file_size and data_size != child_size:
                    result["Success"] = "KO"
                    result["Error"] = "Size mismatch"
                else:
                    child_lastModified = child.get("lastModified")
                    if shoud_test_last_modified and abs(write_time - child_lastModified) > last_modified_tolerance:
                        result["Success"] = "KO"
                        result["Error"] = "Time mismatch: {} / {}".format(write_time, child_lastModified)
                    else:
                        result["Success"] = "OK"
                        result["Error"] = ""
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "Could not check the path details: {}".format(error)
    results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Delete folder"
    try:
        Local.delete_path(random_folder_name)
        result["Success"] = "OK"
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "{}".format(error)
    results.append(result)

    result = {"Test name": "", "Timestamp": "", "Error": "", "Success": ""}
    result["Test name"] = "Checking folder was deleted"
    result["Success"] = "OK"
    try:
        path_details = Local.get_path_details()
        children = path_details.get("children", [])
        for child in children:
            child_name = child.get("name")
            if child_name == random_folder_name:
                result["Success"] = "KO"
                result["Error"] = "Folder {} was not deleted".format(random_folder_name)
    except Exception as error:
        result["Success"] = "KO"
        result["Error"] = "Could not check the path details: {}".format(error)
    results.append(result)

results_df = json_normalize(results)
results_dataset = dataiku.Dataset(output_A_names[0])
results_dataset.write_with_schema(results_df)

if shoud_raise_on_error:
    for result in results:
        error = result.get("Error")
        if error:
            raise Exception("There was at least one error ({})".format(error))
