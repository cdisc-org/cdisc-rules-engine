import os
import re


def validate_folder_structure(rtype: str, root_folder: str, rule_id: str) -> list[str]:
    error_messages = []
    # Define the expected folder structure
    pos_neg_folder_names = ["positive", "negative"]
    data_results_folder_names = ["data", "results"]

    # Check if root folder exists
    if not os.path.exists(root_folder):
        error_messages.append(f"{rtype}:{rule_id}: Root folder '{root_folder}' does not exist.")

    validate_rule_id_folder(error_messages, root_folder, pos_neg_folder_names, rtype, rule_id)

    # Iterate over required subfolders (positive, negative)
    for pos_neg_folder in pos_neg_folder_names:
        pos_neg_folder_path = os.path.join(root_folder, pos_neg_folder)

        if not os.path.exists(pos_neg_folder_path):
            error_messages.append(f"{rtype}:{rule_id}: Folder '{pos_neg_folder}' is missing.")
            continue

        validate_pos_neg_folder(error_messages, pos_neg_folder_path, pos_neg_folder, rtype, rule_id)

        # List subdirectories in 'positive' or 'negative'
        maybe_two_digit_folders = [f.name for f in os.scandir(pos_neg_folder_path) if f.is_dir()]

        validate_two_digit_folders(
            error_messages,
            maybe_two_digit_folders,
            pos_neg_folder_path,
            data_results_folder_names,
            pos_neg_folder,
            rtype,
            rule_id,
        )

    return error_messages


def validate_rule_id_folder(
    error_messages: list, root_folder: str, pos_neg_folder_names: str, rtype: str, rule_id: str
):
    # Ensure no files in ruleid folder level
    for file in os.scandir(root_folder):
        if file.is_file() and not file.name.startswith("."):
            error_messages.append(
                f"{rtype}:{rule_id}: File {file.name} found in rule root folder, which is not allowed."
            )

    for f in os.scandir(root_folder):
        if f.is_dir() and f.name not in pos_neg_folder_names:
            error_messages.append(
                f"{rtype}:{rule_id}: Unexpected folder '{f.name}' found in rule root folder, which is not allowed."
            )


def validate_pos_neg_folder(
    error_messages: list, pos_neg_folder_path: str, pos_neg_folder: str, rtype: str, rule_id: str
):
    # Ensure no files in positive/negative folder level directly
    for file in os.scandir(pos_neg_folder_path):
        if file.is_file() and not file.name.startswith("."):
            error_messages.append(
                f"{rtype}:{rule_id}: File {file.name} found in '{pos_neg_folder}', which is not allowed."
            )


def validate_two_digit_folders(
    error_messages: list,
    maybe_two_digit_folders: list,
    pos_neg_folder_path: str,
    data_results_folder_names: list,
    pos_neg_folder: str,
    rtype: str,
    rule_id: str,
):
    if "01" not in maybe_two_digit_folders:
        error_messages.append(f"{rtype}:{rule_id}: Required folder '01' is missing in '{pos_neg_folder}'.")

    two_digit_regex = re.compile(r"^\d{2}$")
    for maybe_two_digit_folder in maybe_two_digit_folders:
        maybe_two_digit_folder_path = os.path.join(pos_neg_folder_path, maybe_two_digit_folder)

        # Check all subfolders for two-digit naming
        if not two_digit_regex.match(maybe_two_digit_folder):
            error_messages.append(
                f"{rtype}:{rule_id}: Folder '{maybe_two_digit_folder}' in '{pos_neg_folder}' "
                f"does not match two-digit naming."
            )
            continue

        two_digit_folder_path = maybe_two_digit_folder_path
        two_digit_folder = maybe_two_digit_folder

        # Check that there are no unexpected folders at this level
        for f in os.scandir(two_digit_folder_path):
            if f.is_dir() and f.name not in data_results_folder_names:
                error_messages.append(
                    f"{rtype}:{rule_id}: Unexpected folder '{f.name}' found in '{two_digit_folder}' "
                    f"under '{pos_neg_folder}'."
                )

        for subfolder_name in data_results_folder_names:
            # Check for 'data' and 'results' subfolders
            subfolder_data_results_path = os.path.join(two_digit_folder_path, subfolder_name)
            if not os.path.exists(subfolder_data_results_path):
                error_messages.append(
                    f"{rtype}:{rule_id}: Subfolder '{subfolder_name}' is missing in '{two_digit_folder}' "
                    f"under '{pos_neg_folder}'."
                )
                continue

            # Check if there is at least one '.xlsx' file in the 'data' folder
            xlsx_extension = ".xlsx"
            json_extension = ".json"
            if subfolder_name == "data":
                data_folder_validation(
                    error_messages,
                    xlsx_extension,
                    subfolder_data_results_path,
                    two_digit_folder,
                    rtype,
                    rule_id,
                    pos_neg_folder,
                )

            # Check if there are any non-.json or non-xlsx files in results
            if subfolder_name == "results":
                results_folder_validation(
                    error_messages,
                    xlsx_extension,
                    json_extension,
                    subfolder_data_results_path,
                    two_digit_folder,
                    rtype,
                    rule_id,
                    pos_neg_folder,
                )


def data_folder_validation(
    error_messages: list,
    xlsx_extension: str,
    subfolder_data_results_path: str,
    two_digit_folder: str,
    rtype: str,
    rule_id: str,
    pos_neg_folder: str,
):
    found_xlsx = any(f.is_file() and f.name.endswith(xlsx_extension) for f in os.scandir(subfolder_data_results_path))
    if not found_xlsx:
        error_messages.append(
            f"{rtype}:{rule_id}: Missing '.xlsx' file in 'data' folder under '{two_digit_folder}' "
            f"in '{pos_neg_folder}'."
        )
    for file in os.scandir(subfolder_data_results_path):
        if file.is_file() and not (file.name.endswith(xlsx_extension) or file.name.lower() == "define.xml"):
            error_messages.append(
                f"{rtype}:{rule_id}: Non-xlsx and non-'define.xml' file found"
                f"in 'data' folder under '{two_digit_folder}' in '{pos_neg_folder}'."
            )
    for f in os.scandir(subfolder_data_results_path):
        if f.is_dir():
            error_messages.append(
                f"{rtype}:{rule_id}: Unexpected folder '{f.name}' "
                f"found in '{two_digit_folder}/data' under '{pos_neg_folder}'."
            )


def results_folder_validation(
    error_messages: list,
    xlsx_extension: str,
    json_extension: str,
    subfolder_data_results_path: str,
    two_digit_folder: str,
    rtype: str,
    rule_id: str,
    pos_neg_folder: str,
):
    for file in os.scandir(subfolder_data_results_path):
        if file.is_file() and not (file.name.endswith(xlsx_extension) or file.name.endswith(json_extension)):
            error_messages.append(
                f"{rtype}:{rule_id}: Non-xlsx/json file found in 'results' folder "
                f"under '{two_digit_folder}' in '{pos_neg_folder}'."
            )
    for f in os.scandir(subfolder_data_results_path):
        if f.is_dir():
            error_messages.append(
                f"{rtype}:{rule_id}: Unexpected folder '{f.name}' "
                f"found in '{two_digit_folder}/results' "
                f"under '{pos_neg_folder}'."
            )


def get_immediate_subfolders(folder_path):
    return sorted([f.name for f in os.scandir(folder_path) if f.is_dir()])


def print_invalid(rtype: str, path: str):
    error_messages = []
    root_folder = f"{path}/{rtype}/"
    subfolders = get_immediate_subfolders(root_folder)
    for rulefolder in subfolders:
        error_messages.extend(validate_folder_structure(rtype, root_folder + rulefolder, rule_id=rulefolder))

    output_file = f"{path}/___{rtype}_sharepoint_issues.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for item in error_messages:
            f.write(f"{item}\n")


sp_path = os.path.expanduser("~") + "/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting"
print_invalid("SDTMIG", sp_path)
print_invalid("ADAMIG", sp_path)
print_invalid("FDA Business Rules", sp_path)
print_invalid("FDA Validator Rules", sp_path)
