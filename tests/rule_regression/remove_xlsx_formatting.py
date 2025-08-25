# this is useful code should we ever need to automatically remove formatting
# from Excel files in that sharepoint, but we are currently outsourcing that
# problem to rule maintainers...
# import pandas as pd
# import os
# import warnings
# import xlwings as xw
# from openpyxl import load_workbook

# # Suppress specific warning related to openpyxl
# warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# # Function to get all absolute paths for .xlsx files in a folder
# def get_excel_file_paths_and_cleanup(folder_path: str):
#     excel_file_paths = []
#     for root, _, files in os.walk(folder_path):
#         for file in files:
#             if file.endswith(".xlsx"):
#                 file_path = os.path.join(root, file)
#                 # Delete files that start with cleaned_ or _remformat_
#                 if file.startswith("cleaned_") or file.startswith("_remformat_"):
#                     print(f"Deleting file: {file_path}")  # optional logging
#                     os.remove(file_path)
#                     continue
#                 excel_file_paths.append(file_path)
#     return excel_file_paths


# def remove_formatting_with_xlwings(app, file_path: str):
#     # Open Excel with xlwings (in the background, no GUI)

#     # Open the Excel workbook
#     wb = app.books.open(file_path)

#     # Iterate through all sheets and clear the formatting
#     # for sheet in wb.sheets:
#     # Remove all cell formatting (background, borders, font, etc.)
#     # sheet.clear_formats()
#     # sheet.api.UsedRange.Interior.Pattern = -4142

#     # Construct cleaned file path (ensure the path exists)
#     cleaned_file_path = os.path.join(os.path.dirname(file_path), "_remformat_" + os.path.basename(file_path))

#     # Force overwrite by directly saving
#     if os.path.exists(cleaned_file_path):
#         os.remove(cleaned_file_path)  # Delete the existing file to ensure overwriting

#     wb.save(cleaned_file_path)  # Save the cleaned file (overwrites if it exists)

#     # Close the workbook
#     wb.close()

#     return cleaned_file_path


# def remove_formatting_excel_file(app, file_path: str):
#     # Ignore already cleaned files
#     if os.path.basename(file_path).startswith("_remformat_"):
#         return

#     if os.path.basename(file_path).startswith("~"):
#         return

#     # Ignore old test data folders
#     if "/old test data/" in file_path.lower():
#         return

#     # Ignore results folders
#     if "/results/" in file_path.lower():
#         return

#     # First, remove any formatting and save a cleaned file
#     cleaned_file_path = remove_formatting_with_xlwings(app, file_path)

#     # Load the cleaned file with openpyxl (data_only=True to get only evaluated values)
#     wb = load_workbook(cleaned_file_path, data_only=True)

#     # Get the directory and cleaned file path
#     dir_path = os.path.dirname(file_path)
#     os.makedirs(dir_path, exist_ok=True)

#     # Create a Pandas ExcelWriter to save the cleaned data
#     with pd.ExcelWriter(cleaned_file_path, mode="w") as writer:
#         for sheet_name in wb.sheetnames:
#             ws = wb[sheet_name]

#             # Read the data (skip empty rows)
#             data = list(ws.values)
#             first_data_row = next((i for i, row in enumerate(data) if any(cell is not None for cell in row)), None)
#             data = data[first_data_row:]

#             # Create a DataFrame with the first row as the header
#             df_cleaned = pd.DataFrame(data[1:], columns=data[0])

#             # Write the cleaned DataFrame to the new file
#             df_cleaned.to_excel(writer, sheet_name=sheet_name, index=False)


# home = os.path.expanduser("~")
# sdtm_unit_tests_root_path = home + "/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting_test/SDTMIG"
# sdtm_file_paths = get_excel_file_paths_and_cleanup(sdtm_unit_tests_root_path)
# # print(len(sdtm_file_paths))
# # print(sdtm_file_paths)

# adam_unit_tests_root_path = home + "/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting_test/ADAMIG"
# adam_file_paths = get_excel_file_paths_and_cleanup(adam_unit_tests_root_path)
# # print(len(adam_file_paths))
# # print(adam_file_paths)

# fdab_unit_tests_root_path = home + "/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting_test/FDA Business Rules"
# fdab_file_paths = get_excel_file_paths_and_cleanup(fdab_unit_tests_root_path)
# # print(len(fdab_file_paths))
# # print(fdab_file_paths)

# fdav_unit_tests_root_path = home + "/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting_test/FDA Validator Rules"
# fdav_file_paths = get_excel_file_paths_and_cleanup(fdav_unit_tests_root_path)
# # print(len(fdav_file_paths))
# # print(fdav_file_paths)


# with xw.App(visible=False) as app:

# remove_formatting_excel_file(
#     app,
#     f"/Users/verisian/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting_test/"
#     f" FDA Business Rules/FB5115/negative/data/unit-test-coreid-FB5115-negative.xlsx",
# )
# for file_path in sdtm_file_paths:
#     try:
#         clean_excel_file_2(file_path)
#     except Exception as e:
#         print(f"Error cleaning file {file_path}: {e}")

# for file_path in adam_file_paths:
#     try:
#         clean_excel_file_2(file_path)
#     except Exception as e:
#         print(f"Error cleaning file {file_path}: {e}")

# for idx, file_path in enumerate(fdab_file_paths):
#     try:
#         print(str(idx) + "-" + file_path)
#         remove_formatting_excel_file(app, file_path)
#     except Exception as e:
#         print(f"Error cleaning file {file_path}: {e}")

# for idx, file_path in enumerate(fdav_file_paths):
#     try:
#         print(str(idx) + "-" + file_path)
#         remove_formatting_excel_file(app, file_path)
#     except Exception as e:
#         print(f"Error cleaning file {file_path}: {e}")
