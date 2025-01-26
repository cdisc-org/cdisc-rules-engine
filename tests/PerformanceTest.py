import os
import time
import pandas as pd
import subprocess
from statistics import median
import re
import click

# Function to extract preprocessing time from logs
def extract_preprocessing_time_from_logs(output_lines):
    start_time = None
    end_time = None

    for line in output_lines:
        if "Dataset Preprocessing Starts" in line:
            match = re.search(r"\\ST(\d+\.\d+)", line)
            if match:
                start_time = float(match.group(1))
                print(f"Extracted start time: {start_time}")
        elif "Dataset Preprocessing Ends" in line:
            match = re.search(r"\\ST(\d+\.\d+)", line)
            if match:
                end_time = float(match.group(1))
                print(f"Extracted end time: {end_time}")

    if start_time is not None and end_time is not None:
        return end_time - start_time

    return 0


# Function to extract operator times from logs
def extract_operator_times(output_lines):
    operator_times = {}
    start_times = {}

    for line in output_lines:
        match_start = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) starts", line)
        if match_start:
            timestamp, operation_name = float(match_start.group(1)), match_start.group(2)
            start_times[operation_name] = timestamp

        match_end = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) ends", line)
        if match_end:
            timestamp, operation_name = float(match_end.group(1)), match_end.group(2)
            if operation_name in start_times:
                duration = timestamp - start_times.pop(operation_name, 0)
                if operation_name in operator_times:
                    operator_times[operation_name].append(duration)
                else:
                    operator_times[operation_name] = [duration]

    return operator_times


# Function to extract operation times from terminal logs
def extract_operation_times_from_logs(output_lines):
    operation_times = []

    for line in output_lines:
        match_start = re.search(r"\\OPRNT(\d+\.\d+)-Operation Starts", line)
        if match_start:
            timestamp = float(match_start.group(1))
            start_time = time.time()

        match_end = re.search(r"\\OPRNT(\d+\.\d+)-Operation Ends", line)
        if match_end:
            timestamp = float(match_end.group(1))
            operation_times.append(time.time() - start_time)

    return operation_times

def all_rules_against_each_dataset(dataset_dir, rule_dir, total_calls):

    results = []  # To store the final report
    rule_results = {}  # To store rule-specific results for Excel sheet creation

    dataset_files = [
            os.path.join(dataset_dir, file)
            for file in os.listdir(dataset_dir)
            if file.endswith((".json", ".xpt"))
        ]
    rules = [
            file
            for file in os.listdir(rule_dir)
            if os.path.isfile(os.path.join(rule_dir, file))
        ]

    # For 
    for dataset_path in dataset_files:
        dataset_name = os.path.basename(dataset_path)

        # Initialize variables to collect times for the dataset across all rules
        all_time_taken = []
        all_preprocessing_times = []
        all_operator_times = {}
        all_operation_times = []

        for rule in rules:
            rule_name = os.path.basename(rule)
            time_taken = []  # Time for individual rule
            preprocessing_times = []  # Preprocessing times for individual rule
            operator_times = {}  # Operator times for individual rule
            operation_times = []  # Operation times for individual rule
            rule_executions = 0  # Count how many times the rule was executed for this dataset

            for num_call in range(total_calls):
                rule_path = os.path.join(rule_dir, rule)
                command = [
                    "python3",
                    "core.py",
                    "test",
                    "-s",
                    "sdtmig",
                    "-v",
                    "3.4",
                    "-r",
                    rule_path,
                    "-dp",
                    dataset_path,
                    "-l",
                    "critical"
                ]
                print(f"Executing: {' '.join(command)} for call {num_call + 1}")

                try:
                    start_time = time.time()
                    process = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    stdout, stderr = process.communicate()
                    end_time = time.time()

                    output_lines = stderr.splitlines()

                    # Extract preprocessing time, operator times, and operation times
                    preprocessing_time = extract_preprocessing_time_from_logs(output_lines)
                    rule_operator_times = extract_operator_times(output_lines)
                    rule_operation_times = extract_operation_times_from_logs(output_lines)

                    if process.returncode == 0:
                        time_taken.append(end_time - start_time)
                        preprocessing_times.append(preprocessing_time)
                        rule_executions += 1  # Increment rule execution count

                        # Aggregate operator times and operation times
                        for op, durations in rule_operator_times.items():
                            all_operator_times.setdefault(op, []).extend(durations)
                        all_operation_times.extend(rule_operation_times)
                    else:
                        raise subprocess.CalledProcessError(process.returncode, command, stderr)

                except subprocess.CalledProcessError as e:
                    results.append(
                        {
                            "function type": "TimeTestFunction",
                            "rule name": rule_name,
                            "dataset": dataset_name,
                            "status": "Failed",
                            "Number of Calls": num_call + 1,
                            "Mean Time": None,
                            "Median Time": None,
                            "Min Time": None,
                            "Max Time": None,
                            "Preprocessing Time": None,
                            "Operator Times": None,
                            "Operation Times": None,
                            "Error": e.stderr,
                        }
                    )
                    break

            # After all calls for a rule, summarize and add the times to the dataset-level collection
            if time_taken:
                all_time_taken.extend(time_taken)
                all_preprocessing_times.extend(preprocessing_times)

                # Store rule-specific results for creating separate sheets in Excel
                if rule_name not in rule_results:
                    rule_results[rule_name] = []
                rule_results[rule_name].append({
                    "Dataset": dataset_name,
                    "Number of Calls": rule_executions,
                    "Mean Time": sum(time_taken) / len(time_taken),
                    "Median Time": median(time_taken),
                    "Min Time": min(time_taken),
                    "Max Time": max(time_taken),
                    "Preprocessing Time": ", ".join(map(str, preprocessing_times)),
                    "Operator Times": ", ".join([f"{op}: {durations}" for op, durations in rule_operator_times.items()]),
                    "Operation Times": ", ".join(map(str, all_operation_times)),
                })

        # After all rules have been processed for a dataset, calculate the overall stats
        if all_time_taken:
            results.append(
                {
                    "function type": "TimeTestFunction",
                    "rule name": "All Rules Combined",
                    "dataset": dataset_name,
                    "status": "Successful",
                    "Number of Calls for each rule": total_calls,
                    "Mean Time": sum(all_time_taken) / len(all_time_taken),
                    "Median Time": median(all_time_taken),
                    "Min Time": min(all_time_taken),
                    "Max Time": max(all_time_taken),
                    "Preprocessing Time": ", ".join(map(str, all_preprocessing_times)),
                    "Operator Times": all_operator_times,
                    "Operation Times": ", ".join(map(str, all_operation_times)),
                    "Error": None,
                }
            )

    return results, rule_results

def all_datset_against_each_rule(dataset_dir, rule_dir, total_calls):
    results = []  # To store the final report
    dataset_results = {}  # To store dataset-specific results for Excel sheet creation

    dataset_files = [
        os.path.join(dataset_dir, file)
        for file in os.listdir(dataset_dir)
        if file.endswith((".json", ".xpt"))
    ]
    rules = [
        file
        for file in os.listdir(rule_dir)
        if os.path.isfile(os.path.join(rule_dir, file))
    ]

    for rule in rules:
        rule_name = os.path.basename(rule)

        # Initialize variables to collect times for the dataset across all rules
        all_time_taken = []
        all_preprocessing_times = []
        all_operator_times = {}
        all_operation_times = []

        rule_names=[]
        for dataset_path in dataset_files:
            dataset_name = os.path.basename(dataset_path)
            time_taken = []  # Time for individual rule
            preprocessing_times = []  # Preprocessing times for individual rule
            operator_times = {}  # Operator times for individual rule
            operation_times = []  # Operation times for individual rule
            rule_executions = 0  # Count how many times the rule was executed for this dataset

            for num_call in range(total_calls):
                rule_path = os.path.join(rule_dir, rule)
                command = [
                    "python3",
                    "core.py",
                    "test",
                    "-s",
                    "sdtmig",
                    "-v",
                    "3.4",
                    "-r",
                    rule_path,
                    "-dp",
                    dataset_path,
                    "-l",
                    "critical",
                ]
                print(f"Executing: {' '.join(command)} for call {num_call + 1}")

                try:
                    start_time = time.time()
                    process = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    stdout, stderr = process.communicate()
                    end_time = time.time()

                    output_lines = stderr.splitlines()

                    # Extract preprocessing time, operator times, and operation times
                    preprocessing_time = extract_preprocessing_time_from_logs(output_lines)
                    rule_operator_times = extract_operator_times(output_lines)
                    rule_operation_times = extract_operation_times_from_logs(output_lines)

                    if process.returncode == 0:
                        time_taken.append(end_time - start_time)
                        preprocessing_times.append(preprocessing_time)
                        rule_executions += 1  # Increment rule execution count

                        # Aggregate operator times and operation times
                        for op, durations in rule_operator_times.items():
                            all_operator_times.setdefault(op, []).extend(durations)
                        all_operation_times.extend(rule_operation_times)
                    else:
                        raise subprocess.CalledProcessError(process.returncode, command, stderr)

                except subprocess.CalledProcessError as e:
                    results.append(
                        {
                            "function type": "TimeTestFunction",
                            "rule name": rule_name,
                            "dataset": dataset_name,
                            "status": "Failed",
                            "Number of Calls": num_call + 1,
                            "Mean Time": None,
                            "Median Time": None,
                            "Min Time": None,
                            "Max Time": None,
                            "Preprocessing Time": None,
                            "Operator Times": None,
                            "Operation Times": None,
                            "Error": e.stderr,
                        }
                    )
                    break

            # After all calls for a rule, summarize and add the times to the dataset-level collection
            if time_taken:
                all_time_taken.extend(time_taken)
                all_preprocessing_times.extend(preprocessing_times)

                # Append dataset-specific results for creating the grouped sheet
                if dataset_name not in dataset_results:
                    dataset_results[dataset_name]=[]
                dataset_results[dataset_name].append({
                    "Dataset": dataset_name,
                    "Rule Name": rule_name,
                    "Number of Calls": rule_executions,
                    "Mean Time": sum(time_taken) / len(time_taken),
                    "Median Time": median(time_taken),
                    "Min Time": min(time_taken),
                    "Max Time": max(time_taken),
                    "Preprocessing Time": ", ".join(map(str, preprocessing_times)),
                    "Operator Times": ", ".join([f"{op}: {durations}" for op, durations in operator_times.items()]),
                    "Operation Times": ", ".join(map(str, operation_times)),
                })

        if all_time_taken:
            results.append(
                {
                    "function type": "TimeTestFunction",
                    "rule name": rule_name,
                    "dataset": "All datasets combined",
                    "status": "Successful",
                    "Number of Calls for each rule": total_calls,
                    "Mean Time": sum(all_time_taken) / len(all_time_taken),
                    "Median Time": median(all_time_taken),
                    "Min Time": min(all_time_taken),
                    "Max Time": max(all_time_taken),
                    "Preprocessing Time": ", ".join(map(str, all_preprocessing_times)),
                    "Operator Times": all_operator_times,
                    "Operation Times": ", ".join(map(str, all_operation_times)),
                    "Error": None,
                }
            )

    return results, dataset_results


def TimeTestFunction(dataset_dir, rule_dir, total_calls):
    print("Running for Grouped by rule and individual rule report creation")
    collective_rule_result, individual_rule_result = all_rules_against_each_dataset(dataset_dir, rule_dir, total_calls)
    print("\n\nRunning for Group by dataset report\n")
    collective_dataset_result, individual_dataset_result = all_datset_against_each_rule(dataset_dir, rule_dir, total_calls)
    
    return collective_rule_result, individual_rule_result, collective_dataset_result, individual_dataset_result


def delete_run_report_files(pattern, directory=None):
    """
    Deletes files in the specified or current directory that match a given pattern.
    
    Args:
        pattern (str): The regex pattern to match file names.
        directory (str, optional): The directory to search for files. Defaults to the current working directory.
    
    Returns:
        list: A list of deleted file names.
    """
    if directory is None:
        directory = os.getcwd()  # Use the current working directory if none is specified

    deleted_files = []
    regex = re.compile(pattern)

    for filename in os.listdir(directory):
        if regex.match(filename):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
                deleted_files.append(filename)
                print(f"Deleted: {filename}")
            except Exception as e:
                print(f"Error deleting {filename}: {e}")


@click.command()
@click.option('-dd', type=str)
@click.option('-rd', type=str)
@click.option('-total_calls', type=int)
@click.option('-od', default=os.getcwd(), help="Directory to save the output file (default is current directory)")
def main(dd, rd, total_calls, od):
    total_time_start = time.time()

    collective_rule_result, individual_rule_result, collective_dataset_result, individual_dataset_result = TimeTestFunction(dd, rd, total_calls)

    total_time = time.time() - total_time_start

    # Create an Excel writer and save the results to multiple sheets
    output_path = os.path.join(od, "rule_execution_report.xlsx")
    with pd.ExcelWriter(output_path) as writer:
        # Overall collective rule results
        collective_rule_df = pd.DataFrame(collective_rule_result)
        collective_rule_df.to_excel(writer, sheet_name="Collective Rule Result", index=False)

        # Individual rule results
        for rule_name, rule_data in individual_rule_result.items():
            sanitized_rule_name = re.sub(r'[\\/*?:[\]]', '_', rule_name)  # Replace invalid characters with '_'
            rule_df = pd.DataFrame(rule_data)
            rule_df.to_excel(writer, sheet_name=f"Rule_{sanitized_rule_name[:28]}", index=False)  # Truncate to 31 chars

        # Overall collective dataset results
        collective_dataset_df = pd.DataFrame(collective_dataset_result)
        collective_dataset_df.to_excel(writer, sheet_name="Collective Dataset Result", index=False)

        # Individual dataset results
        for dataset_name, dataset_data in individual_dataset_result.items():
            sanitized_dataset_name = re.sub(r'[\\/*?:[\]]', '_', dataset_name)  # Replace invalid characters with '_'
            dataset_df = pd.DataFrame(dataset_data)
            dataset_df.to_excel(writer, sheet_name=f"Dataset_{sanitized_dataset_name[:28]}", index=False)  # Truncate to 31 chars

    print(f"\nExecution results saved to '{output_path}'")
    file_pattern = r"CORE-Report-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.xlsx"


    delete_run_report_files(file_pattern)


if __name__ == "__main__":
    main()