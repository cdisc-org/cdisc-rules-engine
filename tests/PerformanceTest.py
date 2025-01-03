import os
import time
import subprocess
import pandas as pd
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

    return end_time - start_time if start_time and end_time else 0


# Function to extract operator times from logs
def extract_operator_times(output_lines):
    operator_times = {}
    start_times = {}

    for line in output_lines:
        match_start = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) starts", line)
        if match_start:
            timestamp, operation_name = float(match_start.group(1)), match_start.group(
                2
            )
            start_times[operation_name] = timestamp

        match_end = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) ends", line)
        if match_end:
            timestamp, operation_name = float(match_end.group(1)), match_end.group(2)
            if operation_name in start_times:
                duration = timestamp - start_times.pop(operation_name)
                operator_times.setdefault(operation_name, []).append(duration)

    return operator_times


# Function to extract operation times from terminal logs
def extract_operation_times_from_logs(output_lines):
    operation_times = {}
    start_times = {}

    for line in output_lines:
        match_start = re.search(r"\\OPRNT(\d+\.\d+)-Operation Starts", line)
        if match_start:
            timestamp = float(match_start.group(1))
            start_times[timestamp] = time.time()

        match_end = re.search(r"\\OPRNT(\d+\.\d+)-Operation Ends", line)
        if match_end:
            timestamp = float(match_end.group(1))
            if timestamp in start_times:
                duration = time.time() - start_times.pop(timestamp)
                operation_times[timestamp] = duration

    return operation_times


# Simplified TimeTestFunction
def TimeTestFunction(data_dir, rule_dir, total_calls):
    results = []

    # Collect all dataset files and rules
    data_files = [
        os.path.join(data_dir, file)
        for file in os.listdir(data_dir)
        if file.endswith((".json", ".xpt"))
    ]
    rules = [
        file
        for file in os.listdir(rule_dir)
        if os.path.isfile(os.path.join(rule_dir, file))
    ]

    # Execute each rule on each dataset
    for dataset_path in data_files:
        for rule in rules:
            time_taken = []
            preprocessing_times = []
            all_operator_times = {}
            all_operation_times = {}

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

                    preprocessing_time = extract_preprocessing_time_from_logs(
                        output_lines
                    )
                    operator_times = extract_operator_times(output_lines)
                    operation_times = extract_operation_times_from_logs(output_lines)

                    if process.returncode == 0:
                        time_taken.append(end_time - start_time)
                        preprocessing_times.append(preprocessing_time)

                        for op, durations in operator_times.items():
                            all_operator_times.setdefault(op, []).extend(durations)
                        for op, duration in operation_times.items():
                            all_operation_times.setdefault(op, []).append(duration)
                    else:
                        raise subprocess.CalledProcessError(
                            process.returncode, command, stderr
                        )

                except subprocess.CalledProcessError as e:
                    results.append(
                        {
                            "function type": "TimeTestFunction",
                            "rule name": rule,
                            "dataset": os.path.basename(dataset_path),
                            "status": "Failed",
                            "Number of Calls": num_call,
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

            if time_taken:
                results.append(
                    {
                        "function type": "TimeTestFunction",
                        "rule name": rule,
                        "dataset": os.path.basename(dataset_path),
                        "status": "Successful",
                        "Number of Calls": total_calls,
                        "Mean Time": sum(time_taken) / len(time_taken),
                        "Median Time": median(time_taken),
                        "Min Time": min(time_taken),
                        "Max Time": max(time_taken),
                        "Preprocessing Time": (
                            ", ".join(map(str, preprocessing_times))
                            if preprocessing_times
                            else None
                        ),
                        "Operator Times": {
                            op: durations
                            for op, durations in all_operator_times.items()
                        },
                        "Operation Times": {
                            op: durations
                            for op, durations in all_operation_times.items()
                        },
                        "Error": None,
                    }
                )

    return results


# Main execution
@click.command()
@click.option("-dd", type=str)
@click.option("-rd", type=str)
@click.option("-total_calls", type=int)
@click.option(
    "-od",
    default=os.getcwd(),
    help="Directory to save the output file (default is current directory)",
)
def main(dd, rd, total_calls, od):
    total_time_start = time.time()

    test_results = TimeTestFunction(dd, rd, total_calls)

    total_time = time.time() - total_time_start

    # Create a DataFrame and save to Excel/JSON
    results_df = pd.DataFrame(test_results)

    # Add total execution time
    total_time_row = (
        ["Total Time"] + [None] * (len(results_df.columns) - 2) + [total_time]
    )
    results_df.loc[len(results_df)] = total_time_row

    output_path = os.path.join(od, "rule_execution_report.xlsx")
    results_df.to_excel(output_path, index=False)
    results_df.to_json(os.path.join(od, "rule_execution_report.json"))
    print(f"\nExecution results saved to '{output_path}'")
    print(results_df)


if __name__ == "__main__":
    main()
