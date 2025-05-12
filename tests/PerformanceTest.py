import os
import sys
import time
import pandas as pd
import subprocess
import re
import click
from statistics import median


def parse_preprocessing_times(log_lines):
    start, end = None, None
    for entry in log_lines:
        if "Dataset Preprocessing Starts" in entry:
            m = re.search(r"\\ST(\d+\.\d+)", entry)
            if m:
                start = float(m.group(1))
        elif "Dataset Preprocessing Ends" in entry:
            m = re.search(r"\\ST(\d+\.\d+)", entry)
            if m:
                end = float(m.group(1))
    return end - start if (start is not None and end is not None) else 0


def parse_operator_durations(log_lines):
    ops_start, ops_duration = {}, {}
    for entry in log_lines:
        match_start = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) starts", entry)
        if match_start:
            ts, op_name = float(match_start.group(1)), match_start.group(2)
            ops_start[op_name] = ts
        match_end = re.search(r"\\OPRT(\d+\.\d+)-operator (\w+) ends", entry)
        if match_end:
            ts, op_name = float(match_end.group(1)), match_end.group(2)
            if op_name in ops_start:
                duration = ts - ops_start.pop(op_name)
                ops_duration.setdefault(op_name, []).append(duration)
    return ops_duration


def parse_operation_times(log_lines):
    operation_durations = []
    start_marker = None
    for entry in log_lines:
        if "\\OPRNT" in entry and "Operation Starts" in entry:
            start_marker = time.time()
        elif "\\OPRNT" in entry and "Operation Ends" in entry and start_marker:
            operation_durations.append(time.time() - start_marker)
    return operation_durations


def execute_rules_on_datasets(dataset_dir, rule_dir, total_calls, standard, version, define_xml_path):
    final_results = []
    per_rule_summaries = {}
    datasets = [os.path.join(dataset_dir, f) for f in os.listdir(dataset_dir) if f.endswith(('.json', '.xpt'))]
    rules = [f for f in os.listdir(rule_dir) if os.path.isfile(os.path.join(rule_dir, f))]

    for rule in rules:
        rule_name = os.path.basename(rule)
        all_timings, all_preproc_times, all_operator_metrics, all_oprnt_metrics = [], [], {}, []

        for dataset_path in datasets:
            dataset_name = os.path.basename(dataset_path)
            timings, preproc_list, operator_data, oprnt_data = [], [], {}, []
            exec_count = 0

            for call_idx in range(total_calls):
                cmd = [
                    sys.executable, "core.py", "validate",
                    "-s", standard,
                    "-v", version,
                    "-lr", os.path.join(rule_dir, rule),
                    "-dp", dataset_path,
                    "-l", "critical"
                ]
                if define_xml_path:
                    cmd += ["-dxp", define_xml_path]

                print(f"Running: {' '.join(cmd)} [call {call_idx + 1}]")

                try:
                    t0 = time.time()
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    out, err = process.communicate()
                    t1 = time.time()

                    log_lines = err.splitlines()
                    preproc_time = parse_preprocessing_times(log_lines)
                    operator_times = parse_operator_durations(log_lines)
                    oprnt_times = parse_operation_times(log_lines)

                    if process.returncode == 0:
                        timings.append(t1 - t0)
                        preproc_list.append(preproc_time)
                        exec_count += 1

                        for op, durations in operator_times.items():
                            all_operator_metrics.setdefault(op, []).extend(durations)

                        all_oprnt_metrics.extend(oprnt_times)
                    else:
                        raise subprocess.CalledProcessError(process.returncode, cmd, err)

                except subprocess.CalledProcessError as exc:
                    final_results.append({
                        "function type": "TimeTestFunction",
                        "rule name": rule_name,
                        "dataset": dataset_name,
                        "status": "Failed",
                        "Number of Calls": call_idx + 1,
                        "Mean Time": None, "Median Time": None,
                        "Min Time": None, "Max Time": None,
                        "Preprocessing Time": None,
                        "Operator Times": None,
                        "Operation Times": None,
                        "Error": exc.stderr
                    })
                    break

            if timings:
                all_timings.extend(timings)
                all_preproc_times.extend(preproc_list)
                per_rule_summaries.setdefault(rule_name, []).append({
                    "Dataset": dataset_name,
                    "Number of Calls": exec_count,
                    "Mean Time": sum(timings) / len(timings),
                    "Median Time": median(timings),
                    "Min Time": min(timings),
                    "Max Time": max(timings),
                    "Preprocessing Time": ", ".join(map(str, preproc_list)),
                    "Operator Times": ", ".join(f"{k}: {v}" for k, v in operator_times.items()),
                    "Operation Times": ", ".join(map(str, oprnt_times)),
                })

        if all_timings:
            final_results.append({
                "function type": "TimeTestFunction",
                "rule name": "All Rules Combined",
                "dataset": dataset_name,
                "status": "Successful",
                "Number of Calls for each rule": total_calls,
                "Mean Time": sum(all_timings) / len(all_timings),
                "Median Time": median(all_timings),
                "Min Time": min(all_timings),
                "Max Time": max(all_timings),
                "Preprocessing Time": ", ".join(map(str, all_preproc_times)),
                "Operator Times": all_operator_metrics,
                "Operation Times": ", ".join(map(str, all_oprnt_metrics)),
                "Error": None,
            })

    return final_results, per_rule_summaries


def execute_datasets_on_rules(dataset_dir, rule_dir, total_calls, standard, version, define_xml_path):
    final_results = []
    per_dataset_summaries = {}
    datasets = [os.path.join(dataset_dir, f) for f in os.listdir(dataset_dir) if f.endswith(('.json', '.xpt'))]
    rules = [f for f in os.listdir(rule_dir) if os.path.isfile(os.path.join(rule_dir, f))]

    for dataset_path in datasets:
        dataset_name = os.path.basename(dataset_path)
        cumulative_times, cumulative_preproc, cumulative_ops, cumulative_oprnt = [], [], {}, []

        for rule in rules:
            rule_name = os.path.basename(rule)
            timings, preproc_list, operator_data, oprnt_data = [], [], {}, []
            exec_count = 0

            for call_idx in range(total_calls):
                cmd = [
                    sys.executable, "core.py", "validate",
                    "-s", standard,
                    "-v", version,
                    "-lr", os.path.join(rule_dir, rule),
                    "-dp", dataset_path,
                    "-l", "critical"
                ]
                if define_xml_path:
                    cmd += ["-dxp", define_xml_path]

                print(f"Running: {' '.join(cmd)} [call {call_idx + 1}]")

                try:
                    t0 = time.time()
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    out, err = process.communicate()
                    t1 = time.time()

                    log_lines = err.splitlines()
                    preproc_time = parse_preprocessing_times(log_lines)
                    operator_times = parse_operator_durations(log_lines)
                    oprnt_times = parse_operation_times(log_lines)

                    if process.returncode == 0:
                        timings.append(t1 - t0)
                        preproc_list.append(preproc_time)
                        exec_count += 1

                        for op, durations in operator_times.items():
                            cumulative_ops.setdefault(op, []).extend(durations)

                        cumulative_oprnt.extend(oprnt_times)
                    else:
                        raise subprocess.CalledProcessError(process.returncode, cmd, err)

                except subprocess.CalledProcessError as exc:
                    final_results.append({
                        "function type": "TimeTestFunction",
                        "rule name": rule_name,
                        "dataset": dataset_name,
                        "status": "Failed",
                        "Number of Calls": call_idx + 1,
                        "Mean Time": None, "Median Time": None,
                        "Min Time": None, "Max Time": None,
                        "Preprocessing Time": None,
                        "Operator Times": None,
                        "Operation Times": None,
                        "Error": exc.stderr
                    })
                    break

            if timings:
                cumulative_times.extend(timings)
                cumulative_preproc.extend(preproc_list)
                per_dataset_summaries.setdefault(dataset_name, []).append({
                    "Dataset": dataset_name,
                    "Rule Name": rule_name,
                    "Number of Calls": exec_count,
                    "Mean Time": sum(timings) / len(timings),
                    "Median Time": median(timings),
                    "Min Time": min(timings),
                    "Max Time": max(timings),
                    "Preprocessing Time": ", ".join(map(str, preproc_list)),
                    "Operator Times": ", ".join(f"{k}: {v}" for k, v in operator_times.items()),
                    "Operation Times": ", ".join(map(str, oprnt_times)),
                })

        if cumulative_times:
            final_results.append({
                "function type": "TimeTestFunction",
                "rule name": rule_name,
                "dataset": "All datasets combined",
                "status": "Successful",
                "Number of Calls for each rule": total_calls,
                "Mean Time": sum(cumulative_times) / len(cumulative_times),
                "Median Time": median(cumulative_times),
                "Min Time": min(cumulative_times),
                "Max Time": max(cumulative_times),
                "Preprocessing Time": ", ".join(map(str, cumulative_preproc)),
                "Operator Times": cumulative_ops,
                "Operation Times": ", ".join(map(str, cumulative_oprnt)),
                "Error": None,
            })

    return final_results, per_dataset_summaries


def TimeTestFunction(dataset_dir, rule_dir, total_calls, standard, version, define_xml_path):
    print("Running grouped by rule...")
    rule_level_results, rule_breakdown = execute_rules_on_datasets(dataset_dir, rule_dir, total_calls, standard, version, define_xml_path)
    print("\nRunning grouped by dataset...")
    dataset_level_results, dataset_breakdown = execute_datasets_on_rules(dataset_dir, rule_dir, total_calls, standard, version, define_xml_path)
    return rule_level_results, rule_breakdown, dataset_level_results, dataset_breakdown

def save_results_to_excel(rule_results, rule_breakdown, dataset_results, dataset_breakdown, output_dir):
    report_path = os.path.join(output_dir, "performance_report.xlsx")
    with pd.ExcelWriter(report_path, engine='xlsxwriter') as writer:
        # Sheet 1: Rule-level results
        df_rules = pd.DataFrame(rule_results)
        df_rules.to_excel(writer, sheet_name="Rule Level Results", index=False)

        # Each dataset gets its own sheet
        for dataset_name, summaries in dataset_breakdown.items():
            df_dataset = pd.DataFrame(summaries)
            # Clean sheet name (Excel limits to 31 characters)
            safe_name = dataset_name[:31].replace("/", "_").replace("\\", "_")
            df_dataset.to_excel(writer, sheet_name=safe_name, index=False)

        # Optionally, you could add an overview of dataset-level summary
        df_dataset_summary = pd.DataFrame(dataset_results)
        df_dataset_summary.to_excel(writer, sheet_name="Dataset Summary", index=False)

    print(f"\nResults saved to: {report_path}")


@click.command()
@click.option("-d", type=str, required=True)
@click.option("-lr", type=str, required=True)
@click.option("-total_calls", type=int, required=True)
@click.option("-od", default=os.getcwd(), help="Output directory for report files")
@click.option("-s", "standard", type=str, required=True, help="Standard name (e.g., 'sdtmig')")
@click.option("-v", "version", type=str, required=True, help="Standard version (e.g., '3.4')")
@click.option("-dxp", "define_xml_path", type=str, required=False, help="Path to Define-XML file")
def main(d, lr, total_calls, od, standard, version, define_xml_path):
    rule_results, rule_breakdown, dataset_results, dataset_breakdown = TimeTestFunction(
    d, lr, total_calls, standard, version, define_xml_path
    )
    save_results_to_excel(rule_results, rule_breakdown, dataset_results, dataset_breakdown, od)



if __name__ == "__main__":
    main()
