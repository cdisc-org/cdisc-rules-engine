import asyncio
import json
import logging
import os
import pickle
from datetime import datetime
from multiprocessing import freeze_support
from typing import Iterable, Tuple


from cdisc_rules_engine.config import config
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.models.test_args import TestArgs
from scripts.test_rule import test as test_rule
from scripts.run_validation import run_validation
from cdisc_rules_engine.services.cache.cache_populator_service import CachePopulator
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.utilities.utils import (
    generate_report_filename,
    get_rules_cache_key,
)
from scripts.list_dataset_metadata_handler import list_dataset_metadata_handler
from version import __version__



import pandas as pd
import json
import numpy as np
import yaml
from openpyxl import load_workbook

def rename_keys(item):
    if isinstance(item, dict):
        new_dict = {}
        for k, v in item.items():
            if ' ' in k:
                new_key = k.replace(' ', '_')
            else:
                new_key = k
            new_dict[new_key] = rename_keys(v)
        return new_dict
    elif isinstance(item, list):
        return [rename_keys(elem) for elem in item]
    else:
        return item
    

def Yamls_to_json(directory_path,output_path):

    # Iterate through all the files in the directory
    for filename in os.listdir(directory_path):
        # Get the full path of the file by joining the directory and the filename
        print("Converting file: " + filename)
        file_path = os.path.join(directory_path, filename)
        yaml_to_json(file_path,output_path)





def yaml_to_json(file_path,output_path):

    with open(file_path, 'r') as f:
        yaml_data = yaml.safe_load(f)
    rule_id=yaml_data["Core"]["Id"]
    yaml_data=rename_keys(yaml_data)

            
    with open(fr"{output_path}\{rule_id}.json", "w") as file:
        json.dump(yaml_data,file, indent=4)




def xpt_to_json(path,output_path):

    df = pd.read_sas(path,encoding='utf-8')  
    data = df.to_dict() 
    domain= df.iloc[0]['DOMAIN']        

    result ={"datasets": [
            {
                "filename":domain.lower()+".xpt",
                "domain":domain,
                "variables": [
                    {
                        "name": col,
                        "label": df[col].name,
                        "type":str(df[col].dtype),
                        "length": str(df[col].astype(str).str.len().max())
                    } for col in df.columns
                ],
                "records":{col: list(data[col].values()) for col in df.columns}
            }
        ]}

    with open(fr'{output_path}\{domain}.json', 'w') as file:
        json.dump(result, file)




def xpts_to_json(path):
    datasets={"datasets":[]}
    for filename in os.listdir(path):
    # Get the full path of the file by joining the directory and the filename
        if filename.endswith(".xpt"):
            file_path = os.path.join(path, filename)
            df = pd.read_sas(file_path,encoding='utf-8')  
            data = df.to_dict() 
            filename=filename.replace(".xpt","").upper()
            domain= filename      
            result = {
                        "filename":domain.lower()+".xpt",
                        "domain":domain,
                        "variables": [
                            {
                                "name": col,
                                "label": df[col].name,
                                "type":str(df[col].dtype),
                                "length": str(df[col].astype(str).str.len().max())
                            } for col in df.columns
                        ],
                        "records":{col: list(data[col].values()) for col in df.columns}
                    }
                
            datasets["datasets"].append(result)

    with open(fr'{path}\datasets.json', 'w') as file:
        json.dump(datasets, file)





def excel_to_JSON(path,sheet,output_path):
    domain=sheet[:-4].upper()
    df = pd.read_excel(path,sheet_name=sheet, na_values=["NA"], keep_default_na=False)

    df = df.replace({float("nan"): "NA"})
    df2 = df.iloc[3:]
    data = df.to_dict()
    data2 = df2.to_dict()
    # format the dictionary
    result = {"datasets": [
        {
            "filename": sheet,
            "domain": domain,
            "variables": [
                {
                    "name": col,
                    "label": data[col][0],
                    "type": data[col][1],
                    "length": data[col][2]
                } for col in df.columns
            ],
            "records":{col: list(data2[col].values()) for col in df.columns}
        }
    ]}

    with open(fr'{output_path}\{domain}.json', 'w') as file:
        json.dumps(result, file)

def csvs_to_JSON(path_dir):
    datasets={"datasets":[]}
    for filename in os.listdir(path_dir):
    # Get the full path of the file by joining the directory and the filename
        if filename.endswith(".csv"):
            file_path = os.path.join(path_dir, filename)
            df = pd.read_csv(file_path, na_values=["NA"], keep_default_na=False)
          
            data = df.to_dict()
            filename=filename.replace(".csv","").upper()
            domain= filename
            # format the dictionary
            result = {
                    "filename": filename+".xpt",
                    "domain": domain,
                    "variables": [
                        {
                            "name": col,
                            "label": df[col].name,
                            "type":str(df[col].dtype),
                            "length": str(df[col].astype(str).str.len().max())
                        } for col in df.columns
                    ],
                    "records":{col: list(data[col].values()) for col in df.columns}
                }
            datasets["datasets"].append(result)
    

    with open(fr'{path_dir}\datasets.json', 'w') as file:
        json.dump(datasets, file)




def test(
    cache_path,
    dataset_path,
    standard="sdtmig", 
    version="3.3",
    controlled_terminology_package="",
    define_version="",
    whodrug="",
    meddra="",
    rule="",
    define_xml_path="",
    validate_xml="N"
):





    args = TestArgs(
        cache_path,
        dataset_path,
        rule,
        standard,
        version,
        whodrug,
        meddra,
        controlled_terminology_package,
        define_version,
        define_xml_path,
        validate_xml,
    )
    test_rule(args)



def validate(
    ctx,
    cache: str,
    pool_size: int,
    data: str,
    dataset_path: Tuple[str],
    log_level: str,
    report_template: str,
    standard: str,
    version: str,
    controlled_terminology_package: Tuple[str],
    output: str,
    output_format: Tuple[str],
    raw_report: bool,
    define_version: str,
    whodrug: str,
    meddra: str,
    rules: Tuple[str],
    progress: str,
):
    """
    Validate data using CDISC Rules Engine

    Example:

    python core.py -s SDTM -v 3.4 -d /path/to/datasets
    """

    # Validate conditional options
    logger = logging.getLogger("validator")
    if raw_report is True:
        if not (len(output_format) == 1 and output_format[0] == ReportTypes.JSON.value):
            logger.error(
                "Flag --raw-report can be used only when --output-format is JSON"
            )
            ctx.exit()

    cache_path: str = f"{os.path.dirname(__file__)}/{cache}"

    if data:
        if dataset_path:
            logger.error(
                "Argument --dataset-path cannot be used together with argument --data"
            )
            ctx.exit()
        dataset_paths: Iterable[str] = [
            f"{data}/{fn}"
            for fn in os.listdir(data)
            if fn.lower() != DEFINE_XML_FILE_NAME
        ]
    elif dataset_path:
        if data:
            logger.error(
                "Argument --dataset-path cannot be used together with argument --data"
            )
            ctx.exit()
        dataset_paths: Iterable[str] = dataset_path
    else:
        logger.error(
            "You must pass one of the following arguments: --dataset-path, --data"
        )
        # no need to define dataset_paths here, the program execution will stop
        ctx.exit()

    run_validation(
        Validation_args(
            cache_path,
            pool_size,
            dataset_paths,
            log_level,
            report_template,
            standard,
            version,
            set(controlled_terminology_package),  # avoiding duplicates
            output,
            set(output_format),  # avoiding duplicates
            raw_report,
            define_version,
            whodrug,
            meddra,
            rules,
            progress,
        )
    )


def get_latest_file(path):
    files = os.listdir(path)
    paths = [os.path.join(path, file) for file in files]
    latest_file = max(paths, key=os.path.getmtime)
    return latest_file


def run_validation(datasets_path):
    freeze_support()

    xpts_to_json(datasets_path)
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct an absolute path relative to the script directory
    yaml_path = os.path.join(script_dir, r'Yaml_Rules')
    json_path = os.path.join(script_dir, r'Jsons_Rules')
    Yamls_to_json(yaml_path,json_path)
   
    t=test(cache_path = DefaultFilePaths.CACHE.value,dataset_path= fr"{datasets_path}\datasets.json", rule=r"Jsons_Rules")
    report=get_latest_file(datasets_path+"\Reports")
    Added_rules = pd.read_excel(r'resources\templates\Added_rules.xlsx')
    with pd.ExcelWriter(report, engine='openpyxl', mode='a') as writer:
        writer.book = load_workbook(report)
        Added_rules.to_excel(writer, sheet_name='Manual_Review', index=False)
    return report




if __name__ == "__main__":
    freeze_support()

    dataset_path=r"C:\Users\ariel\Desktop\cdisc\data\data.json"
    rule_path=r"C:\Users\ariel\Desktop\cdisc\data\CG0028.json"

    t=test(cache_path = DefaultFilePaths.CACHE.value,dataset_path= dataset_path, rule=rule_path)


    #a=validate(cache=DefaultFilePaths.CACHE.value, pool_size=1, data=r"C:\Users\ariel\Desktop\Tests\test data bioforum rules", dataset_path=None, log_level="INFO", report_template=None, standard="SDTM", version="3.4", controlled_terminology_package=None, output=r"C:\Users\ariel\Desktop\Tests\test data bioforum rules\output", output_format=["JSON"], raw_report=False, define_version=None, whodrug=None, meddra=None, rules=None, progress="bar")