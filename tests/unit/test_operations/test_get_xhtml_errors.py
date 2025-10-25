from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import SchemaNotFoundError
from cdisc_rules_engine.models.dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.get_xhtml_errors import GetXhtmlErrors


@pytest.mark.parametrize(
    "target_column, dataset, namespace, expected",
    [
        (  # column is not in the dataset
            "target",
            PandasDataset.from_records(
                [
                    {"column": ""},
                    {"column": ""},
                    {"column": ""},
                ]
            ),
            "http://www.cdisc.org/ns/usdm/xhtml/v1.0",
            KeyError,
        ),
        (  # schema violation
            "target",
            PandasDataset.from_records(
                [
                    {
                        "target": '<div xmlns="http://www.w3.org/1999/xhtml"><p>Table LZZT.1 lists the clinical laboratory tests that will be performed at Visit 1.</p>\r\n<p><b>Table LZZT.1. Laboratory Tests Performed at Admission (Visit 1)</b></p>\r\n<style>\r\n  ul.no-bullets {\r\n    list-style-type: none;\r\n    margin: 0;\r\n    padding: 5;\r\n  }\r\n</style>\r\n<p><b>Safety Laboratory Tests</b></p>\r\n<table class="table">\r\n<tr>\r\n<td>\r\n<p><b>Hematology:</b></p>\r\n<p><ul class="no-bullets">\r\n<li>Hemoglobin</li>\r\n<li>Hematocrit</li>\r\n<li>Erythrocyte count (RBC)</li>\r\n<li>Mean cell volume (MCV)</li>\r\n<li>Mean cell hemoglobin (MCH) </li>\r\n<li>Mean cell hemoglobin concentration (MCHC) </li>\r\n<li>Leukocytes (WBC)</li>\r\n<li>Neutrophils, segmented </li>\r\n<li>Neutrophils, juvenile (bands)</li>\r\n<li>Lymphocytes </li>\r\n<li>Monocytes </li>\r\n<li>Eosinophils </li>\r\n<li>Basophils </li>\r\n<li>Platelet </li>\r\n<li>Cell morphology</li>\r\n</ul></p>\r\n<p><b>Urinalysis:</b></p>\r\n<p><ul class="no-bullets">\r\n<li>Color </li>\r\n<li>Specific gravity</li>\r\n<li>pH</li>\r\n<li>Protein</li>\r\n<li>Glucose</li>\r\n<li>Ketones</li>\r\n<li>Bilirubin</li>\r\n<li>Urobilinogen</li>\r\n<li>Blood </li>\r\n<li>Nitrite</li>\r\n<li>Microscopic examination of sediment</li>\r\n</ul></p>\r\n</td>\r\n<td>\r\n<p><b>Clinical Chemistry - Serum Concentration of:</b></p>\r\n<p><ul class="no-bullets">\r\n<li>Sodium</li>\r\n<li>Potassium</li>\r\n<li>Bicarbonate</li>\r\n<li>Total bilirubin </li>\r\n<li>Alkaline phosphatase (ALP) </li>\r\n<li>Gamma-glutamyl transferase (GGT) </li>\r\n<li>Alanine transaminase (ALT/SGPT) </li>\r\n<li>Aspartate transaminase (AST/SGOT) </li>\r\n<li>Blood urea nitrogen (BUN) </li>\r\n<li>Serum creatinine </li>\r\n<li>Uric acid </li>\r\n<li>Phosphorus </li>\r\n<li>Calcium </li>\r\n<li>Glucose, nonfasting </li>\r\n<li>Total protein </li>\r\n<li>Albumin </li>\r\n<li>Cholesterol </li>\r\n<li>Creatine kinase (CK)</li>\r\n</ul></p>\r\n<p><b>Thyroid Function Test (Visit 1 only):</b></p>\r\n<ul class="no-bullets">\r\n<li>Free thyroid index</li>\r\n<li>T<sub>3</sub> Uptake</li>\r\n<li>T<sub>4</sub></li>\r\n<li>Thyroid-stimulating hormone (TSH)</li>\r\n</ul>\r\n<p><b>Other Tests (Visit 1 only):</b></p>\r\n<ul class="no-bullets">\r\n<li>Folate</li>\r\n<li>Vitamin B 12</li>\r\n<li>Syphilis screening</li>\r\n<li>Hemoglobin A<sup>1C</sup> (IDDM patients only)</li></ul>\r\n</td>\r\n</tr>\r\n</table>\r\n<p>Laboratory values that fall outside a clinically accepted reference range or values that differ significantly from previous values must be evaluated and commented on by the investigator by marking CS (for clinically significant) or NCS (for not clinically significant) next to the values. Any clinically significant laboratory values that are outside a clinically acceptable range or differ importantly from a previous value should be further commented on in the clinical report form comments page.</p>\r\n<p>Hematology, and clinical chemistry will also be performed at Visits 4, 5, 7, 8, 9, 10, 11, 12, and 13. Patients that experience a rash and/or eosinophilia may have additional hematology samples obtained as described in 3.9.3.4 (Other Safety Measures).</p>\r\n<p>Urinalysis will also be performed at Visits 4, 9, and 12. The following criteria have been developed to monitor hepatic function.</p>\r\n<ul>\r\n<li><p>Patients with ALT/SGPT levels &gt;120 IU will be retested weekly.</p>\r\n<li><p>Patients with ALT/SGPT values &gt;400 IU, or alternatively, an elevated ALT/SGPT accompanied by GGT and/or ALP values &gt;500 IU will be retested within 2 days. The sponsor\'s clinical research administrator or clinical research physician is to be notified. If the retest value does not decrease by at least 10%, the study drug will be discontinued; additional laboratory tests will be performed until levels return to normal. If the retest value does decrease by 10% or more, the study drug may be continued with monitoring at 3 day intervals until ALT/SGPT values decrease to &lt;400 IU or GGT and/or ALP values decrease to &lt;500 IU. \r\n</p></li></li></ul></div>'  # noqa: E501
                    },
                ]
            ),
            "http://www.cdisc.org/ns/usdm/xhtml/v1.0",
            [
                [
                    "Invalid XHTML line 3 [ERROR]: Element 'style': This element is not expected."
                ]
            ],
        ),
        (  # valid dataset
            "target",
            PandasDataset.from_records(
                [
                    {
                        "target": '<div xmlns="http://www.w3.org/1999/xhtml"><p>Table LZZT.1 lists the clinical laboratory tests that will be performed at Visit 1.</p>\r\n<p><b>Table LZZT.1. Laboratory Tests Performed at Admission (Visit 1)</b></p>\r\n<p><b>Safety Laboratory Tests</b></p>\r\n<table class="table">\r\n<tr>\r\n<td>\r\n<p><b>Hematology:</b></p>\r\n<p></p>\r\n<p><b>Urinalysis:</b></p>\r\n<p></p>\r\n</td>\r\n<td>\r\n<p><b>Clinical Chemistry - Serum Concentration of:</b></p>\r\n<p></p>\r\n<p><b>Thyroid Function Test (Visit 1 only):</b></p>\r\n<p><b>Other Tests (Visit 1 only):</b></p>\r\n</td>\r\n</tr>\r\n</table>\r\n<p>Laboratory values that fall outside a clinically accepted reference range or values that differ significantly from previous values must be evaluated and commented on by the investigator by marking CS (for clinically significant) or NCS (for not clinically significant) next to the values. Any clinically significant laboratory values that are outside a clinically acceptable range or differ importantly from a previous value should be further commented on in the clinical report form comments page.</p>\r\n<p>Hematology, and clinical chemistry will also be performed at Visits 4, 5, 7, 8, 9, 10, 11, 12, and 13. Patients that experience a rash and/or eosinophilia may have additional hematology samples obtained as described in 3.9.3.4 (Other Safety Measures).</p>\r\n<p>Urinalysis will also be performed at Visits 4, 9, and 12. The following criteria have been developed to monitor hepatic function.</p>\r\n</div>'  # noqa: E501
                    },
                ]
            ),
            "http://www.cdisc.org/ns/usdm/xhtml/v1.0",
            [[]],
        ),
        (  # invalid namespace
            "target",
            PandasDataset.from_records(
                [
                    {
                        "target": '<div xmlns="http://www.w3.org/1999/xhtml"><p>Table LZZT.1 lists the clinical laboratory tests that will be performed at Visit 1.</p>\r\n<p><b>Table LZZT.1. Laboratory Tests Performed at Admission (Visit 1)</b></p>\r\n<p><b>Safety Laboratory Tests</b></p>\r\n<table class="table">\r\n<tr>\r\n<td>\r\n<p><b>Hematology:</b></p>\r\n<p></p>\r\n<p><b>Urinalysis:</b></p>\r\n<p></p>\r\n</td>\r\n<td>\r\n<p><b>Clinical Chemistry - Serum Concentration of:</b></p>\r\n<p></p>\r\n<p><b>Thyroid Function Test (Visit 1 only):</b></p>\r\n<p><b>Other Tests (Visit 1 only):</b></p>\r\n</td>\r\n</tr>\r\n</table>\r\n<p>Laboratory values that fall outside a clinically accepted reference range or values that differ significantly from previous values must be evaluated and commented on by the investigator by marking CS (for clinically significant) or NCS (for not clinically significant) next to the values. Any clinically significant laboratory values that are outside a clinically acceptable range or differ importantly from a previous value should be further commented on in the clinical report form comments page.</p>\r\n<p>Hematology, and clinical chemistry will also be performed at Visits 4, 5, 7, 8, 9, 10, 11, 12, and 13. Patients that experience a rash and/or eosinophilia may have additional hematology samples obtained as described in 3.9.3.4 (Other Safety Measures).</p>\r\n<p>Urinalysis will also be performed at Visits 4, 9, and 12. The following criteria have been developed to monitor hepatic function.</p>\r\n</div>'  # noqa: E501
                    },
                ]
            ),
            "invalid-namespace",
            SchemaNotFoundError,
        ),
    ],
)
def test_get_xhtml_errors(
    target_column: str,
    dataset: PandasDataset,
    namespace: str,
    expected: Any,
    operation_params: OperationParams,
):
    operation_id = "$get_xhtml_errors"
    operation_params.operation_id = operation_id
    operation_params.namespace = namespace
    operation = GetXhtmlErrors(
        params=operation_params,
        original_dataset=dataset,
        cache_service=MagicMock(),
        data_service=MagicMock(),
    )
    try:
        result = operation._execute_operation()
    except Exception as e:
        result = pd.Series(type(e))
    assert result.equals(pd.Series(expected))
