import pandas as pd

from cdisc_rules_engine.dummy_models.dummy_variable import DummyVariable


class DummyDataset:
    def __init__(self, dataset_data):

        if "clinicalData" in dataset_data or "referenceData" in dataset_data:
            if "clinicalData" in dataset_data:
                _data_key = "clinicalData"
            elif "referenceData" in dataset_data:
                _data_key = "referenceData"

            _items_data = next(
                (
                    d
                    for d in dataset_data[_data_key]["itemGroupData"].values()
                    if "items" in d
                ),
                {},
            )

            self.name = _items_data.get("name")
            self.label = _items_data.get("label")
            self.filesize = _items_data.get("records")
            self.filename = _items_data.get("name") + ".json"
            _domain_index = next(
                (
                    index
                    for index, item in enumerate(_items_data["items"])
                    if item.get("name") == "DOMAIN"
                ),
                None,
            )
            if _domain_index:
                self.domain = _items_data["itemData"][0][_domain_index]
            else:
                self.domain = ""



            """
            self.domain = _items_data["itemData"][0][
                next(
                    (
                        index
                        for index, item in enumerate(_items_data["items"])
                        if item.get("name") == "DOMAIN"
                    ),
                    None,
                )
            ]
            """
            self.variables = [
                DummyVariable(variable_data)
                for variable_data in _items_data.get("items", [])[1:]
            ]
            self.data = pd.DataFrame(
                [item[1:] for item in _items_data.get("itemData", [])],
                columns=[item["name"] for item in _items_data.get("items", [])[1:]],
            )
            self.data = self.data.applymap(
                lambda x: round(x, 15) if isinstance(x, float) else x
            )

        else:
            self.name = dataset_data.get("name")
            self.label = dataset_data.get("label")
            self.filesize = dataset_data.get("filesize")
            self.filename = dataset_data.get("filename")
            self.domain = dataset_data.get("domain")
            self.variables = [
                DummyVariable(variable_data)
                for variable_data in dataset_data.get("variables", [])
            ]
            self.data = pd.DataFrame.from_dict(dataset_data.get("records", {}))

    def get_metadata(self):
        return {
            "dataset_size": [self.filesize or 1000],
            "dataset_name": [
                self.filename.split(".")[0].upper() if self.filename else "test"
            ],
            "dataset_label": [self.label or "test"],
            "filename": [self.filename],
            "length": [len(self.data.index)],
        }
