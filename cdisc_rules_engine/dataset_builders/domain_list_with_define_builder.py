from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class DomainListWithDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a dataframe with one row per dataset in Define-XML.

        Columns:
        - domain: The domain name
        - filename: The file name if the dataset exists, None otherwise
        - define_dataset_name
        - define_dataset_label
        - define_dataset_location
        - define_dataset_domain
        - define_dataset_class
        - define_dataset_structure
        - define_dataset_is_non_standard
        - define_dataset_has_no_data
        - define_dataset_key_sequence
        - define_dataset_variables

        Dataset example:
           domain  filename  define_dataset_name  define_dataset_has_no_data
        0  AE      ae.xpt    AE                   False
        1  EC      ec.xpt    EC                   False
        2  SE      None      SE                   True
        """
        domain_files = {ds.unsplit_name: ds.filename for ds in self.datasets}
        all_define_metadata = self.get_define_metadata()
        records = []
        for define_item in all_define_metadata:
            domain_name = define_item.get("define_dataset_name", "")
            record = {
                "domain": domain_name,
                "filename": domain_files.get(domain_name),
                **define_item,
            }
            records.append(record)

        return self.dataset_implementation.from_records(records)
