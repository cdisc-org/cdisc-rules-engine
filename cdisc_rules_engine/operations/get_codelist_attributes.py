import pandas as pd
import logging
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService


class CodeListAttributes(BaseOperation):
    """
    A class for fetching codelist attributes for a trial summary domain.
    """

    def _execute_operation(self):
        """
        Executes the operation to fetch codelist attributes for a trial
        summary (TS) domain.

        Returns:
            pd.Series: A Series of lists containing codelist, where each list
                represents the codelist package and version.
                The length of the Series is equal to the length of the given
                dataframe.
        """
        return self._get_codelist_attributes()

    def _get_codelist_attributes(self):
        """
        Fetches codelist for a given codelist package and version from the TS
        dataset.
        Returns it as a Series of lists like:
          0    ["STUDYID", "DOMAIN", ...]
          1    ["STUDYID", "DOMAIN", ...]
          2    ["STUDYID", "DOMAIN", ...]
          ...

        pd.Series: A Series of lists containing codelist, where each list
            represents the codelist package and version.
            The length of the Series is equal to the length of the given
            dataframe.
        """

        # 1.0 get input variables
        # -------------------------------------------------------------------
        ct_name = "CT_PACKAGE"  # a column for controlled term package names
        # Get controlled term attribute column name specified in rule
        ct_attribute = self.params.ct_attribute

        # 2.0 build codelist from cache
        # -------------------------------------------------------------------
        ct_cache = self._get_ct_from_library_metadata(
            ct_key=ct_name, ct_val=ct_attribute
        )

        # 3.0 get dataset records
        # -------------------------------------------------------------------
        ct_data = self._get_ct_from_dataset(ct_key=ct_name, ct_val=ct_attribute)

        # 4.0 merge the two datasets by CC
        # -------------------------------------------------------------------
        cc_key = ct_data[ct_name].to_list()
        ct_list = ct_cache[(ct_cache[ct_name].isin(cc_key))]
        ds_len = self.params.dataframe.len()
        result = pd.Series([ct_list[ct_attribute].values[0] for _ in range(ds_len)])
        return result

    def _get_ct_from_library_metadata(self, ct_key: str, ct_val: str):
        """
        Retrieves the codelist information from the cache based on the given
        ct_key and ct_val.

        Args:
            ct_key (str): The key for identifying the codelist.
            ct_val (str): The value associated with the codelist.

        Returns:
            pd.DataFrame: A DataFrame containing the codelist information
            retrieved from the cache.
        """
        ct_packages = self.params.ct_packages
        ct_term_maps = (
            []
            if ct_packages is None
            else [
                self.library_metadata.get_ct_package_metadata(package) or {}
                for package in ct_packages
            ]
        )

        # convert codelist to dataframe
        ct_result = {ct_key: [], ct_val: []}
        ct_result = self._add_codelist(ct_key, ct_val, ct_term_maps, ct_result)

        is_contained = set(ct_packages).issubset(set(ct_result[ct_key]))
        # if all the CT packages exist in Cache, we return the result
        if is_contained:
            return pd.DataFrame(ct_result)

        # if not, we need to get them from library
        config = ConfigService()
        logger = logging.getLogger()
        api_key = config.getValue("CDISC_LIBRARY_API_KEY")
        ct_diff = list(set(ct_packages) - set(set(ct_result[ct_key])))

        cls = CDISCLibraryService(api_key, self.cache)
        ct_pkgs = cls.get_all_ct_packages()
        ct_names = [item["href"].split("/")[-1] for item in ct_pkgs]

        for ct in ct_diff:
            if ct not in ct_names:
                logger.info(f"Requested package {ct} not in CT library.")
                continue
            ct_code = cls.get_codelist_terms_map(ct)
            ct_result = self._add_codelist(ct_key, ct_val, ct_code, ct_result)
        return pd.DataFrame(ct_result)

    def _get_ct_from_dataset(self, ct_key: str, ct_val: str):
        """
        Retrieves the codelist information from the dataset based on the given
        ct_key and ct_val.

        Args:
            ct_key (str): The key for identifying the codelist.
            ct_val (str): The value associated with the codelist.

        Returns:
            pd.DataFrame: A DataFrame containing the codelist information
            retrieved from the dataset.
        """
        ct_packages = self.params.ct_packages
        # get attribute variable specified in rule
        ct_attribute = self.params.ct_attribute

        ct_target = self.params.target  # target variable specified in rule
        ct_version = self.params.ct_version  # controlled term version
        if ct_attribute == "Term CCODE":
            ct_attribute = "TSVALCD"
        sel_cols = [ct_target, ct_version, ct_attribute, ct_key]

        # get dataframe from dataset records
        df = self.params.dataframe

        # add CT_PACKAGE column
        df[ct_key] = df.data.apply(
            lambda row: "sdtmct-" + row[ct_version]
            if row[ct_target] is not None and row[ct_target] in ("CDISC", "CDISC CT")
            else row[ct_target] + "-" + row[ct_version],
            axis=1,
        )

        # select records
        df_sel = df[(df[ct_key].isin(ct_packages))].loc[:, sel_cols]

        # group the records
        result = df_sel.groupby(ct_key)[ct_attribute].unique().reset_index()
        result.rename(columns={ct_attribute: ct_val})

        return result

    def _add_codelist(self, ct_key, ct_val, ct_term_maps, ct_result):
        """
        Adds codelist information to the result dictionary.

        Args:
            ct_key (str): The key for identifying the codelist.
            ct_val (str): The value associated with the codelist.
            ct_term_maps (list[dict]): A list of dictionaries containing
                codelist information.
            ct_result (dict): The dictionary to store the codelist information.

        Returns:
            dict: The updated ct_result dictionary.
        """
        for item in ct_term_maps:
            ct_result[ct_key].append(item.get("package"))
            codes = set(code for code in item.keys() if code != "package")
            ct_result[ct_val].append(codes)
        return ct_result
