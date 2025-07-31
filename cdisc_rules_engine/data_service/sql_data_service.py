from abc import ABC, abstractmethod
from pathlib import Path

from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.utilities.ig_specification import IGSpecification


class SQLDataService(ABC):

    def __init__(
        self,
        ig_specs: IGSpecification,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        codelists_path: Path = None,
        metadata_standards_path: Path = None,
        terminology_paths: dict = None,
    ):
        """
        Initialize the data service.

        Parameters:
        - datasets_path: Path to the folder containing datasets.
        - define_xml_path: Path to the Define-XML file.
        - terminology_paths: A dictionary with keys:
            'whodrug', 'loinc', 'medrt', 'meddra', 'unii',
            each mapped to a Path representing the respective folder.
        """
        self.datasets_path = datasets_path
        self.define_xml_path = define_xml_path
        self.ig_specs = ig_specs
        self.codelists_path = codelists_path
        self.metadata_standards_path = metadata_standards_path
        self.terminology_paths = terminology_paths

        self._create_sql_tables_from_dataset_paths()
        self._create_definexml_tables()
        self._create_codelist_tables()
        self._create_standards_tables()
        self._create_terminology_tables()

    @abstractmethod
    def from_list_of_testdatasets(cls, test_datasets: list[TestDataset]) -> None:
        """
        Constructor for tests, passing in list of TestDataset
        and create corresponding SQL tables (content and metadata)
        """
        pass

    @abstractmethod
    def _create_sql_tables_from_dataset_paths(self) -> None:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        # run XPTReader

        # write contents table to database

        # write dataset and variable metadata tables to database
        pass

    @abstractmethod
    def _create_definexml_tables(self) -> None:
        """
        Read the self.define_xml_path and create corresponding SQL tables.
        """
        # run DefineXMLReader

        # write table to database
        pass

    @abstractmethod
    def _create_standards_tables(self) -> None:
        """
        Create all necessary SQL tables for IG standards.
        """
        # for each IG type (sdtm, adam) in standards:

        #       query available data model and see which IG metadata tables already exist in the database

        #       for every IG metadata version that is not in the database, run CodelistReader

        #       write table to database
        pass

    @abstractmethod
    def _create_codelist_tables(self) -> None:
        """
        Create all necessary SQL tables for CDISC codelists.
        """
        # query available data model and see which codelist tables already exist in the database

        # for every codelist that is not in the database, run CodelistReader

        # write table to database
        pass

    @abstractmethod
    def _create_terminology_tables(self) -> None:
        """
        Iterate through self.terminology_paths dict
        and create corresponding SQL tables if paths exist.
        """
        # for each terminology in self.terminology_paths:

        #       query available data model and see which terminology tables already exist in the database

        #       for every terminology that is not in the database, run TerminologyReader subclass for terminology

        #       write table to database
        pass
