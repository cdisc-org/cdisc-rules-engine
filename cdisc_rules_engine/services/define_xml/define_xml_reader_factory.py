import os
import re
from pathlib import Path
from re import compile
from typing import Union
from xml.etree import ElementTree

from cdisc_rules_engine.constants.define_xml_constants import (
    DEFINE_XML_FILE_NAME,
    ODM_NAMESPACE,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_0 import (
    DefineXMLReader20,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_1 import (
    DefineXMLReader21,
)
from cdisc_rules_engine.utilities.utils import get_directory_path


class DefineXMLReaderFactory:
    """
    The class has 2 constructors: from filename and
    from file contents.
    Ex. 1:
        filename = "define.xml"
        reader = DefineXMLReaderFactory.from_filename(filename)
        reader.read()

    Ex. 2:
        file_contents: bytes = b"...."
        reader = DefineXMLReaderFactory.from_file_contents(file_contents)
        reader.read()
    """

    _define_xml_readers: tuple[BaseDefineXMLReader, ...] = (
        DefineXMLReader20,
        DefineXMLReader21,
    )
    _DEFINE_VERSION_RE = re.compile(r'(?<=def:DefineVersion=")2\.1\.\d+(?=")')

    @classmethod
    def from_filename(cls, filename: str):
        logger.info(f"Reading Define-XML from file. filename={filename}")
        return cls._build_reader(Path(filename).read_text(encoding="utf-8"))

    @classmethod
    def from_file_contents(
        cls,
        file_contents: Union[str, bytes],
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
    ):
        """
        Inits a DefineXMLReader object from file contents.
        """
        logger.info("Reading Define-XML from file contents")
        if isinstance(file_contents, bytes):
            file_contents: str = file_contents.decode("utf-8")
        return cls._build_reader(
            file_contents, cache_service_obj, study_id, data_bundle_id
        )

    @classmethod
    def _normalize_define_version(cls, content: str) -> tuple[str, str | None]:
        match = cls._DEFINE_VERSION_RE.search(content)
        if match:
            return cls._DEFINE_VERSION_RE.sub("2.1", content), match.group()
        return content, None

    @classmethod
    def _build_reader(
        cls, content: str, cache_service_obj=None, study_id=None, data_bundle_id=None
    ) -> "BaseDefineXMLReader":
        content, original_version = cls._normalize_define_version(content)
        root = ElementTree.fromstring(content)
        reader_class = cls._get_define_xml_reader(root)
        reader = reader_class(cache_service_obj, study_id, data_bundle_id)
        reader._original_define_version = original_version
        reader._odm_loader.load_odm_string(content)
        return reader

    @classmethod
    def _get_define_xml_reader(
        cls, root: ElementTree.Element
    ) -> type[BaseDefineXMLReader]:
        elt = root.find(
            "Study/MetaDataVersion",
            namespaces={"": ODM_NAMESPACE},
        )
        pattern = compile(r"(\{(.*)\})?DefineVersion")
        define_version = next(
            iter(
                cls._from_namespace(match.group(2))
                for match in [pattern.fullmatch(name) for name, _ in elt.items()]
                if match
            ),
            None,
        )
        return define_version

    @classmethod
    def _from_namespace(cls, namespace: str) -> BaseDefineXMLReader:
        define_xml_reader = next(
            iter(
                define_xml_reader
                for define_xml_reader in cls._define_xml_readers
                if namespace == define_xml_reader.class_define_xml_version().namespace
            ),
            None,
        )
        return define_xml_reader

    @classmethod
    def get_define_xml_reader(
        cls, dataset_path: str, define_xml_path: str, data_service, cache
    ):
        directory_path = get_directory_path(dataset_path)
        if define_xml_path is None:
            define_xml_path: str = os.path.join(
                directory_path,
                DEFINE_XML_FILE_NAME,
            )
        define_xml_contents: bytes = data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReaderFactory.from_file_contents(
            define_xml_contents, cache_service_obj=cache
        )

        return define_xml_reader
