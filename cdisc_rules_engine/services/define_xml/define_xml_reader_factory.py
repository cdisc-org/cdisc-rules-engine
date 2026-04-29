import re
from pathlib import Path
from os.path import dirname, join
from re import compile
from typing import Union
from xml.etree import ElementTree

from cdisc_rules_engine.constants.define_xml_constants import (
    DEFINE_XML_FILE_NAME,
    ODM_NAMESPACE,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_0 import (
    DefineXMLReader20,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_1 import (
    DefineXMLReader21,
)
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
)


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
        logger.info("Reading Define-XML from file contents")
        return cls._build_reader(
            file_contents, cache_service_obj, study_id, data_bundle_id
        )

    @classmethod
    def _build_reader(
        cls,
        content: Union[str, bytes],
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
    ) -> "BaseDefineXMLReader":
        root = ElementTree.fromstring(content)
        reader_class, original_version = cls._get_define_xml_reader(root)

        if isinstance(content, bytes):
            content = ElementTree.tostring(root, encoding="unicode")

        if original_version:
            content = content.replace(
                f'DefineVersion="{original_version}"',
                'DefineVersion="2.1"',
            )

        reader = reader_class(cache_service_obj, study_id, data_bundle_id)
        reader._original_define_version = original_version
        reader._odm_loader.load_odm_string(content)
        return reader

    @classmethod
    def _get_define_xml_reader(
        cls, root: ElementTree.Element
    ) -> tuple[type[BaseDefineXMLReader], str]:
        elt = root.find("Study/MetaDataVersion", namespaces={"": ODM_NAMESPACE})
        pattern = compile(r"(\{(.*)\})?DefineVersion")
        for name, value in elt.items():
            match = pattern.fullmatch(name)
            if match:
                reader_class = cls._from_namespace(match.group(2))
                # only look after 2.1.* versions
                original_version = value if re.fullmatch(r"2\.1\.\d+", value) else None
                return reader_class, original_version
        return None, None

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
        directory_path = dirname(dataset_path)
        if define_xml_path is None:
            define_xml_path: str = join(
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
