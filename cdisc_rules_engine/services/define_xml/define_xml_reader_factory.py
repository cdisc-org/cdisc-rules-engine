from xml.etree import ElementTree
from re import compile
from typing import Union

from cdisc_rules_engine.constants.define_xml_constants import (
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
    def from_filename(cls, filename: str, validate_xml: str = None):
        """
        Inits a DefineXMLReader object from file.
        """
        logger.info(f"Reading Define-XML from file name. filename={filename}")
        define_xml_reader_class: type = cls._get_define_xml_reader(
            ElementTree.parse(filename).getroot()
        )
        reader: BaseDefineXMLReader = define_xml_reader_class()
        reader._odm_loader.open_odm_document(filename)
        return reader

    @classmethod
    def from_file_contents(
        cls,
        file_contents: Union[str, bytes],
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
        validate_xml: str = None,
    ):
        """
        Inits a DefineXMLReader object from file contents.
        """
        logger.info("Reading Define-XML from file contents")
        define_xml_reader_class: type = cls._get_define_xml_reader(
            ElementTree.fromstring(file_contents)
        )
        reader: BaseDefineXMLReader = define_xml_reader_class(
            cache_service_obj, study_id, data_bundle_id, validate_xml
        )
        reader._odm_loader.load_odm_string(file_contents)
        return reader

    @classmethod
    def _get_define_xml_reader(cls, root: ElementTree.Element) -> BaseDefineXMLReader:
        elt = root.find(
            "Study/MetaDataVersion",
            namespaces={"": ODM_NAMESPACE},
        )
        pattern = compile(r"(\{(.*)\})?DefineVersion")
        define_version = next(
            iter(
                cls._from_namespace_version(match.group(2), value)
                for match, value in [
                    (pattern.fullmatch(name), value) for name, value in elt.items()
                ]
                if match
            ),
            None,
        )
        return define_version

    @classmethod
    def _from_namespace_version(
        cls,
        namespace: str,
        version: str,
    ) -> BaseDefineXMLReader:
        define_xml_reader = next(
            iter(
                define_xml_reader
                for define_xml_reader in cls._define_xml_readers
                if namespace == define_xml_reader.class_define_xml_version().namespace
                and version == define_xml_reader.class_define_xml_version().version
            ),
            None,
        )
        return define_xml_reader
