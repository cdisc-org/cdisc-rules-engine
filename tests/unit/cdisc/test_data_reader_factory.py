from unittest.mock import MagicMock

from cdisc_rules_engine.interfaces import DataReaderInterface
from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.services.data_readers.stf_xml_reader import STFXMLReader


def test_get_registered_service():
    """
    Unit test that registers a new service and gets it.
    """
    # use type function to declare a new class dynamically
    new_service_class = type(
        "TestReader",
        (DataReaderInterface,),
        {
            "read": MagicMock(),
            "from_file": MagicMock(),
        },
    )
    service_name: str = "test_service"

    DataReaderFactory.register_service(service_name, new_service_class)
    factory = DataReaderFactory()
    assert isinstance(factory.get_service(service_name), new_service_class)


def test_get_stf_reader_service():
    factory = DataReaderFactory()
    service = factory.get_service(DataFormatTypes.STF.value)
    assert isinstance(service, STFXMLReader)
