from unittest.mock import MagicMock

from cdisc_rules_engine.interfaces import DataReaderInterface
from cdisc_rules_engine.services.data_readers import DataReaderFactory


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
