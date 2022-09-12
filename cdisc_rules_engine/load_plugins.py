from importlib.metadata import entry_points, EntryPoint
from typing import Type, Any, Tuple, Dict

from cdisc_rules_engine.services.data_readers import (
    DataReaderFactory,
)
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
)
from cdisc_rules_engine.operations import OperationsFactory
from cdisc_rules_engine.interfaces import FactoryInterface


def _register_group_plugins(
    factory_class: Type[FactoryInterface], group_plugins: Tuple[EntryPoint]
):
    """
    Registers all given plugins in the factory.
    """
    # for each plugin
    for plugin in group_plugins:
        # load plugin and register in the factory
        custom_service: Type[Any] = plugin.load()
        factory_class.register_service(plugin.name, custom_service)


def load_plugins():
    """
    Loads plugins from venv and extends factories
    from cdisc_rules_engine package.
    """
    group_factory_map: Dict[str, Type[FactoryInterface]] = {
        "core.plugins.data_readers": DataReaderFactory,
        "core.plugins.data_services": DataServiceFactory,
        "core.plugins.rule_operations": OperationsFactory,
    }
    # for each group
    for group_name, factory_class in group_factory_map.items():
        # discover all group plugins and register them
        group_plugins: Tuple[EntryPoint] = entry_points().get(group_name, [])
        _register_group_plugins(factory_class, group_plugins)


def main():
    """
    This function is an entrypoint to the script.
    The script is automatically executed as engine folder is initialized.
    """
    load_plugins()


if __name__ == "__main__":
    main()
