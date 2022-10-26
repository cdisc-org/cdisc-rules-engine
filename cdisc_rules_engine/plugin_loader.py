import sys

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points, EntryPoints
else:
    from importlib.metadata import entry_points, EntryPoints
from typing import Dict, Type, Any

from cdisc_rules_engine.interfaces import FactoryInterface
from cdisc_rules_engine.operations import OperationsFactory
from cdisc_rules_engine.services import LoggingServiceFactory
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_readers import DataReaderFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory


class PluginLoader:
    """
    This class is responsible for loading plugins
    from virtual environment. A plugin is
    expected to be a Python package with the class
    that implements a desired interface.
    """

    __group_factory_map: Dict[str, Type[FactoryInterface]] = {
        "core.plugins.cache_services": CacheServiceFactory,
        "core.plugins.data_readers": DataReaderFactory,
        "core.plugins.data_services": DataServiceFactory,
        "core.plugins.rule_operations": OperationsFactory,
        "core.plugins.logging": LoggingServiceFactory,
    }

    def load(self):
        """
        Entrypoint method that loads plugins for related groups.
        """
        # for each group
        for group_name, factory_class in self.__group_factory_map.items():
            # discover all group plugins and register them
            group_plugins: EntryPoints = entry_points().select(group=group_name)
            self.__register_group_plugins(factory_class, group_plugins)

    @classmethod
    def register_group_factory(
        cls, group_name: str, factory_class: Type[FactoryInterface]
    ):
        """
        Registers new plugin group and factory.
        """
        if not issubclass(factory_class, FactoryInterface):
            raise ValueError(
                f"Given class {factory_class} must implement FactoryInterface"
            )
        cls.__group_factory_map[group_name] = factory_class

    def __register_group_plugins(
        self, factory_class: Type[FactoryInterface], group_plugins: EntryPoints
    ):
        """
        Registers all given plugins in the factory.
        """
        # for each plugin
        for plugin in group_plugins:
            # load plugin and register in the factory
            custom_service: Type[Any] = plugin.load()
            factory_class.register_service(plugin.name, custom_service)
