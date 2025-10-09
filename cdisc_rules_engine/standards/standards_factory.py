from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)
from cdisc_rules_engine.standards.sdtm_standards_context import SdtmStandardsContext


class StandardsFactory:
    _lookup = {"SDTMIG": SdtmStandardsContext}

    # Temporarily adding Library metadata container
    @staticmethod
    def get_standards_context(
        standard: str, standard_version: str, standard_substandard: str, library_metadata: LibraryMetadataContainer
    ) -> BaseStandardsContext:
        constructor = StandardsFactory._lookup.get(standard.upper(), DefaultStandardsContext)
        return constructor(library_metadata)
