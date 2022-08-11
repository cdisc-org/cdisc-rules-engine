from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader


class DataReaderFactory:
    # TODO: Expand get reader function as more data formats are available.
    def get_reader(self):
        return XPTReader()
