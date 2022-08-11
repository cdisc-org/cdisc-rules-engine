import re
from datetime import datetime


class ValueLevelMetadata:
    def __init__(self):
        self.id = None
        self.operation = None
        self.check_values = None
        self.itemid = None
        self.softhard = None
        self.item = None

    @classmethod
    def from_where_clause_def(cls, where_clause, items):
        value_level_metadata = ValueLevelMetadata()
        range_check = where_clause.RangeCheck[0]
        value_level_metadata.id = where_clause.OID
        value_level_metadata.operation = range_check.Comparator
        value_level_metadata.check_values = range_check.CheckValue
        value_level_metadata.itemid = range_check.ItemOID
        value_level_metadata.item = items.get(range_check.ItemOID)
        value_level_metadata.softhard = range_check.SoftHard
        return value_level_metadata

    def get_filter_function(self):
        operator_map = {
            "LT": self.less_than(),
            "LE": self.less_than_or_equal_to(),
            "GE": self.greater_than_or_equal_to(),
            "GT": self.greater_than(),
            "EQ": self.equal_to(),
            "NE": self.not_equal_to(),
            "IN": self.is_in(),
            "NOTIN": self.is_not_in(),
        }
        return operator_map.get(self.operation)

    def get_type_check_function(self):
        type_check_function_map = {
            "text": self.is_text(),
            "integer": self.is_integer(),
            "float": self.is_float(),
            "datetime": self.is_datetime(),
            "incompleteDateTime": self.is_incomplete_datetime(),
            "date": self.is_date(),
            "time": self.is_time(),
            "partialDate": self.is_partial_date(),
            "partialTime": self.is_partial_time(),
            "partialDateTime": self.is_partial_datetime(),
            "durationDateTime": self.is_duration_datetime(),
            "intervalDateTime": self.is_interval_datetime(),
        }
        return type_check_function_map.get(self.item.DataType, self.is_text())

    def get_length_check_function(self):
        def length_check(dataframe):
            return len(str(dataframe[self.item.Name])) <= int(self.item.Length)

        return length_check

    def less_than(self):
        def lt(dataframe):
            return dataframe[self.item.Name] < self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return lt

    def greater_than(self):
        def gt(dataframe):
            return dataframe[self.item.Name] < self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return gt

    def less_than_or_equal_to(self):
        def le(dataframe):
            return dataframe[self.item.Name] <= self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return le

    def greater_than_or_equal_to(self):
        def ge(dataframe):
            return dataframe[self.item.Name] >= self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return ge

    def equal_to(self):
        def eq(dataframe):
            return dataframe[self.item.Name] == self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return eq

    def not_equal_to(self):
        def neq(dataframe):
            return dataframe[self.item.Name] != self._convert_to_datatype(
                self.check_values[0], dataframe[self.item.Name]
            )

        return neq

    def is_in(self):
        def isin(dataframe):
            return dataframe[self.item.Name] in self._convert_list_to_datatype(
                self.check_values, dataframe[self.item.Name]
            )

        return isin

    def is_not_in(self):
        def notin(dataframe):
            return dataframe[self.item.Name] not in self._convert_list_to_datatype(
                self.check_values, dataframe[self.item.Name]
            )

        return notin

    """
    Data type checks for define xml.
    Valid define-xml datatypes are:
    text
    float
    integer
    datetime
    date
    time
    partialDate
    partialTime
    partialDateTime
    incompleteDateTime
    durationDateTime
    intervalDateTime
    """

    def is_integer(self):
        def is_int(dataframe):
            return float(dataframe[self.item.Name]).is_integer()

        return is_int

    def is_float(self):
        def isfloat(dataframe):
            return not float(dataframe[self.item.Name]).is_integer()

        return isfloat

    def is_text(self):
        def istext(dataframe):
            return isinstance(dataframe[self.item.Name], str)

        return istext

    def is_datetime(self):
        def isdatetime(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            try:
                datetime.fromisoformat(dataframe[self.item.Name])
            except:
                try:
                    datetime.fromisoformat(
                        dataframe[self.item.Name].replace("Z", "+00:00")
                    )
                except:
                    return False
                return True
            return True

        return isdatetime

    def is_date(self):
        def isdate(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            try:
                datetime.date.fromisoformat(dataframe[self.item.Name])
            except:
                return False
            return True

        return isdate

    def is_time(self):
        def istime(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            try:
                datetime.time.fromisoformat(dataframe[self.item.Name])
            except:
                return False
            return True

        return istime

    def is_incomplete_datetime(self):
        def is_incomplete(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            try:
                datetime.fromisoformat(dataframe[self.item.Name])
            except:
                try:
                    datetime.fromisoformat(
                        dataframe[self.item.Name].replace("Z", "+00:00")
                    )
                except:
                    return True
                return False
            return False

        return is_incomplete

    def is_partial_date(self):
        def ispartialdate(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            return bool(
                re.match(r"^\d\d\d\d[-\d\d]?[-\d\d]?", dataframe[self.item.Name])
            )

        return ispartialdate

    def is_partial_time(self):
        hour_regex = r"((([0-1][0-9])|([2][0-3]))(:[0-5][0-9])?(((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z))?)"
        time_regex = r"[1-9][0-9]{3}\-.+?T[^\.]+?(Z|[\+\-].+)"

        def ispartialtime(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            return bool(re.match(hour_regex, dataframe[self.item.Name])) or bool(
                re.match(time_regex, dataframe[self.item.Name])
            )

        return ispartialtime

    def is_partial_datetime(self):
        hour_regex = r"((([0-1][0-9])|([2][0-3]))(:[0-5][0-9])?(((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z))?)"
        time_regex = r"[1-9][0-9]{3}\-.+?T[^\.]+?(Z|[\+\-].+)"
        date_regex = r"^\d\d\d\d[-\d\d]?[-\d\d]?"

        def ispartialdatetime(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            return (
                bool(re.match(hour_regex, dataframe[self.item.Name]))
                or bool(re.match(time_regex, dataframe[self.item.Name]))
                or bool(re.match(date_regex, dataframe[self.item.Name]))
            )

        return ispartialdatetime

    def is_interval_datetime(self):
        # sorry - regex from define schema
        regex = r"((((([0-9][0-9][0-9][0-9])((-(([0][1-9])|([1][0-2])))((-(([0][1-9])|([1-2][0-9])|([3][0-1])))(T((([0-1][0-9])|([2][0-3]))((:([0-5][0-9]))(((:([0-5][0-9]))((\.[0-9]+)?))?)?)?((((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z)))?))?)?)?))/((([0-9][0-9][0-9][0-9])((-(([0][1-9])|([1][0-2])))((-(([0][1-9])|([1-2][0-9])|([3][0-1])))(T((([0-1][0-9])|([2][0-3]))((:([0-5][0-9]))(((:([0-5][0-9]))((\.[0-9]+)?))?)?)?((((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z)))?))?)?)?)))|(((([0-9][0-9][0-9][0-9])((-(([0][1-9])|([1][0-2])))((-(([0][1-9])|([1-2][0-9])|([3][0-1])))(T((([0-1][0-9])|([2][0-3]))((:([0-5][0-9]))(((:([0-5][0-9]))((\.[0-9]+)?))?)?)?((((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z)))?))?)?)?))/(((\+|-)?P(((([0-9]([0-9]+)?)Y)?(([0-9]([0-9]+)?)M)?(([0-9]([0-9]+)?)D)?)(T((([0-9]([0-9]+)?)H)?(([0-9]([0-9]+)?)M)?(([0-9]([0-9]+)?)((\.[0-9]+)?)S)?)?)?|((([0-9]([0-9]+)?)W))))))|((((\+|-)?P(((([0-9]([0-9]+)?)Y)?(([0-9]([0-9]+)?)M)?(([0-9]([0-9]+)?)D)?)(T((([0-9]([0-9]+)?)H)?(([0-9]([0-9]+)?)M)?(([0-9]([0-9]+)?)((\.[0-9]+)?)S)?)?)?|((([0-9]([0-9]+)?)W)))))/((([0-9][0-9][0-9][0-9])((-(([0][1-9])|([1][0-2])))((-(([0][1-9])|([1-2][0-9])|([3][0-1])))(T((([0-1][0-9])|([2][0-3]))((:([0-5][0-9]))(((:([0-5][0-9]))((\.[0-9]+)?))?)?)?((((\+|-)(([0-1][0-9])|([2][0-3])):[0-5][0-9])|(Z)))?))?)?)?))))"

        def isinterval(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            return bool(re.match(regex, dataframe[self.item.Name]))

        return isinterval

    def is_duration_datetime(self):
        regex = r"(-?)P(?=.)((\d+)Y)?((\d+)M)?((\d+)D)?(T(?=.)((\d+)H)?((\d+)M)?(\d*(\.\d+)?S)?)?"

        def isduration(dataframe):
            if dataframe[self.item.Name] == "":
                return True
            return bool(re.match(regex, dataframe[self.item.Name]))

        return isduration

    def _convert_to_datatype(self, target, value):
        data_type = type(value)
        return data_type(target._content)

    def _convert_list_to_datatype(self, target_list, value):
        return [self._convert_to_datatype(target, value) for target in target_list]
