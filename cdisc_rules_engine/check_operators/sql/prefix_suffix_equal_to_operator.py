from .base_sql_operator import BaseSqlOperator


class PrefixSuffixEqualToOperator(BaseSqlOperator):
    """Operator for checking if prefix or suffix equals to expected value."""

    def execute_operator(self, other_value):
        if "prefix" in other_value:
            mode = "prefix"
        elif "suffix" in other_value:
            mode = "suffix"
        else:
            raise ValueError("Missing 'suffix' or 'prefix' key in operator parameters.")

        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")
        comparator = self.replace_prefix(comparator) if not value_is_literal else comparator
        length = self.replace_prefix(other_value.get("length"))
        # For backward compatibility, support 'prefix' and 'suffix' keys
        if length is None:
            if mode == "prefix":
                length = self.replace_prefix(other_value.get("prefix"))
            else:
                length = self.replace_prefix(other_value.get("suffix"))

        return self._handle_comparator(target_column, comparator, value_is_literal, length, mode)

    def _handle_comparator(self, target_column, comparator, value_is_literal, length, mode):
        """Handle any type of comparator (column, literal, list, tuple, or operation variable)."""
        cache_key = f"{target_column}_{mode}_equal_to_{str(comparator).replace(' ', '_')}_{value_is_literal}_{length}"

        def sql():
            target_sql = self._column_sql(target_column)
            if mode == "prefix":
                compare_sql = f"LEFT({target_sql}, {length})"
            else:
                compare_sql = f"RIGHT({target_sql}, {length})"

            # Handle special case for DOMAIN comparator
            if comparator == "DOMAIN" and not value_is_literal:
                domain_value = self.column_prefix_map.get("--", "")
                if domain_value:
                    return f"""NOT ({self._is_empty_sql(target_column)})
                              AND {compare_sql} = {self._constant_sql(domain_value)}"""
                else:
                    return "FALSE"

            # Determine data source and whether to use EXISTS pattern
            if isinstance(comparator, list):
                data_source = f"(VALUES {', '.join(f'({self._constant_sql(v)})' for v in comparator)})"
            elif (
                isinstance(comparator, str)
                and comparator in self.operation_variables
                and self.operation_variables[comparator].type == "collection"
            ):
                data_source = self._collection_sql(comparator)
            else:
                # Single value comparison
                comparator_sql = self._sql(comparator, value_is_literal=value_is_literal)
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {compare_sql} = {comparator_sql}"""

            # Multi-value comparison using EXISTS
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM {data_source} AS values_table(value)
                          WHERE {compare_sql} = values_table.value
                      )"""

        return self._do_check_operator(cache_key, sql)
