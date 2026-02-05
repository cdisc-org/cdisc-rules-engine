# Executability

Indicates the extent to which the rule can be automatically executed and validated by the rules engine. This is informational metadata that describes the rule's execution capability.

### Example

```yaml
Executability: Fully Executable
```

### Notes

- Executability is informational metadata and does not control whether a rule executes
- All rules are executed by the engine regardless of their Executability value
- The Executability value is included in validation reports to inform users about the rule's execution capability

## Fully Executable

The rule can be fully executed and validated automatically by the rules engine.

## Not Executable

The rule cannot be executed automatically by the rules engine.

## Partially Executable

The rule can be partially executed, but may not capture all validation scenarios.

## Partially Executable - Possible Overreporting

The rule can be partially executed but may report more violations than actually exist.

## Partially Executable - Possible Underreporting

The rule can be partially executed but may miss some violations that should be reported.
