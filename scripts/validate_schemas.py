"""
Validates all Avro schema files in ingestion/schema_registry/.
Called by CI to catch schema errors before deployment.
"""
import json
import sys
from pathlib import Path
import fastavro.schema


def validate_all_schemas():
    schema_dir = Path("ingestion/schema_registry")
    schemas    = list(schema_dir.glob("*.avsc"))

    if not schemas:
        print("No .avsc files found — check schema_registry directory")
        sys.exit(1)

    errors = []
    for schema_path in schemas:
        try:
            schema = json.loads(schema_path.read_text())
            fastavro.schema.parse_schema(schema)
            print(f"  ✓ Valid: {schema_path.name}")
        except Exception as e:
            print(f"  ✗ INVALID: {schema_path.name} — {e}")
            errors.append(schema_path.name)

    if errors:
        print(f"\nSchema validation FAILED for: {errors}")
        sys.exit(1)

    print(f"\nAll {len(schemas)} schemas valid.")


if __name__ == "__main__":
    validate_all_schemas()