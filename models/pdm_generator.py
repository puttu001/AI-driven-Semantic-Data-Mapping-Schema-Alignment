"""
Physical Data Model (PDM) Generator

Takes a Logical Data Model (LDM) and a target database dialect, and produces:
- Physical table/column names
- Database-specific data types
- CREATE TABLE DDL with PK, FK, NOT NULL constraints
- Index suggestions
- MongoDB JSON Schema validators (for MongoDB dialect)

Supported dialects: postgresql, mysql, snowflake, mongodb
"""

from typing import Dict, Optional, List
from datetime import datetime
import re


SUPPORTED_DIALECTS = ["postgresql", "mysql", "snowflake", "mongodb"]

# Logical type → physical type mapping per dialect
TYPE_MAPPINGS: Dict[str, Dict[str, str]] = {
    "postgresql": {
        "Identifier": "VARCHAR(50)",
        "Text": "VARCHAR(255)",
        "Numeric": "INTEGER",
        "Currency": "DECIMAL(18,2)",
        "Percentage": "DECIMAL(5,2)",
        "Date": "DATE",
        "DateTime": "TIMESTAMP",
        "Boolean": "BOOLEAN",
        "Code": "VARCHAR(20)",
        "Email": "VARCHAR(255)",
        "Phone": "VARCHAR(30)",
        "Address": "TEXT",
        "URL": "VARCHAR(500)",
        "Binary": "BYTEA",
        "Memo": "TEXT",
    },
    "mysql": {
        "Identifier": "VARCHAR(50)",
        "Text": "VARCHAR(255)",
        "Numeric": "INT",
        "Currency": "DECIMAL(18,2)",
        "Percentage": "DECIMAL(5,2)",
        "Date": "DATE",
        "DateTime": "DATETIME",
        "Boolean": "TINYINT(1)",
        "Code": "VARCHAR(20)",
        "Email": "VARCHAR(255)",
        "Phone": "VARCHAR(30)",
        "Address": "TEXT",
        "URL": "VARCHAR(500)",
        "Binary": "BLOB",
        "Memo": "TEXT",
    },
    "snowflake": {
        "Identifier": "VARCHAR(50)",
        "Text": "VARCHAR(255)",
        "Numeric": "NUMBER(10,0)",
        "Currency": "NUMBER(18,2)",
        "Percentage": "NUMBER(5,2)",
        "Date": "DATE",
        "DateTime": "TIMESTAMP_NTZ",
        "Boolean": "BOOLEAN",
        "Code": "VARCHAR(20)",
        "Email": "VARCHAR(255)",
        "Phone": "VARCHAR(30)",
        "Address": "VARCHAR(1000)",
        "URL": "VARCHAR(500)",
        "Binary": "BINARY",
        "Memo": "VARCHAR(16777216)",
    },
    "mongodb": {
        "Identifier": "string",
        "Text": "string",
        "Numeric": "int",
        "Currency": "decimal",
        "Percentage": "double",
        "Date": "date",
        "DateTime": "date",
        "Boolean": "bool",
        "Code": "string",
        "Email": "string",
        "Phone": "string",
        "Address": "string",
        "URL": "string",
        "Binary": "binData",
        "Memo": "string",
    },
}


def _to_physical_name(name: str) -> str:
    """
    Convert an entity or attribute name to a physical name (snake_case).
    'Customer Master' -> 'customer_master'
    'AccountID' -> 'account_id'
    'already_snake' -> 'already_snake'
    """
    if not name:
        return name

    # If already has underscores and is lowercase-ish, keep as-is
    if '_' in name and name == name.lower():
        return name

    # Replace spaces and hyphens with underscores
    result = name.replace(' ', '_').replace('-', '_')

    # Insert underscore before uppercase letters (camelCase/PascalCase)
    result = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', result)

    # Clean up multiple underscores and lowercase
    result = re.sub(r'_+', '_', result).strip('_').lower()

    return result


def _get_physical_type(logical_type: str, dialect: str) -> str:
    """Map a logical data type to a physical type for the given dialect."""
    dialect_map = TYPE_MAPPINGS.get(dialect, TYPE_MAPPINGS["postgresql"])
    return dialect_map.get(logical_type, dialect_map.get("Text", "VARCHAR(255)"))


def _build_fk_lookup(relationships: List[Dict]) -> Dict[str, Dict]:
    """
    Build a lookup: (source_entity, source_attribute) -> relationship info.
    Used to mark FK columns in the physical model.
    """
    lookup = {}
    for rel in relationships:
        key = (rel['source_entity'], rel['source_attribute'])
        lookup[key] = {
            'target_table': _to_physical_name(rel['target_entity']),
            'target_column': _to_physical_name(rel['target_attribute']),
            'relationship_type': rel.get('relationship_type', '1:N'),
        }
    return lookup


def _suggest_indexes(entity: Dict, fk_lookup: Dict, entity_name: str) -> List[Dict]:
    """
    Suggest indexes for a physical table.
    - All FK columns get an index
    - Date/DateTime columns in fact-like tables get an index
    """
    indexes = []
    physical_table = _to_physical_name(entity_name)

    for attr in entity.get('attributes', []):
        attr_name = attr['attribute_name']
        physical_col = _to_physical_name(attr_name)

        # Skip PKs — they already have an implicit index
        if attr.get('is_primary_key'):
            continue

        # FK columns
        if (entity_name, attr_name) in fk_lookup:
            indexes.append({
                'index_name': f"idx_{physical_table}_{physical_col}",
                'columns': [physical_col],
                'reason': f"Foreign key to {fk_lookup[(entity_name, attr_name)]['target_table']}",
            })

        # Date columns in tables that look like fact tables
        logical_type = attr.get('logical_data_type', '')
        if logical_type in ('Date', 'DateTime'):
            name_lower = entity_name.lower()
            if any(prefix in name_lower for prefix in ('fact_', 'transaction', 'order', 'invoice', 'log', 'event')):
                indexes.append({
                    'index_name': f"idx_{physical_table}_{physical_col}",
                    'columns': [physical_col],
                    'reason': f"Date column in transactional table for range queries",
                })

    return indexes


def _generate_ddl_sql(pdm: Dict, dialect: str) -> str:
    """Generate SQL DDL (CREATE TABLE + indexes) for postgresql/mysql/snowflake."""
    parts = []

    # Determine quoting style
    quote = '"' if dialect in ('postgresql', 'snowflake') else '`'

    for table in pdm['tables']:
        tname = table['physical_table_name']
        col_lines = []
        pk_cols = []
        fk_lines = []

        for col in table['columns']:
            cname = col['physical_column_name']
            ctype = col['physical_data_type']
            nullable = "" if col['is_nullable'] else " NOT NULL"
            col_lines.append(f"    {quote}{cname}{quote} {ctype}{nullable}")

            if col['is_primary_key']:
                pk_cols.append(f"{quote}{cname}{quote}")

            if col.get('is_foreign_key') and col.get('fk_references'):
                ref = col['fk_references']  # format: "table_name(column_name)"
                match = re.match(r'(.+)\((.+)\)', ref)
                if match:
                    ref_table, ref_col = match.groups()
                    fk_lines.append(
                        f"    CONSTRAINT {quote}fk_{tname}_{cname}{quote} "
                        f"FOREIGN KEY ({quote}{cname}{quote}) "
                        f"REFERENCES {quote}{ref_table}{quote}({quote}{ref_col}{quote})"
                    )

        # PK constraint
        if pk_cols:
            col_lines.append(f"    CONSTRAINT {quote}pk_{tname}{quote} PRIMARY KEY ({', '.join(pk_cols)})")

        # FK constraints
        col_lines.extend(fk_lines)

        ddl = f"CREATE TABLE {quote}{tname}{quote} (\n"
        ddl += ",\n".join(col_lines)
        ddl += "\n);"
        parts.append(ddl)

        # Index statements
        for idx in table.get('indexes', []):
            idx_cols = ", ".join(f"{quote}{c}{quote}" for c in idx['columns'])
            parts.append(
                f"CREATE INDEX {quote}{idx['index_name']}{quote} "
                f"ON {quote}{tname}{quote} ({idx_cols});"
            )

        parts.append("")  # blank line between tables

    return "\n".join(parts)


def _generate_mongodb_schema(pdm: Dict) -> str:
    """Generate MongoDB createCollection with JSON Schema validators + createIndex."""
    parts = []

    for table in pdm['tables']:
        coll_name = table['physical_table_name']

        # Build $jsonSchema properties
        properties = {}
        required = []

        for col in table['columns']:
            bson_type = col['physical_data_type']
            properties[col['physical_column_name']] = {
                "bsonType": bson_type,
                "description": col.get('logical_attribute_name', col['physical_column_name'])
            }
            if not col['is_nullable']:
                required.append(col['physical_column_name'])

        schema = {
            "bsonType": "object",
            "required": required,
            "properties": properties
        }

        import json
        schema_str = json.dumps(schema, indent=4)

        parts.append(f"// Collection: {coll_name}")
        parts.append(f"db.createCollection(\"{coll_name}\", {{")
        parts.append(f"    validator: {{")
        parts.append(f"        $jsonSchema: {schema_str}")
        parts.append(f"    }}")
        parts.append(f"}});")
        parts.append("")

        # Indexes
        for idx in table.get('indexes', []):
            idx_fields = ", ".join(f'"{c}": 1' for c in idx['columns'])
            parts.append(
                f'db.{coll_name}.createIndex({{ {idx_fields} }}, '
                f'{{ name: "{idx["index_name"]}" }});'
            )

        parts.append("")

    return "\n".join(parts)


def generate_physical_data_model(ldm: Dict, dialect: str = "postgresql") -> Optional[Dict]:
    """
    Generate a Physical Data Model from a Logical Data Model.

    Args:
        ldm: Logical Data Model dict (output of generate_logical_data_model).
        dialect: Target database dialect ("postgresql", "mysql", "snowflake", "mongodb").

    Returns:
        Dict with structure:
        {
            "dialect": str,
            "tables": [
                {
                    "physical_table_name": str,
                    "logical_entity_name": str,
                    "columns": [
                        {
                            "physical_column_name": str,
                            "logical_attribute_name": str,
                            "physical_data_type": str,
                            "logical_data_type": str,
                            "is_primary_key": bool,
                            "is_nullable": bool,
                            "is_foreign_key": bool,
                            "fk_references": str or None
                        }
                    ],
                    "indexes": [
                        {"index_name": str, "columns": [str], "reason": str}
                    ]
                }
            ],
            "ddl": str,
            "metadata": {
                "dialect": str,
                "total_tables": int,
                "total_columns": int,
                "total_indexes": int,
                "total_foreign_keys": int,
                "generated_at": str
            }
        }
    """
    if dialect not in SUPPORTED_DIALECTS:
        print(f"  Unsupported dialect: {dialect}. Supported: {SUPPORTED_DIALECTS}")
        return None

    if not ldm or 'entities' not in ldm:
        print("  No LDM provided for PDM generation")
        return None

    print(f"\n🔧 Generating Physical Data Model ({dialect})...")

    relationships = ldm.get('relationships', [])
    fk_lookup = _build_fk_lookup(relationships)

    tables = []
    total_columns = 0
    total_indexes = 0
    total_fks = 0

    for entity in ldm['entities']:
        entity_name = entity['entity_name']
        physical_table = _to_physical_name(entity_name)

        columns = []
        for attr in entity.get('attributes', []):
            attr_name = attr['attribute_name']
            logical_type = attr.get('logical_data_type', 'Text')
            physical_col = _to_physical_name(attr_name)
            physical_type = _get_physical_type(logical_type, dialect)

            # Check FK
            is_fk = (entity_name, attr_name) in fk_lookup
            fk_ref = None
            if is_fk:
                fk_info = fk_lookup[(entity_name, attr_name)]
                fk_ref = f"{fk_info['target_table']}({fk_info['target_column']})"
                total_fks += 1

            columns.append({
                'physical_column_name': physical_col,
                'logical_attribute_name': attr_name,
                'physical_data_type': physical_type,
                'logical_data_type': logical_type,
                'is_primary_key': attr.get('is_primary_key', False),
                'is_nullable': attr.get('is_nullable', True),
                'is_foreign_key': is_fk,
                'fk_references': fk_ref,
            })

        # Suggest indexes
        indexes = _suggest_indexes(entity, fk_lookup, entity_name)

        tables.append({
            'physical_table_name': physical_table,
            'logical_entity_name': entity_name,
            'columns': columns,
            'indexes': indexes,
        })

        total_columns += len(columns)
        total_indexes += len(indexes)

    pdm = {
        'dialect': dialect,
        'tables': tables,
    }

    # Generate DDL
    if dialect == "mongodb":
        pdm['ddl'] = _generate_mongodb_schema(pdm)
    else:
        pdm['ddl'] = _generate_ddl_sql(pdm, dialect)

    pdm['metadata'] = {
        'dialect': dialect,
        'total_tables': len(tables),
        'total_columns': total_columns,
        'total_indexes': total_indexes,
        'total_foreign_keys': total_fks,
        'generated_at': datetime.now().isoformat(),
    }

    print(f"  PDM complete: {len(tables)} tables, {total_columns} columns, "
          f"{total_indexes} indexes, {total_fks} foreign keys")

    return pdm
