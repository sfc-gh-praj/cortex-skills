#!/usr/bin/env python3
"""
deploy_semantic_view.py - Deploy a Snowflake Semantic View from a YAML file.

Creates the semantic view object in Snowflake using
SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(). This makes the view available
for Cortex Analyst queries via `cortex analyst query --view DB.SCHEMA.VIEW`.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/deploy_semantic_view.py \
        --yaml-file ./ssas_semantic_view.yaml \
        --target-schema ADVENTUREWORKSDW2022.DBO \
        --connection COCO_JK
"""

import argparse
import sys
from pathlib import Path

import snowflake.connector


def deploy_semantic_view(yaml_content: str, target_schema: str,
                         connection_name: str, dry_run: bool = False) -> str:
    """
    Deploy a semantic view to Snowflake.

    Args:
        yaml_content: The full YAML spec as a string.
        target_schema: Target schema in DB.SCHEMA format.
        connection_name: Snowflake connection name.
        dry_run: If True, only validates the YAML without creating the view.
                 If False (default), creates the semantic view (replaces if exists).

    Returns:
        The fully-qualified semantic view name on success.

    Raises:
        Exception on deployment failure.
    """
    conn = snowflake.connector.connect(connection_name=connection_name)
    try:
        cur = conn.cursor()

        # Set session context so the procedure can resolve base_table references.
        db, schema = target_schema.split(".", 1)
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {target_schema}")

        # Third parameter is dry_run: TRUE = validate only, FALSE = create/replace.
        dry_run_flag = "TRUE" if dry_run else "FALSE"

        # Use $$ dollar-quoting for the YAML content to avoid escaping issues.
        # Fall back to single-quote escaping only if the YAML itself contains $$.
        if "$$" not in yaml_content:
            sql = (
                f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(\n"
                f"  '{target_schema}',\n"
                f"  $${yaml_content}$$,\n"
                f"  {dry_run_flag}\n"
                f")"
            )
        else:
            # YAML contains $$: escape for single-quote SQL literal
            safe = yaml_content.replace("\\", "\\\\").replace("'", "''")
            sql = (
                f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(\n"
                f"  '{target_schema}',\n"
                f"  '{safe}',\n"
                f"  {dry_run_flag}\n"
                f")"
            )

        cur.execute(sql)
        row = cur.fetchone()
        result_msg = row[0] if row else "OK"

        # Extract view name from YAML (first 'name:' field)
        view_name = None
        for line in yaml_content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("name:") and not stripped.startswith("name: "):
                continue
            if stripped.startswith("name:"):
                view_name = stripped.split(":", 1)[1].strip()
                break

        fq_name = f"{target_schema}.{view_name.upper()}" if view_name else target_schema

        # Verify the view was created
        try:
            verify_sql = f"DESCRIBE SEMANTIC VIEW {fq_name}"
            cur.execute(verify_sql)
            print(f"Semantic view deployed: {fq_name}")
            print(f"  Result: {result_msg}")
        except Exception:
            print(f"Semantic view created (could not verify): {fq_name}")
            print(f"  Result: {result_msg}")

        return fq_name
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Deploy a Snowflake Semantic View from YAML"
    )
    parser.add_argument("--yaml-file", required=True,
                        help="Path to the semantic view YAML file")
    parser.add_argument("--target-schema", required=True,
                        help="Snowflake target schema, e.g. MY_DB.MY_SCHEMA")
    parser.add_argument("--connection", default="COCO_JK",
                        help="Snowflake connection name (default: COCO_JK)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate the YAML only — do not create the semantic view")
    args = parser.parse_args()

    yaml_path = Path(args.yaml_file)
    if not yaml_path.exists():
        print(f"ERROR: YAML file not found: {args.yaml_file}", file=sys.stderr)
        sys.exit(1)

    yaml_content = yaml_path.read_text(encoding="utf-8")
    if not yaml_content.strip():
        print("ERROR: YAML file is empty.", file=sys.stderr)
        sys.exit(1)

    # Strip comment lines (lines starting with #) — Snowflake's parser
    # may not handle them in the YAML spec.
    lines = yaml_content.split("\n")
    clean_lines = [l for l in lines if not l.strip().startswith("#")]
    yaml_clean = "\n".join(clean_lines)

    try:
        fq_name = deploy_semantic_view(
            yaml_clean,
            args.target_schema.strip(),
            args.connection,
            dry_run=args.dry_run,
        )
        print(f"\nQuery it with:")
        print(f"  cortex analyst query \"your question\" --view {fq_name}")
    except Exception as e:
        print(f"ERROR: Deployment failed — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
