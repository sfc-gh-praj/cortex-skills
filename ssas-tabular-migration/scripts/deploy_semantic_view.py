#!/usr/bin/env python3
"""
deploy_semantic_view.py - Deploy a Snowflake Semantic View from a YAML file.

Delegates deployment to the semantic-view bundled skill's upload_semantic_view_yaml.py
so that any improvements in the skill are automatically picked up without code duplication.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/deploy_semantic_view.py \
        --yaml-file ./ssas_semantic_view.yaml \
        --target-schema ADVENTUREWORKSDW2022.DBO \
        --connection COCO_JK \
        --analyst-yaml ./ssas_semantic_view_analyst.yaml
"""

import argparse
import glob
import subprocess
import sys
import tempfile
from pathlib import Path


def _find_semantic_view_skill_dir() -> Path | None:
    """Locate the semantic-view bundled skill directory (version-independent)."""
    home = Path.home()
    pattern = str(home / ".local" / "share" / "cortex" / "*" / "bundled_skills" / "semantic-view")
    matches = sorted(glob.glob(pattern))
    return Path(matches[-1]) if matches else None


def _upload_via_skill(yaml_file: str, target_schema: str, connection_name: str,
                      verify_only: bool = False, skill_dir: Path | None = None) -> int:
    """
    Call the semantic-view skill's upload_semantic_view_yaml.py script.

    Returns the subprocess exit code (0 = success).
    """
    if skill_dir is None:
        skill_dir = _find_semantic_view_skill_dir()
    if skill_dir is None:
        raise RuntimeError(
            "Could not find the semantic-view bundled skill. "
            "Ensure Cortex Code is installed and the semantic-view skill is available."
        )

    upload_script = skill_dir / "scripts" / "upload_semantic_view_yaml.py"
    if not upload_script.exists():
        raise RuntimeError(f"Upload script not found at expected path: {upload_script}")

    cmd = [
        "uv", "run",
        "--project", str(skill_dir),
        "python", str(upload_script),
        yaml_file,
        target_schema,
        "--connection", connection_name,
    ]
    if verify_only:
        cmd.append("--verify-only")

    result = subprocess.run(cmd, text=True)
    return result.returncode


def deploy_semantic_view(yaml_content: str, target_schema: str,
                         connection_name: str, dry_run: bool = False) -> str:
    """
    Deploy a semantic view to Snowflake via the semantic-view skill's upload script.

    Args:
        yaml_content: The full YAML spec as a string.
        target_schema: Target schema in DB.SCHEMA format.
        connection_name: Snowflake connection name.
        dry_run: If True, only validates the YAML without creating the view.

    Returns:
        The fully-qualified semantic view name on success.

    Raises:
        RuntimeError on deployment failure.
    """
    skill_dir = _find_semantic_view_skill_dir()
    if skill_dir is None:
        raise RuntimeError(
            "Could not find the semantic-view bundled skill. "
            "Ensure Cortex Code is installed."
        )

    # Write YAML to a temp file for the upload script
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(yaml_content)
        tmp_path = tmp.name

    try:
        rc = _upload_via_skill(tmp_path, target_schema, connection_name,
                               verify_only=dry_run, skill_dir=skill_dir)
        if rc != 0:
            raise RuntimeError(f"semantic-view upload script exited with code {rc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    # Extract view name from YAML (first non-comment 'name:' field)
    view_name = None
    for line in yaml_content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if stripped.startswith("name:"):
            view_name = stripped.split(":", 1)[1].strip()
            break

    return f"{target_schema}.{view_name.upper()}" if view_name else target_schema


def _test_view_flag(fq_name: str, connection_name: str) -> bool:
    """
    Attempt a quick --view test. Returns True if Cortex Analyst accepted the view,
    False if it rejected it (schema mismatch / unknown field errors).
    """
    result = subprocess.run(
        ["cortex", "analyst", "query", "ping", "--view", fq_name,
         "--connection", connection_name],
        capture_output=True, text=True,
    )
    output = (result.stdout + result.stderr).lower()
    # Schema-mismatch errors contain these phrases
    rejected = any(kw in output for kw in [
        "unknown field", "failed to parse", "invalid value", "schema mismatch",
    ])
    return not rejected


def main():
    parser = argparse.ArgumentParser(
        description="Deploy a Snowflake Semantic View from YAML via the semantic-view skill"
    )
    parser.add_argument("--yaml-file", required=True,
                        help="Path to the semantic view YAML file (joins: format)")
    parser.add_argument("--target-schema", required=True,
                        help="Snowflake target schema, e.g. MY_DB.MY_SCHEMA")
    parser.add_argument("--connection", default="COCO_JK",
                        help="Snowflake connection name (default: COCO_JK)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate the YAML only — do not create the semantic view")
    parser.add_argument("--analyst-yaml", default=None,
                        help="Path to the Cortex Analyst --model compatible YAML (*_analyst.yaml). "
                             "If provided, runs a test query after deployment.")
    parser.add_argument("--test-question", default="What is the total internet sales?",
                        help="Question to test with Cortex Analyst after deployment.")
    args = parser.parse_args()

    yaml_path = Path(args.yaml_file)
    if not yaml_path.exists():
        print(f"ERROR: YAML file not found: {args.yaml_file}", file=sys.stderr)
        sys.exit(1)

    yaml_content = yaml_path.read_text(encoding="utf-8")
    if not yaml_content.strip():
        print("ERROR: YAML file is empty.", file=sys.stderr)
        sys.exit(1)

    # Strip comment lines before deploying
    clean_lines = [l for l in yaml_content.split("\n") if not l.strip().startswith("#")]
    yaml_clean = "\n".join(clean_lines)

    try:
        fq_name = deploy_semantic_view(
            yaml_clean,
            args.target_schema.strip(),
            args.connection,
            dry_run=args.dry_run,
        )
        print(f"\nDeploy complete: {fq_name}", flush=True)
    except Exception as e:
        print(f"ERROR: Deployment failed — {e}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        return

    # Try --view to see if this Cortex version accepts the deployed format
    print(f"\nTesting --view compatibility...")
    view_ok = _test_view_flag(fq_name, args.connection)
    if view_ok:
        print(f"  --view is compatible. Test with:")
        print(f"  cortex analyst query \"{args.test_question}\" --view {fq_name} --connection {args.connection}")
    else:
        print(f"  --view has a YAML format mismatch (deployed format uses 'joins:', CLI expects 'relationships:').")
        print(f"  Use --model with the analyst YAML instead:")
        if args.analyst_yaml:
            print(f"  cortex analyst query \"{args.test_question}\" --model {args.analyst_yaml} --connection {args.connection}")
            print(f"\nRunning Cortex Analyst test with --model...")
            cmd = [
                "cortex", "analyst", "query", args.test_question,
                "--model", args.analyst_yaml,
                "--connection", args.connection,
            ]
            print(f"  {' '.join(cmd)}\n")
            subprocess.run(cmd, text=True)
        else:
            print(f"  cortex analyst query \"{args.test_question}\" --model <analyst_yaml_path> --connection {args.connection}")
            print(f"  (Pass --analyst-yaml <path> to auto-test after deployment)")


if __name__ == "__main__":
    main()
