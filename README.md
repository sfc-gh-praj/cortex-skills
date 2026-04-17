# Cortex Skills

A collection of custom skills for [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) — Snowflake's AI coding agent.

Each skill encodes a domain-specific workflow that Cortex Code follows when you load it into a session. Skills enforce phase gates, gather required inputs, reference curated knowledge, and drive structured multi-step processes that plain prompting cannot reliably replicate.

## Available Skills

| Skill | Description |
|-------|-------------|
| [ssis-migration](./ssis-migration/) | 5-phase SSIS-to-Snowflake migration framework with Human-in-the-Loop gates, EWI-driven planning, architecture decision questionnaires, and end-to-end validation |

## How to Add a Skill to Cortex Code

### Option 1 — Reference the raw file URL directly in your prompt

Paste the raw GitHub URL of `SKILL.md` into your Cortex Code session:

```
Read the skill at:
https://raw.githubusercontent.com/sfc-gh-praj/cortex-skills/main/ssis-migration/SKILL.md

Follow this skill's workflow for my SSIS migration.
```

### Option 2 — Clone and load locally

```bash
git clone https://github.com/sfc-gh-praj/cortex-skills.git
```

Then in your Cortex Code session:
```
Read /path/to/cortex-skills/ssis-migration/SKILL.md
and follow its workflow for my SSIS migration.
```

### Option 3 — Install as a named Cortex Code skill

Copy the `ssis-migration` folder into your Cortex Code skills directory:

```bash
# macOS / Linux
cp -r cortex-skills/ssis-migration ~/.snowflake/cortex/skills/

# Then invoke it by name in any CoCo session:
# "Load the ssis-migration skill"
```

## Contributing

Each skill lives in its own folder. The folder name is the skill name. Each folder must contain a `SKILL.md` that defines the workflow, and optionally a `references/` folder with supporting knowledge files.

```
cortex-skills/
└── <skill-name>/
    ├── SKILL.md              ← skill definition (required)
    ├── README.md             ← usage instructions (recommended)
    └── references/           ← supporting knowledge files (optional)
        ├── snowflake_patterns.md
        ├── component_mapping_reference.md
        └── phase3_migration_plan_template.md
```

## Related

- Blog post: [Migrating SSIS to Snowflake with Cortex Code: A Framework That Actually Works](#)
- Cortex Code documentation: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
