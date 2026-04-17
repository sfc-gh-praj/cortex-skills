# Cortex Skills

A collection of custom skills for [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) — Snowflake's AI coding agent.

Each skill encodes a domain-specific workflow that Cortex Code follows when you load it into a session. Skills enforce phase gates, gather required inputs, reference curated knowledge, and drive structured multi-step processes that plain prompting cannot reliably replicate.

## Available Skills

| Skill | Description |
|-------|-------------|
| [ssis-migration](./ssis-migration/) | 5-phase SSIS-to-Snowflake migration framework with Human-in-the-Loop gates, EWI-driven planning, architecture decision questionnaires, and end-to-end validation |

## How to Add a Skill to Cortex Code

### Option 1 — Reference the raw file URL directly in your prompt

Paste the raw GitHub URL of `requirements.md` into your Cortex Code session and ask it to follow the skill:

```
Use the skill at https://raw.githubusercontent.com/sfc-gh-praj/cortex-skills/main/ssis-migration/requirements.md
and follow its workflow for my SSIS migration.
```

### Option 2 — Download and load locally

```bash
# Clone the repo
git clone https://github.com/sfc-gh-praj/cortex-skills.git

# Point Cortex Code at the local file
# In your CoCo session:
# "Read the skill at /path/to/cortex-skills/ssis-migration/requirements.md and follow its workflow"
```

### Option 3 — Copy into your project

Copy `ssis-migration/requirements.md` into your SSIS project folder. Cortex Code will pick it up when you reference it in the session.

## Contributing

Each skill lives in its own folder. The folder name is the skill name. Each folder must contain a `requirements.md` file that defines the skill workflow.

```
cortex-skills/
└── <skill-name>/
    └── requirements.md   ← skill definition (required)
```

## Related

- Blog post: [Migrating SSIS to Snowflake with Cortex Code: A Framework That Actually Works](#)
- Cortex Code documentation: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
