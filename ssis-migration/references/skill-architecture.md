# SSIS Migration Skill Architecture

## How `snowflake-migration:migration` and `ssis-migration` relate

### The plugin does handle SSIS — via `migrate-etl-package`

The `snowflake-migration:migration` plugin's skill match table includes `migrate-etl-package`:
a 345-line, heavily engineered sub-skill with parallel AI agents, TDD-based fix loops, a ROADMAP
planner, and platform-specific SSIS handling (also supports Informatica and others).

### They operate at different stages of the migration

**Plugin path (SnowConvert-first):**
```
.dtsx files
    ↓
SnowConvert (automated conversion)
    ↓ produces converted SQL with EWI/FDM gaps
migrate-etl-package  ← plugin's own SSIS handler
    ↓ fixes SnowConvert gaps via TDD
Done
```

**ssis-migration path (full lifecycle):**
```
.dtsx files
    ↓
ssis-migration (Phase 1 → 2 → 3 → 4 → 5)
    ↓ Phase 4 calls migrate-etl-package as a sub-skill
Done
```

### What each skill covers

| Capability | `migrate-etl-package` (plugin) | `ssis-migration` (local) |
|---|---|---|
| Assessment + scoring | No | Yes — Phase 1 |
| Migration planning (user selects orchestration, dbt, file strategy) | No | Yes — Phase 2 |
| Detailed mapping → MIGRATION_PLAN.md | No | Yes — Phase 3 |
| TDD-based SnowConvert gap fixing | Yes (core purpose) | Yes — delegates to migrate-etl-package in Phase 4 |
| E2E validation and testing | No | Yes — Phase 5 |
| Multi-platform (SSIS, Informatica) | Yes | SSIS only |
| Requires SnowConvert output | Yes | Optional |

### The actual gap

The `snowflake-migration:migration` plugin's top-level routing (`migration_status` → `migrate-objects`)
is designed for **database objects** (tables, views, stored procedures). If a user says
"migrate my SSIS packages" to the plugin, it routes through `migrate-objects` → `migrate-etl-package`.
But the plugin does not surface `ssis-migration`'s planning phases — those only exist in the local skill.

### Summary

The two skills are **complementary, not competing**:

- Use `migrate-etl-package` (via the plugin) when SnowConvert output already exists and you need
  to fix EWI/FDM gaps using TDD.
- Use `ssis-migration` when you need the full lifecycle — assessment, architecture planning,
  MIGRATION_PLAN.md, and then TDD fixing — for an end-to-end guided migration.

`ssis-migration` actually **depends on** the plugin's `migrate-etl-package` in Phase 4.
