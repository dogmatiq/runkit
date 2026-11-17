# Schema upgrades

These flowcharts show what happens to the database schema when an application
built with runkit applies it, and what to do at each outcome.

An application applies online upgrades automatically, by calling
`ApplySchemaOnline()` as it starts. Every instance does this, while other
instances keep running. An upgrade that raises the major number of the schema
version has to be applied offline, with every instance stopped. The
application's developer provides a separate way to run it, such as a deployment
step that calls `ApplySchemaOffline()`, as [Major schema upgrades] explains.

## Applying the schema

Each instance of the application compares the database's schema version with the
schema version that its runkit version needs. A node that starts with 🛑 holds
the start of an error message that the application reports.

```mermaid
---
title: Apply Schema Flowchart
---
flowchart TD
	apply@{shape: circle, label: "Apply schema"}

	read@{shape: rect, label: "Read schema version"}
	exists@{shape: diamond, label: "Schema exists?"}
	majorAhead@{shape: diamond, label: "Database major number higher?"}
	migratingAhead@{shape: diamond, label: "Migrating to higher major version?"}
	satisfied@{shape: diamond, label: "At or above needed version?"}
	busy@{shape: diamond, label: "Another instance upgrading?"}
	wait@{shape: delay, label: "⏳ Wait for it"}
	upgrade@{shape: subproc, label: "Create or upgrade schema"}

	tooOld@{shape: doc, label: "🛑 incompatible database schema: … already at …"}
	inProgress@{shape: doc, label: "🛑 incompatible database schema: … in-progress migration …"}

	ready@{shape: stadium, label: "✅ Schema ready"}
	newer@{shape: stadium, label: "Deploy newer application version"}
	finish@{shape: stadium, label: "Finish with version that started it"}

	apply --> read
	read --> exists
	exists -->|No| busy
	exists -->|Yes| majorAhead
	majorAhead -->|Yes| tooOld
	majorAhead -->|No| migratingAhead
	migratingAhead -->|Yes| inProgress
	migratingAhead -->|No| satisfied
	satisfied -->|Yes| ready
	satisfied -->|No| busy
	busy -->|Yes| wait
	wait --> read
	busy -->|No| upgrade
	upgrade -->|Done| ready

	tooOld --> newer
	inProgress --> finish
```

## Creating or upgrading the schema

One instance at a time creates or upgrades the schema, and resumes where an
earlier attempt stopped. [Upgrading] says how to run an offline upgrade.

```mermaid
---
title: Create or Upgrade Schema Flowchart
---
flowchart TD
	upgrade@{shape: circle, label: "Create or upgrade schema"}

	migratingAhead@{shape: diamond, label: "Migration to higher version in progress?"}
	majorBehind@{shape: diamond, label: "Database major number lower?"}
	offline@{shape: diamond, label: "Offline upgrade?"}
	indexes@{shape: diamond, label: "Invalid indexes?"}
	exists@{shape: diamond, label: "Schema exists?"}
	create@{shape: rect, label: "Create schema"}
	inProgress@{shape: diamond, label: "Migration in progress?"}
	included@{shape: diamond, label: "In this runkit version?"}
	progress@{shape: diamond, label: "Recorded progress valid?"}
	resume@{shape: rect, label: "Resume after last completed step"}
	more@{shape: diamond, label: "Steps left?"}
	step@{shape: rect, label: "Apply next step"}
	failed@{shape: diamond, label: "Step failed?"}
	timedOut@{shape: diamond, label: "Timed out in online upgrade?"}
	record@{shape: rect, label: "Record new schema version"}

	inProgressErr@{shape: doc, label: "🛑 incompatible database schema: … in-progress migration …"}
	majorErr@{shape: doc, label: "🛑 major database schema upgrade required"}
	indexesErr@{shape: doc, label: "🛑 database schema migration requires human intervention"}
	progressErr@{shape: doc, label: "🛑 invalid migration progress in the database"}
	timeoutErr@{shape: doc, label: "🛑 database schema migration timed out"}
	stepErr@{shape: doc, label: "🛑 unable to apply step"}

	ready@{shape: stadium, label: "✅ Schema ready"}
	finish@{shape: stadium, label: "Finish with version that started it"}
	stopAll@{shape: stadium, label: "Stop all instances, upgrade offline"}
	fixIndexes@{shape: stadium, label: "Drop or rebuild each index manually"}
	report@{shape: stadium, label: "Report to runkit maintainers"}
	retry@{shape: stadium, label: "Fix cause, apply again"}

	upgrade --> migratingAhead
	migratingAhead -->|Yes| inProgressErr
	migratingAhead -->|No| majorBehind
	majorBehind -->|Yes| offline
	majorBehind -->|No| indexes
	offline -->|No| majorErr
	offline -->|Yes| indexes
	indexes -->|Yes| indexesErr
	indexes -->|No| exists
	exists -->|No| create
	exists -->|Yes| inProgress
	create --> record
	inProgress -->|Yes| included
	inProgress -->|No| more
	included -->|No| inProgressErr
	included -->|Yes| progress
	progress -->|No| progressErr
	progress -->|Yes| resume
	resume --> more
	more -->|Yes| step
	more -->|No| record
	step --> failed
	failed -->|No| more
	failed -->|Yes| timedOut
	timedOut -->|Yes| timeoutErr
	timedOut -->|No| stepErr
	record --> ready

	inProgressErr --> finish
	majorErr --> stopAll
	timeoutErr --> stopAll
	indexesErr --> fixIndexes
	progressErr --> report
	stepErr --> retry
```

<!-- references -->

[major schema upgrades]: ../README.md#major-schema-upgrades
[upgrading]: ../README.md#upgrading
