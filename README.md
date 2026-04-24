# Catena

Catena is a small command-line job launcher for running analysis jobs on
Amarel through SLURM. It is organized as a monorepo with separate client and
server install targets, plus shared schemas and path helpers.

The current implementation is intentionally file-based and CLI-driven. There
is no HTTP service and no database backend. Job state, staging, and bundles are
managed directly on the filesystem through the client and server CLIs.

## What Catena Does

Catena provides a thin CLI workflow for:

- staging files on Amarel
- submitting validated job requests over SSH
- generating SLURM scripts on the server
- polling status and failure reasons
- bundling and fetching results
- managing job directories and staging areas

Implemented task types:

- `python`
- `cpp`
- `delphes`
- `mg5_pythia`
- `pythia8`

## Repository Layout

```text
packages/
  catena-common/   Shared models, config, JSON helpers, and path helpers
  catena-client/   Client CLI used from Pascal3 or another login node
  catena-server/   Server CLI used on Amarel

docs/
  USER_GUIDE.md    Detailed user-facing workflow and command reference
  JOB_TEMPLATES.md End-to-end submit templates for every implemented task type
  IMPLEMENTATION.md Internal implementation notes and behavior details
  USAGE_SPEC.yaml  Machine-readable usage spec for agents and automation
```

## Installation

Install exactly one role package per machine. Each role includes the shared
`catena_common` package, so there is no separate common install step.

Pascal3 or another client machine:

```bash
python -m pip install -e packages/catena-client
```

Amarel or the server-side environment:

```bash
python -m pip install -e packages/catena-server
```

## Quick Start

1. Upload any large input files to the staging area:

```bash
catena-client upload my_job input.dat extra.dat
```

2. Submit a validated request JSON:

```bash
catena-client submit request.json
```

3. Watch until the job reaches a terminal state:

```bash
catena-client watch my_job
```

4. Fetch the result bundle:

```bash
catena-client fetch my_job
catena-client fetch my_job --include-inputs
```

By default, `fetch` uses `--no-inputs` so the downloaded bundle excludes the
staged `inputs/` tree unless you explicitly ask for it.

## Common Commands

Client-side:

```bash
catena-client submit request.json
catena-client status JOB_ID
catena-client jobs
catena-client watch JOB_ID --interval 10
catena-client upload JOB_ID FILE [FILE ...]
catena-client fetch JOB_ID [--include-inputs]
catena-client delete JOB_ID [--force-cancel]
catena-client stages
catena-client stage-tree JOB_ID [--depth 2]
catena-client clear-stage JOB_ID
```

Server-side:

```bash
catena-server submit request.json
catena-server status JOB_ID
catena-server bundle JOB_ID [--no-inputs]
catena-server jobs
catena-server delete JOB_ID [--force-cancel]
catena-server stages
catena-server stage-tree JOB_ID [--depth 2]
catena-server clear-stage JOB_ID
```

## Request Model Summary

Requests are JSON validated by Pydantic v2.

- `job_id` allows only letters, numbers, `_`, and `-`
- `entry_file` must be relative
- input names must be safe relative paths with no `..`
- input modes are `inline`, `uploaded`, and `server_path`

Minimal example:

```json
{
  "job_id": "example_python_job",
  "task_type": "python",
  "entry_file": "run.py",
  "cli_args": ["--events", "100"],
  "input_files": [
    {
      "name": "run.py",
      "mode": "inline",
      "content_b64": "cHJpbnQoJ2hlbGxvIGZyb20gQ2F0ZW5hJykK"
    }
  ],
  "extra": {}
}
```

## Documentation

Use these docs depending on what you need:

- [docs/USER_GUIDE.md](docs/USER_GUIDE.md): full user guide, command behavior, paths, bundles, states, and management workflows
- [docs/JOB_TEMPLATES.md](docs/JOB_TEMPLATES.md): submission templates for `python`, `cpp`, `delphes`, `mg5_pythia`, and `pythia8`
- [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md): implementation details and current design notes
- [docs/USAGE_SPEC.yaml](docs/USAGE_SPEC.yaml): machine-readable usage spec for agents, automation, and tooling

## Notes

- Client/server communication is SSH-based; there is no HTTP service.
- Server commands emit machine-readable JSON.
- Client commands either forward JSON or render compact tables and trees locally.
- Transfer progress and watch updates go to `stderr`, keeping `stdout` parseable.
- Job state, staging, and bundles are file-based; there is no database backend yet.
