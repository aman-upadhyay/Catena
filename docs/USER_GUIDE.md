# Catena User Guide

This is the primary human-facing guide for using Catena. It keeps operational
detail out of the GitHub front-page README while preserving all of the concrete
usage information.

## Overview

Catena is a CLI/SSH job launcher for Amarel. The client runs locally and the
server runs remotely. Jobs, staging, state, logs, and bundles are all stored on
the filesystem.

Design constraints:

- no HTTP service
- no database backend
- no daemon
- JSON request/response model
- Typer CLIs on both client and server

## Repository Layout

```text
packages/
  catena-common/
    src/catena_common/
      config.py
      jsonio.py
      models.py
      paths.py

  catena-client/
    src/catena_client/
      cli.py
      ssh.py
      transfer.py

  catena-server/
    src/catena_server/
      cli.py
      registry.py
      slurm.py
      bundle.py
      runners/
        cpp.py
        delphes.py
        mg5_pythia.py
        pythia8.py
        python_run.py
```

## Installation

Install exactly one role package per machine.

Client machine:

```bash
python -m pip install -e packages/catena-client
```

Server machine:

```bash
python -m pip install -e packages/catena-server
```

## Fixed Runtime Paths

Catena currently uses fixed Rutgers/Amarel-specific paths from
`catena_common.config`.

```text
Job root:         /scratch/au152/agent_job
Stage root:       /scratch/au152/catena_stage
Remote host:      amarel.rutgers.edu
Remote user:      au152
Remote server:    /home/au152/Software/miniconda3/envs/Catena/bin/catena-server
Conda source:     /home/au152/Software/miniconda3/etc/profile.d/conda.sh
Catena env:       Catena
MG env:           MG
DLPS env:         DLPS
LHAPDF data path: /home/au152/Software/miniconda3/envs/DLPS/share/LHAPDF
```

SLURM defaults:

```text
partition=main
requeue=True
nodes=1
ntasks=1
cpus_per_task=50
mem_mb=50000
time=70:00:00
array=1
```

## Job Request Format

Requests are validated by `catena_common.models.JobRequest`.

Rules:

- `job_id` may only contain letters, numbers, underscores, and dashes.
- `entry_file` must be relative.
- input file names must be safe relative paths.
- no `..` path traversal.

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

## Input Modes

### `inline`

The request contains base64 content:

```json
{
  "name": "run.py",
  "mode": "inline",
  "content_b64": "..."
}
```

### `uploaded`

The file must already exist in `/scratch/au152/catena_stage/{job_id}/`:

```json
{
  "name": "events.hepmc",
  "mode": "uploaded"
}
```

### `server_path`

The server copies the file from an absolute existing path:

```json
{
  "name": "input.dat",
  "mode": "server_path",
  "path": "/scratch/au152/data/input.dat"
}
```

## Client Commands

### Submit

```bash
catena-client submit request.json
catena-client submit request.json --host amarel.rutgers.edu --user au152
```

Behavior:

- validates the local request JSON
- sends it to the remote server over SSH via stdin
- prints machine-readable JSON to stdout

### Status

```bash
catena-client status JOB_ID
```

Behavior:

- asks the remote server for the current job status
- prints machine-readable JSON to stdout

### Watch

```bash
catena-client watch JOB_ID
catena-client watch JOB_ID --interval 10
```

Behavior:

- polls remote status until terminal
- intermediate lines go to stderr
- final JSON goes to stdout

### Upload

```bash
catena-client upload JOB_ID FILE [FILE ...]
catena-client upload JOB_ID large.hepmc --progress
```

Behavior:

- ensures the remote stage directory exists
- prefers `rsync`, falls back to `scp`
- prints transfer progress to stderr only
- prints machine-readable JSON to stdout

### Fetch

```bash
catena-client fetch JOB_ID
catena-client fetch JOB_ID --include-inputs
catena-client fetch JOB_ID --dest outputs/job.zip
catena-client fetch JOB_ID --progress
```

Behavior:

- remotely runs `catena-server bundle`
- defaults to `--no-inputs`
- can include inputs explicitly with `--include-inputs`
- prints mode/progress info to stderr
- prints machine-readable JSON to stdout

### Jobs

```bash
catena-client jobs
```

Behavior:

- calls remote `catena-server jobs`
- renders a compact table locally

### Delete

```bash
catena-client delete JOB_ID
catena-client delete JOB_ID --force-cancel
```

Behavior:

- deletes an inactive job
- active jobs require `--force-cancel`

### Stages

```bash
catena-client stages
```

Behavior:

- calls remote `catena-server stages`
- renders a compact table locally

### Stage Tree

```bash
catena-client stage-tree JOB_ID
catena-client stage-tree JOB_ID --depth 1
```

Behavior:

- prints a pruned staging tree view

### Clear Stage

```bash
catena-client clear-stage JOB_ID
```

Behavior:

- deletes one staging directory remotely

## Server Commands

### Submit

```bash
catena-server submit request.json
cat request.json | catena-server submit -
```

### Status

```bash
catena-server status JOB_ID
```

### Bundle

```bash
catena-server bundle JOB_ID
catena-server bundle JOB_ID --no-inputs
```

### Jobs

```bash
catena-server jobs
```

### Delete

```bash
catena-server delete JOB_ID
catena-server delete JOB_ID --force-cancel
```

### Stages

```bash
catena-server stages
```

### Stage Tree

```bash
catena-server stage-tree JOB_ID
catena-server stage-tree JOB_ID --depth 1
```

### Clear Stage

```bash
catena-server clear-stage JOB_ID
```

## Submit Flow

`catena-server submit` performs:

1. request validation
2. duplicate job check
3. job directory creation
4. `job.json` and `state.json` writes
5. input materialization into `inputs/`
6. SLURM script generation
7. `sbatch --parsable`
8. persisted `SUBMITTED` state

## State and Status

Persisted state includes:

- `job_id`
- `state`
- `active`
- `slurm_job_id`
- `job_dir`
- `message`
- `submit_time`
- `finish_time`
- `last_update_time`
- `final_slurm_state`
- `bundle_path`
- `failure_reason`
- `exit_code`

SLURM mappings:

```text
PENDING   -> PENDING
RUNNING   -> RUNNING
COMPLETED -> COMPLETED
FAILED    -> FAILED
CANCELLED -> CANCELLED
TIMEOUT   -> FAILED
unknown   -> UNKNOWN
```

## Bundle Behavior

Bundles are written to:

```text
/scratch/au152/agent_job/{job_id}/bundle/{job_id}.zip
```

Bundle metadata includes:

- `job_id`
- `job_dir`
- `zip_path`
- `zip_size_bytes`
- `zip_sha256`
- `message`

## Management Commands

Job listing fields:

- `job_id`
- `task_type`
- `state`
- `submit_time`
- `finish_time`
- `last_update_time`
- `message`
- `failure_reason`

Stage listing fields:

- `stage_id`
- `modified_time`
- `file_count`
- `total_size_bytes`

`stage-tree` returns a formatted tree string inside JSON on the server; the
client prints that tree directly.

## Task Types

Implemented:

- `python`
- `cpp`
- `delphes`
- `mg5_pythia`
- `pythia8`

See [Job Templates](JOB_TEMPLATES.md) for complete examples.

## Error Handling

Common client error types:

- `invalid_input`
- `ssh_failure`
- `remote_response_error`
- `transfer_failure`
- `error`

Common server error types:

- `invalid_input`
- `job_id_exists`
- `state_read_failure`
- `jobs_list_failure`
- `stages_list_failure`

## More Docs

- [Job Templates](JOB_TEMPLATES.md)
- [Implementation Notes](IMPLEMENTATION.md)
- [Usage Spec](USAGE_SPEC.yaml)
