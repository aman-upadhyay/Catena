# Catena

Catena is a small command-line job launcher for running analysis jobs on
Amarel through SLURM. It is organized as a monorepo with separate client and
server install targets, plus shared schemas and path helpers.

The current implementation is intentionally file-based and CLI-driven. There
is no HTTP service, no database, and no runner dispatch beyond the Python
runner yet.

## Repository Layout

```text
packages/
  catena-common/
    src/catena_common/
      config.py       Shared constants for paths, conda envs, SLURM defaults, SSH defaults.
      jsonio.py       Small JSON read/write helpers.
      models.py       Pydantic v2 request/status/input models and validators.
      paths.py        Job, bundle, and staging path helpers.

  catena-client/
    src/catena_client/
      cli.py          Local Typer CLI used from Pascal3 or another login node.
      ssh.py          SSH command execution and remote JSON parsing.
      transfer.py     rsync/scp upload and fetch helpers.

  catena-server/
    src/catena_server/
      cli.py          Remote Typer CLI used on Amarel.
      registry.py     File-based job registry and state persistence.
      slurm.py        SLURM script rendering.
      bundle.py       Zip bundle creation and checksum metadata.
      runners/
        cpp.py        C++ task SLURM body builder.
        python_run.py Python task SLURM body builder.
```

## Installation

Install exactly one role package on each machine. The shared `catena_common`
package is included by each role target, so you do not manually install it.

Pascal3 or client machine:

```bash
python -m pip install -e packages/catena-client
```

Amarel or server machine:

```bash
python -m pip install -e packages/catena-server
```

## Fixed Runtime Paths

Catena currently uses static Rutgers/Amarel paths from
`catena_common.config`.

```text
Job root:       /scratch/au152/agent_job
Stage root:     /scratch/au152/catena_stage
Remote host:    amarel.rutgers.edu
Remote user:    au152
Remote server:  /home/au152/Software/miniconda3/envs/Catena/bin/catena-server
Conda source:   /home/au152/Software/miniconda3/etc/profile.d/conda.sh
Python env:     Catena
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

A request is JSON validated by Pydantic v2. `job_id` may contain only letters,
numbers, underscores, and dashes. `entry_file` must be relative. Input names
must be safe relative paths with no `..`.

Minimal inline Python job:

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

Large staged input reference:

```json
{
  "job_id": "large_input_job",
  "task_type": "python",
  "entry_file": "run.py",
  "input_files": [
    {"name": "run.py", "mode": "inline", "content_b64": "cHJpbnQoJ29rJykK"},
    {"name": "events.hepmc", "mode": "uploaded"}
  ]
}
```

Server-side file reference:

```json
{
  "job_id": "server_file_job",
  "task_type": "python",
  "entry_file": "run.py",
  "input_files": [
    {"name": "input.dat", "mode": "server_path", "path": "/scratch/au152/data/input.dat"}
  ]
}
```

## Client Commands

Run these from the client machine.

Submit a request over SSH. The client sends the request JSON through stdin to
the remote server command, so it does not rely on the remote non-interactive
shell PATH.

```bash
catena-client submit request.json
```

Check status:

```bash
catena-client status example_python_job
```

Watch until terminal state. Intermediate status lines go to stderr. The final
machine-readable JSON response is printed once to stdout.

```bash
catena-client watch example_python_job
catena-client watch example_python_job --interval 20
```

Upload large files to the server-side staging area before submit:

```bash
catena-client upload large_input_job events.hepmc weights.dat
```

Fetch a completed job bundle. This remotely runs `catena-server bundle JOB_ID`
first, then copies the zip back using rsync if available or scp as fallback.

```bash
catena-client fetch example_python_job
catena-client fetch example_python_job --dest results/example_python_job.zip
```

Use `--host` and `--user` on client commands to override the default SSH
target. `upload` and `fetch` also support `--progress/--no-progress`; progress
output is sent to stderr only.

## Server Commands

Run these on Amarel, normally through the absolute remote executable path used
by the client.

Submit a request file:

```bash
catena-server submit request.json
```

Submit from stdin:

```bash
cat request.json | catena-server submit -
```

Check status:

```bash
catena-server status example_python_job
```

Create or refresh a bundle:

```bash
catena-server bundle example_python_job
```

## What Submit Does

`catena-server submit` performs these steps:

1. Loads and validates the JSON request.
2. Rejects duplicate `job_id` values if the final job directory already exists.
3. Creates the job layout under `/scratch/au152/agent_job/{job_id}`.
4. Persists `job.json` and initializes `state.json`.
5. Materializes input files into `inputs/`.
6. Builds a SLURM script for supported task types.
7. Runs `sbatch --parsable /scratch/au152/agent_job/{job_id}/slurm.sh`.
8. Persists the returned SLURM job id and prints machine-readable JSON.

Implemented task types are `python` and `cpp`. Other task types are modeled
but return a clear not-implemented error.

## Python Runner Behavior

For Python jobs, the generated SLURM body:

- Defines `WORKDIR`, `INPUTS_DIR`, and `OUTPUTS_DIR`.
- Sources the configured conda initialization script.
- Activates the `Catena` conda environment.
- Changes directory into `WORKDIR/inputs`.
- Runs `python {entry_file} {cli_args...}` with shell-safe quoting.
- Prints success or failure markers.
- Copies newly created files from `inputs/` into `outputs/`.
- Leaves original input files in place.

## C++ Runner Behavior

For C++ jobs, the generated SLURM body:

- Defines `WORKDIR`, `INPUTS_DIR`, and `OUTPUTS_DIR`.
- Sources the configured conda initialization script.
- Activates the `Catena` conda environment.
- Changes directory into `WORKDIR/inputs`.
- Compiles `entry_file` to `job_exe` with `g++ -O2 -std=c++17`.
- Uses `root-config`, `CONDA_PREFIX/include`, `CONDA_PREFIX/lib`, and links
  `Delphes`, `cnpy`, and `z`.
- Prints the full compile command before running it.
- Preserves compiler stderr in `err.log` through the normal SLURM stderr path.
- Runs `./job_exe` with `cli_args`.
- Copies newly created files from `inputs/` into `outputs/`.
- Leaves original input files in place.

## Job Directory Layout

Each job gets:

```text
/scratch/au152/agent_job/{job_id}/
  inputs/
  outputs/
  bundle/
  job.json
  state.json
  slurm.sh
  out.log
  err.log
```

The bundle zip is written to:

```text
/scratch/au152/agent_job/{job_id}/bundle/{job_id}.zip
```

Staged uploads are kept separate until submit:

```text
/scratch/au152/catena_stage/{job_id}/
```

## State and Status

`state.json` is the file-based source of truth for local Catena state. It
stores:

- `job_id`
- `state`
- `active`
- `slurm_job_id`
- `job_dir`
- `message`
- `created_at`
- `updated_at`
- `submit_time`
- `finish_time`
- `last_update_time`
- `final_slurm_state`
- `bundle_path`

`catena-server status` reads the local state, queries `squeue` first, then
queries `sacct` if the job is no longer visible in `squeue`. SLURM states are
mapped into Catena states:

```text
PENDING   -> PENDING
RUNNING   -> RUNNING
COMPLETED -> COMPLETED
FAILED    -> FAILED
CANCELLED -> CANCELLED
TIMEOUT   -> FAILED
unknown   -> UNKNOWN
```

Active states are `SUBMITTED`, `PENDING`, and `RUNNING`.

## Bundle Metadata

`catena-server bundle JOB_ID` zips the job directory while avoiding recursive
inclusion of the zip itself. The response includes:

```json
{
  "job_id": "example_python_job",
  "job_dir": "/scratch/au152/agent_job/example_python_job",
  "zip_path": "/scratch/au152/agent_job/example_python_job/bundle/example_python_job.zip",
  "zip_size_bytes": 12345,
  "zip_sha256": "64_character_sha256_hex_digest",
  "message": "bundle created"
}
```

## Error Handling

Commands print machine-readable JSON on stdout. On failure, commands exit
nonzero and include a `message`. Where practical, errors also include an
`error_type`, such as:

```text
invalid_input
ssh_failure
remote_response_error
transfer_failure
state_read_failure
```

Transfer progress, watch updates, and SSH transfer progress are kept on stderr
so stdout remains parseable JSON.

## Development Checks

Useful local checks:

```bash
python -m py_compile packages/catena-common/src/catena_common/*.py \
  packages/catena-client/src/catena_client/*.py \
  packages/catena-server/src/catena_server/*.py \
  packages/catena-server/src/catena_server/runners/*.py

PYTHONPATH=packages/catena-common/src:packages/catena-client/src:packages/catena-server/src \
  python -c "import catena_common, catena_client.cli, catena_server.cli; print('ok')"

catena-client --help
catena-server --help
```

## Not Implemented Yet

- Real runner implementations for MG5+Pythia, Delphes, or Sherpa.
- HTTP or daemon server mode.
- SQLite or another database backend.
- Artifact manifests beyond bundle zip creation.
- Automatic runner-specific environment setup beyond the Python runner.
