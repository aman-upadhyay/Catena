# Catena Implementation Notes

This document describes what has been implemented so far and how the pieces
fit together. It is intended for future development and debugging.

## Package Responsibilities

`catena_common` contains code shared by the client and server install targets.
It must not depend on `catena_client` or `catena_server`.

`catena_client` contains local commands that run from Pascal3 or another
machine that can SSH to Amarel. The client never imports server code directly.
It calls the server through SSH and expects the remote command to return JSON.

`catena_server` contains the Amarel-side implementation. It owns the job
directory, file-based registry, SLURM script generation, SLURM status queries,
and bundle creation.

## Install Model

The repo intentionally uses two role install targets:

```bash
python -m pip install -e packages/catena-client
python -m pip install -e packages/catena-server
```

Each role target includes `catena_common` in its own build using setuptools
`package-dir` configuration. This avoids a fragile runtime dependency like
`catena-common @ file:../catena-common`, which pip can resolve relative to the
wrong working directory in editable installs.

## Data Model

The request and response models live in `catena_common.models`.

Implemented task types:

- `python`
- `cpp`

Modeled but not implemented task types:

- `mg5_pythia`
- `delphes`
- `sherpa`

Input file modes:

- `inline`: the request contains `content_b64`; the server decodes it into the
  final job `inputs/` directory.
- `uploaded`: the client previously uploaded the file into the staging area;
  the server copies it from staging into the final job `inputs/` directory.
- `server_path`: the request references an absolute path already present on the
  server; the server copies it into `inputs/`.

Important validators:

- `job_id` must match letters, numbers, underscore, and dash only.
- `entry_file` must not be absolute.
- input names must be safe relative paths and must not contain `..`.

## Server Submit Flow

`catena-server submit REQUEST` and `catena-server submit -` share the same
flow:

1. Load JSON and validate `JobRequest`.
2. Reject duplicate jobs if `/scratch/au152/agent_job/{job_id}` already exists.
3. Build the runner body for the requested task type.
4. Create the job directory layout.
5. Persist `state.json` first as `RECEIVED`, then as `PREPARING`.
6. Write `job.json`.
7. Materialize inline, uploaded, and server-path inputs into `inputs/`.
8. Write `slurm.sh`.
9. Run `sbatch --parsable slurm.sh`.
10. Persist `SUBMITTED` with the returned SLURM job id.
11. Print a `JobStatus` JSON payload.

If `sbatch` or script writing fails, state is persisted as `FAILED` and the
command exits nonzero.

## Server Status Flow

`catena-server status JOB_ID` reads `state.json` first. If a `slurm_job_id` is
present, it queries:

1. `squeue --noheader --format=%T --jobs JOB_ID`
2. `sacct --noheader --parsable2 --format=State --jobs JOB_ID`

`squeue` is preferred for active jobs. `sacct` is used when the job has left
the queue.

The server updates `state.json` whenever it observes useful new information.
It also refreshes `last_update_time` on each status call.

Terminal states are:

- `COMPLETED`
- `FAILED`
- `CANCELLED`

When a terminal SLURM state is observed, the server stores:

- `finish_time`
- `final_slurm_state`

## Persisted State Fields

`state.json` currently contains:

```text
job_id
state
active
slurm_job_id
job_dir
message
created_at
updated_at
submit_time
finish_time
last_update_time
final_slurm_state
bundle_path
```

`updated_at` and `last_update_time` currently move together. Both are kept for
readability and future compatibility.

## Bundle Flow

`catena-server bundle JOB_ID`:

1. Verifies the job directory exists.
2. Creates or refreshes `/scratch/au152/agent_job/{job_id}/bundle/{job_id}.zip`.
3. Avoids adding the zip file into itself on reruns.
4. Computes zip size in bytes.
5. Computes SHA-256 using the Python standard library.
6. Stores `bundle_path` in `state.json` when a state file exists.
7. Prints JSON with `job_id`, `job_dir`, `zip_path`, `zip_size_bytes`,
   `zip_sha256`, and `message`.

Direct server bundles include `inputs/` by default for compatibility. The
`--no-inputs` option omits files under `inputs/`.

## Client SSH Design

The client uses `subprocess.run` with SSH and a shell-quoted remote command
string. It invokes the absolute server executable:

```text
/home/au152/Software/miniconda3/envs/Catena/bin/catena-server
```

This avoids relying on `PATH` or conda activation in a non-interactive SSH
shell.

Client submit sends the request JSON through stdin:

```text
catena-server submit -
```

Client status and watch call:

```text
catena-server status JOB_ID
```

Client fetch calls:

```text
catena-server bundle JOB_ID --no-inputs
```

This keeps fetched archives focused on logs, metadata, and outputs instead of
downloading the original input files again.

## Transfer Design

Large-file upload is intentionally two-step:

1. `catena-client upload JOB_ID FILE...` copies local files into
   `/scratch/au152/catena_stage/{job_id}/`.
2. A later submit request references each uploaded file by name with
   `"mode": "uploaded"`.

This keeps partially uploaded files out of the final job directory until the
server validates and accepts a submit request.

Transfers prefer `rsync` and fall back to `scp` when available. Progress output
is sent to stderr only. Final JSON is always printed on stdout.

## Watch Design

`catena-client watch JOB_ID` polls remote status until either:

- the remote command exits nonzero, or
- the returned JSON has `"active": false`.

The default interval is 20 seconds. Intermediate updates are human-readable
and go to stderr. The final status JSON is printed once to stdout.

## Error Handling Design

Commands try to preserve machine-readable stdout even on failure.

Client-side error categories:

- `invalid_input`: bad local file path, invalid job id, invalid JSON request.
- `ssh_failure`: SSH failed before returning usable server output.
- `remote_response_error`: remote output was empty or not JSON.
- `transfer_failure`: rsync/scp or remote staging directory setup failed.

Server-side error categories:

- `invalid_input`: invalid job id or invalid request JSON.
- `state_read_failure`: `state.json` exists but cannot be read or parsed.

Some older response shapes are intentionally preserved. For example,
submit/status still return the same `JobStatus` fields on success.

## C++ Runner

`catena_server.runners.cpp` builds a SLURM body for `task_type="cpp"`.

The runner compiles from `WORKDIR/inputs` and writes the executable as
`job_exe`. It logs the full compile command to stdout, while compiler stderr
flows to the SLURM `err.log` path. A compile failure exits nonzero immediately,
which lets SLURM and Catena status mark the job as failed.

The compile pattern is:

```bash
g++ -O2 -std=c++17 main.cpp \
    $(root-config --cflags) \
    -I"$CONDA_PREFIX/include" -L"$CONDA_PREFIX/lib" \
    -Wl,-rpath,$CONDA_PREFIX/lib \
    $(root-config --libs) \
    -lDelphes -lcnpy -lz \
    -o job_exe
```

After a successful compile, the runner executes `./job_exe` with request
`cli_args` and copies newly created files from `inputs/` to `outputs/`.

## Known Limitations

- Python and C++ jobs are executable today.
- Python jobs copy newly created files from `inputs/` to `outputs/`, but this
  is a simple file comparison and not a full artifact manifest.
- There is no retry or cancellation command yet.
- There is no database or locking mechanism around concurrent submits with the
  same `job_id`.
- SLURM state mapping is deliberately small and should be expanded as needed.
