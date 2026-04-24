# Catena Job Templates

This document gives a practical submission template for each implemented task
type. Each section includes:

- a minimal example input or generator/source file
- a matching `request.json`
- upload steps when relevant
- submit and watch commands

Notes:

- `sherpa` is modeled but not implemented, so it is not included here.
- These examples are intentionally small and informative, not production-grade.

## Python

### Example `run.py`

```python
from pathlib import Path
import sys

count = int(sys.argv[1]) if len(sys.argv) > 1 else 3
Path("summary.txt").write_text(f"count={count}\n", encoding="utf-8")
print("python job ran")
```

### Example `request.json`

```json
{
  "job_id": "python_demo01",
  "task_type": "python",
  "entry_file": "run.py",
  "cli_args": ["5"],
  "input_files": [
    {
      "name": "run.py",
      "mode": "uploaded"
    }
  ],
  "extra": {}
}
```

### Upload

```bash
catena-client upload python_demo01 run.py
```

### Submit

```bash
catena-client submit request.json
catena-client watch python_demo01
catena-client fetch python_demo01
```

## C++

### Example `main.cpp`

```cpp
#include <fstream>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
  std::string out = argc > 1 ? argv[1] : "result.txt";
  std::ofstream handle(out);
  handle << "hello from cpp\n";
  std::cout << "wrote " << out << std::endl;
  return 0;
}
```

### Example `request.json`

```json
{
  "job_id": "cpp_demo01",
  "task_type": "cpp",
  "entry_file": "main.cpp",
  "cli_args": ["result.txt"],
  "input_files": [
    {
      "name": "main.cpp",
      "mode": "uploaded"
    }
  ],
  "extra": {}
}
```

### Upload

```bash
catena-client upload cpp_demo01 main.cpp
```

### Submit

```bash
catena-client submit request.json
catena-client watch cpp_demo01
catena-client fetch cpp_demo01
```

## Delphes

### Example Delphes card `delphes_card.tcl`

```tcl
set ExecutionPath .
set MaxEvents 10
```

### Example request

This task also needs a HepMC file already prepared.

```json
{
  "job_id": "delphes_demo01",
  "task_type": "delphes",
  "entry_file": null,
  "cli_args": [],
  "input_files": [
    {
      "name": "delphes_card.tcl",
      "mode": "uploaded"
    },
    {
      "name": "events.hepmc",
      "mode": "uploaded"
    }
  ],
  "extra": {
    "delphes_card": "delphes_card.tcl",
    "hepmc_file": "events.hepmc",
    "out_root": "output.root"
  }
}
```

### Upload

```bash
catena-client upload delphes_demo01 delphes_card.tcl events.hepmc
```

### Submit

```bash
catena-client submit request.json
catena-client watch delphes_demo01
catena-client fetch delphes_demo01
```

## MG5 + Pythia

### Example `gen_mg5.txt`

```text
import model sm
generate p p > z j
output zj_smoke
launch zj_smoke
shower=Pythia8
done
```

### Example `run_card.dat`

```text
1000 = nevents
```

### Example `pythia8_card.dat`

```text
Main:numberOfEvents = 1000
```

### Example `request.json`

```json
{
  "job_id": "mg5_demo01",
  "task_type": "mg5_pythia",
  "entry_file": "gen_mg5.txt",
  "cli_args": [],
  "input_files": [
    {
      "name": "gen_mg5.txt",
      "mode": "uploaded"
    },
    {
      "name": "run_card.dat",
      "mode": "uploaded"
    },
    {
      "name": "pythia8_card.dat",
      "mode": "uploaded"
    }
  ],
  "extra": {}
}
```

### Upload

```bash
catena-client upload mg5_demo01 gen_mg5.txt run_card.dat pythia8_card.dat
```

### Submit

```bash
catena-client submit request.json
catena-client watch mg5_demo01
catena-client fetch mg5_demo01
```

## Pythia8

### Example `main_all.cc`

```cpp
#include "Pythia8/Pythia.h"

#include <iostream>

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "usage: " << argv[0] << " main_all.cmnd\n";
    return 2;
  }

  Pythia8::Pythia pythia;
  pythia.readFile(argv[1]);
  if (!pythia.init()) return 1;

  for (int i = 0; i < 10; ++i) {
    if (!pythia.next()) continue;
  }

  pythia.stat();
  return 0;
}
```

### Example `main_all.cmnd`

```text
Main:numberOfEvents = 10
Beams:idA = 2212
Beams:idB = 2212
Beams:eCM = 13000.
HardQCD:all = on
```

### Example `request.json`

```json
{
  "job_id": "pythia_demo01",
  "task_type": "pythia8",
  "entry_file": "main_all.cc",
  "cli_args": ["main_all.cmnd"],
  "input_files": [
    {
      "name": "main_all.cc",
      "mode": "uploaded"
    },
    {
      "name": "main_all.cmnd",
      "mode": "uploaded"
    }
  ],
  "extra": {
    "binary_name": "main_all"
  }
}
```

### Optional LHAPDF-enabled source fragment

```cpp
auto pdf = LHAPDF::mkPDF("NNPDF31_lo_as_0118", 0);
```

If the source contains an obvious hardcoded set name like this, Catena can:

- infer that LHAPDF is in use
- infer the set name
- auto-install the set by default if it is missing

### Upload

```bash
catena-client upload pythia_demo01 main_all.cc main_all.cmnd
```

### Submit

```bash
catena-client submit request.json
catena-client watch pythia_demo01
catena-client fetch pythia_demo01
```

## General Submission Pattern

Use this checklist for any task type:

1. Create a unique `job_id`.
2. Prepare the generator/source/card/input files.
3. Upload files that should be staged remotely:

```bash
catena-client upload JOB_ID FILE [FILE ...]
```

4. Write `request.json`.
5. Submit:

```bash
catena-client submit request.json
```

6. Watch:

```bash
catena-client watch JOB_ID
```

7. Fetch:

```bash
catena-client fetch JOB_ID
catena-client fetch JOB_ID --include-inputs
```

## Related Docs

- [User Guide](USER_GUIDE.md)
- [Implementation Notes](IMPLEMENTATION.md)
- [Usage Spec](USAGE_SPEC.yaml)
