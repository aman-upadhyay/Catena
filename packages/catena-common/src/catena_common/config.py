"""Static configuration values for Catena."""

from __future__ import annotations

from pathlib import Path

BASE_JOB_DIR = "/scratch/au152/agent_job"
BASE_STAGE_DIR = "/scratch/au152/catena_stage"

SLURM_PARTITION = "main"
SLURM_REQUEUE = True
SLURM_NODES = 1
SLURM_NTASKS = 1
SLURM_CPUS_PER_TASK = 50
SLURM_MEM_MB = 50000
SLURM_TIME = "70:00:00"
SLURM_ARRAY = "1"

CONDA_SH = "/home/au152/Software/miniconda3/etc/profile.d/conda.sh"
MG5_EXEC = "/home/au152/Software/MG/MG5_aMC_v3_5_15/bin/mg5_aMC"
DELPHES_HEPMC2_EXE = "/home/au152/Software/miniconda3/envs/DLPS/bin/DelphesHepMC2"
DELPHES_HEPMC3_EXE = "/home/au152/Software/miniconda3/envs/DLPS/bin/DelphesHepMC3"
DELPHES_EXE = DELPHES_HEPMC3_EXE
SRPA_EXEC = "/home/au152/Software/SRPA/sherpa/bin/Sherpa"
PYTHIA8_MAKEFILE_INC = str(Path(__file__).with_name("pythia8") / "Makefile.inc")
LHAPDF_DATA_PATH = "/home/au152/Software/miniconda3/envs/DLPS/share/LHAPDF"
LHAPDF_LOCK_DIR = "/home/au152/.cache/Catena/locks/catena_locks"

MG_ENV = "MG"
DLPS_ENV = "DLPS"
SRPA_ENV = "SRPA"
CATENA_ENV = "Catena"

REMOTE_HOST = "amarel.rutgers.edu"
REMOTE_USER = "au152"
REMOTE_SERVER_CMD = "/home/au152/Software/miniconda3/envs/Catena/bin/catena-server"
