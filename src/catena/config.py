"""Static configuration values for Catena."""

from __future__ import annotations

BASE_JOB_DIR = "/scratch/au152/agent_job"

SLURM_PARTITION = "main"
SLURM_REQUEUE = True
SLURM_NODES = 1
SLURM_NTASKS = 1
SLURM_CPUS_PER_TASK = 50
SLURM_MEM_MB = 50000
SLURM_TIME = "70:00:00"
SLURM_ARRAY = "1"

CONDA_SH = "/home/au152/Software/miniconda3/etc/profile.d/conda.sh"
MG5_EXEC = "/home/au152/Software/MG5_aMC_v3_5_8/bin/mg5_aMC"
DELPHES_EXE = "/home/au152/Software/miniconda3/envs/DLPS/bin/DelphesHepMC2"
SRPA_EXEC = "/home/au152/Software/SRPA/sherpa/bin/Sherpa"

MG_ENV = "MG"
DLPS_ENV = "DLPS"
SRPA_ENV = "SRPA"
CATENA_ENV = "Catena"

REMOTE_HOST = "amarel.rutgers.edu"
REMOTE_USER = "au152"
REMOTE_SERVER_CMD = "catena-server"
