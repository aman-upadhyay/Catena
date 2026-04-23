# PYTHIA configuration file.
# Generated on Wed Feb 19 10:46:21 AM UTC 2025 with the user supplied options:
# --with-python-include=/home/au152/Software/miniconda3/envs/DLPS/include/python3.13
# --with-python-bin=/home/au152/Software/miniconda3/envs/DLPS/bin/
# --arch=LINUX
# --prefix=/home/au152/Software/miniconda3/envs/DLPS
# --with-lhapdf6=/home/au152/Software/miniconda3/envs/DLPS
# --with-fastjet3=/home/au152/Software/miniconda3/envs/DLPS
# --with-hepmc2=/home/au152/Software/miniconda3/envs/DLPS
# --with-hepmc3=/home/au152/Software/miniconda3/envs/DLPS
# --with-gzip=/home/au152/Software/miniconda3/envs/DLPS
# --with-mg5mes

# Install directory prefixes.
PREFIX_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin
PREFIX_INCLUDE=/home/au152/Software/miniconda3/envs/DLPS/include
PREFIX_LIB=/home/au152/Software/miniconda3/envs/DLPS/lib
PREFIX_SHARE=/home/au152/Software/miniconda3/envs/DLPS/share/Pythia8

# Compilation flags (see ./configure --help for further documentation).
CXX=/home/au152/Software/miniconda3/envs/DLPS/bin/x86_64-conda-linux-gnu-c++
CXX_DTAGS=-Wl,--disable-new-dtags
CXX_COMMON=-O2 -std=c++11 -pedantic -W -Wall -Wshadow -fPIC -pthread -DGZIP -I/home/au152/Software/miniconda3/envs/DLPS/include
CXX_SHARED=-shared
CXX_SONAME=-Wl,-soname,
LIB_SUFFIX=.so
OBJ_COMMON=

EVTGEN_USE=false
EVTGEN_CONFIG=
EVTGEN_BIN=
EVTGEN_INCLUDE=
EVTGEN_LIB=

FASTJET3_USE=true
FASTJET3_CONFIG=fastjet-config
FASTJET3_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
FASTJET3_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include
FASTJET3_LIB=-L/home/au152/Software/miniconda3/envs/DLPS/lib -Wl,-rpath,/home/au152/Software/miniconda3/envs/DLPS/lib -lfastjet

HEPMC2_USE=true
HEPMC2_CONFIG=
HEPMC2_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
HEPMC2_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include
HEPMC2_LIB=-L/home/au152/Software/miniconda3/envs/DLPS/lib -Wl,-rpath,/home/au152/Software/miniconda3/envs/DLPS/lib -lHepMC

HEPMC3_USE=true
HEPMC3_CONFIG=HepMC3-config
HEPMC3_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
HEPMC3_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include
HEPMC3_LIB=-L/home/au152/Software/miniconda3/envs/DLPS/lib -Wl,-rpath,/home/au152/Software/miniconda3/envs/DLPS/lib -lHepMC3

LHAPDF5_USE=false
LHAPDF5_CONFIG=
LHAPDF5_BIN=
LHAPDF5_INCLUDE=
LHAPDF5_LIB=

LHAPDF6_USE=true
LHAPDF6_CONFIG=lhapdf-config
LHAPDF6_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
LHAPDF6_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include
LHAPDF6_LIB=-L/home/au152/Software/miniconda3/envs/DLPS/lib -Wl,-rpath,/home/au152/Software/miniconda3/envs/DLPS/lib -lLHAPDF

POWHEG_USE=false
POWHEG_CONFIG=
POWHEG_BIN=
POWHEG_INCLUDE=
POWHEG_LIB=

RIVET_USE=false
RIVET_CONFIG=
RIVET_BIN=
RIVET_INCLUDE=
RIVET_LIB=

ROOT_USE=false
ROOT_CONFIG=
ROOT_BIN=
ROOT_INCLUDE=
ROOT_LIB=

GZIP_USE=true
GZIP_CONFIG=
GZIP_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
GZIP_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include
GZIP_LIB=-L/home/au152/Software/miniconda3/envs/DLPS/lib -Wl,-rpath,/home/au152/Software/miniconda3/envs/DLPS/lib -lz

PYTHON_USE=true
PYTHON_CONFIG=python-config
PYTHON_BIN=/home/au152/Software/miniconda3/envs/DLPS/bin/
PYTHON_INCLUDE=-I/home/au152/Software/miniconda3/envs/DLPS/include/python3.13
PYTHON_LIB=-L -Wl,-rpath,

MG5MES_USE=true
MG5MES_CONFIG=
MG5MES_BIN=plugins/mg5mes/
MG5MES_INCLUDE=
MG5MES_LIB=

OPENMP_USE=false
OPENMP_CONFIG=
OPENMP_BIN=
OPENMP_INCLUDE=
OPENMP_LIB=

MPICH_USE=false
MPICH_CONFIG=
MPICH_BIN=
MPICH_INCLUDE=
MPICH_LIB=

HDF5_USE=false
HDF5_CONFIG=
HDF5_BIN=
HDF5_INCLUDE=
HDF5_LIB=

HIGHFIVE_USE=false
HIGHFIVE_CONFIG=
HIGHFIVE_BIN=
HIGHFIVE_INCLUDE=
HIGHFIVE_LIB=
