#!/usr/bin/env python

"""
Example of how one could compute PIP weights in parallel.
"""

# ------------------------------------------------------------------------------
# IMPORTING LIBRARIES
#
# The different libraries are imported here.
# ------------------------------------------------------------------------------

#- Parse args first to enable --help on login nodes where MPI crashes
#from __future__ import absolute_import, division, print_function
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--survey_log", type=str, required=False,
                    help="Eventually we would pass in a file containing the log"
                    " of when each fiber assignment was run and for which tiles, "
                    "along with the options that were used.")

parser.add_argument("--sky", type=str, required=False, nargs="+",
                    help="Input file with sky or supp_sky targets.  "
                    "These target files are assumed to be constant and not "
                    "tracked by the MTL ledger.")

parser.add_argument("--mtl", type=str, required=True, nargs="+",
                    help="The MTL ledger.  This is still a work in progress and"
                    " I am not sure what the interface will be, but given the "
                    "fiber assignment dates in the survey log, we should be able"
                    " to get the MTL state at that time.  For now, this option"
                    " is just one or more target files.")

parser.add_argument("--footprint", type=str, required=False, default=None,
                    help="Optional FITS file defining the footprint.  If"
                    " not specified, the default footprint from desimodel"
                    " is used.")

parser.add_argument("--tiles", type=str, required=False, default=None,
                    help="Optional text file containing a subset of the"
                    " tile IDs to use in the footprint, one ID per line."
                    " Default uses all tiles in the footprint.")

parser.add_argument("--out", type=str, required=False, default=None,
                    help="Output directory.")

parser.add_argument("--realizations", type=int, required=False, default=10,
                    help="Number of realizations.")

args = parser.parse_args()

#- Then initialize MPI ASAP before proceeding with other imports
from mpi4py import MPI

import os
import sys

from bitarray import bitarray

import fitsio

import fiberassign

from fiberassign.utils import Logger

# This distribute_discrete function is not inside fiberassign. Commenting for now.
# (23/11/2020)
from fiberassign.utils import distribute_discrete

from fiberassign.hardware import load_hardware

from fiberassign.tiles import load_tiles

from fiberassign.targets import (
    Targets,
    TargetsAvailable,
    TargetTree,
    LocationsAvailable,
    load_target_file
)

from fiberassign.assign import Assignment, run

def main():
    log = Logger.get()
    
    # Variable for world communicator, number of MPI tasks and rank of the present MPI
    # process.
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    
    print('I am rank {} and this is the MTL file provided:\n {}'.format(args.mtl))

if __name__ == '__main__':
    main()