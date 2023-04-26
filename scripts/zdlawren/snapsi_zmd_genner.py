""" This python script automatically generates a series of 
bash scripts for computing zonal mean datasets of the SNAPSI
data, which can be submitted thru SLURM with sbatch. 

It requires commandline arguments specifying the 
institution_id (model center) and sub_experiment_id (init date) as
targets; from there it will generate scripts for every available 
experiment listed in the SNAPSI intake catalog (nominally, 
at least 'control', 'nudged', and 'free')

This is sort of a kludgey way to accomplish this task, but I 
am not very familiar with SLURM and how to pass specific args 
to scripts.

Original Author: Z. D. Lawrence
Last modified: 2023-04-25
"""

import os
import pathlib
import argparse
import intake

USER_HOME = pathlib.Path("~/").expanduser()

# Default locations - can modify these for your setup!
# The default python location in particular is specific to zdlawren's setup!
DEFAULT_OUTPUT_DIR = USER_HOME / "autoscripts"
DEFAULT_PYTHON_LOC = USER_HOME / "mambaforge/envs/py311/bin/python" 
DEFAULT_ZMD_LOC = USER_HOME / "snapsi-diagnostics/scripts/zdlawren/zmd_snapsi.py"

# Set up the argument parser
parser = argparse.ArgumentParser()
parser.add_argument("institution", type=str, help="institution_id")
parser.add_argument("subexperiment", type=str, help="sub_experiment_id")
parser.add_argument("--outdir", type=str, default=str(DEFAULT_OUTPUT_DIR))
parser.add_argument("--python", type=str, default=str(DEFAULT_PYTHON_LOC))
parser.add_argument("--zmd", type=str, default=str(DEFAULT_ZMD_LOC))
parser.add_argument("--submit", action="store_true", help="submit with sbatch")
args = parser.parse_args()

# iff the output directory is the default, let's ensure we have a location to 
# put the scripts. only do this for the default location
if args.outdir == str(DEFAULT_OUTPUT_DIR):
    DEFAULT_OUTPUT_DIR.mkdir(exist_ok=True)

# Open the Intake catalog and subset on the institution and subexperiment
catalog = intake.open_esm_datastore("/gws/nopw/j04/snapsi/test-snapsi-catalog.json")
subset = catalog.search(
    variable_id=["ua", "va", "ta", "zg", "wap"],
    institution_id=args.institution,
    sub_experiment_id=args.subexperiment,
)

# Get list of available experiment IDs, and iterate over them
experiment_ids = subset.df.experiment_id.unique()
for eid in experiment_ids:
    # Auto-genned script name
    scripts_fi = f"{args.institution}_{args.subexperiment}_{eid}.sh"
    
    # Content that goes into the file
    print(f"Generating {args.outdir}/{scripts_fi}")
    text_for_script = "#!/bin/bash\n\n"
    text_for_script += f"#SBATCH --job-name=\"{args.institution}_{args.subexperiment}_{eid}\"\n"
    text_for_script += "#SBATCH --mem=32G\n"
    text_for_script += "#SBATCH --time=04:00:00\n\n"
    text_for_script += f"{args.python} -u {args.zmd} {args.institution} {args.subexperiment} {eid}\n"

    # Write string to file and chmod it for use
    with open(f"{args.outdir}/{scripts_fi}", "w") as fi:
        fi.write(text_for_script)
    chmod =  f"chmod u+x {args.outdir}/{scripts_fi}"
    os.system(chmod)

    # Submit the jobs with sbatch if desired
    if args.submit is True:
        cmd = f"sbatch {args.outdir}/{scripts_fi}"
        print(f"Submitting `{cmd}`")
        os.system(cmd)
