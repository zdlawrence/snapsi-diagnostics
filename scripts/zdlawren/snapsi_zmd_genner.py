import os
import argparse
import intake

# Default locations - can modify these for your setup!
DEFAULT_OUTPUT_DIR = "/home/users/zdlawren/autoscripts" # where to store the auto-genned bash scripts
DEFAULT_PYTHON_LOC = "/home/users/zdlawren/mambaforge/envs/py311/bin/python" # absolute path to python 
DEFAULT_ZMD_LOC = "/home/users/zdlawren/snapsi-diagnostics/scripts/zdlawren/zmd_snapsi.py" # absolute path to the zmd_snapsi.py script

# Set up the argument parser
parser = argparse.ArgumentParser()
parser.add_argument("institution", type=str, help="institution_id")
parser.add_argument("subexperiment", type=str, help="sub_experiment_id")
parser.add_argument("--outdir", type=str, default=DEFAULT_OUTPUT_DIR)
parser.add_argument("--python", type=str, default=DEFAULT_PYTHON_LOC)
parser.add_argument("--zmd", type=str, default=DEFAULT_ZMD_LOC)
parser.add_argument("--submit", action="store_true", help="submit with sbatch")
args = parser.parse_args()

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
