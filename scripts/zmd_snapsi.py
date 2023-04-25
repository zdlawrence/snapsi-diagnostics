import argparse
import pathlib
from datetime import datetime

import intake
import numpy as np

from pyzome.recipes import create_zonal_mean_dataset

# Set up argument parser
parser = argparse.ArgumentParser()
parser.add_argument("institution", type=str, help="institution_id")
parser.add_argument("subexperiment", type=str, help="sub_experiment_id")
parser.add_argument("experiment", type=str, help="experiment_id")
args = parser.parse_args()

# Pull args into variables for convenience
institution_id = args.institution
experiment_id = args.experiment
sub_experiment_id = args.subexperiment

# Open the Intake catalog and subset to the specific model, init, and experiment
catalog = intake.open_esm_datastore("/gws/nopw/j04/snapsi/test-snapsi-catalog.json")
subset = catalog.search(
    variable_id=["ua", "va", "ta", "zg", "wap"],
    institution_id=args.institution, 
    sub_experiment_id=sub_experiment_id,
    experiment_id=experiment_id
)

# Setup the output directory
output_dir = pathlib.Path(f"/gws/nopw/j04/snapsi/zmd/{institution_id}/{sub_experiment_id}/{experiment_id}/")
output_dir.mkdir(parents=True, exist_ok=True)

# Convert our query into a dataset and rename data_vars 
# to names that pyzome expects
ds = subset.to_dask()
print(ds)
ds = ds.rename({"ua":"u", "va":"v", "wap":"w", "ta":"T", "zg": "Z"})

# Iterate over ensemble members
for member in ds.member_id.values:
    print(f"Now working on {member} for {institution_id} {experiment_id} {sub_experiment_id}")
    zmd = create_zonal_mean_dataset(
        ds.sel(member_id=member),
        verbose=True, 
        include_waves=True, 
        waves=[1,2,3], 
        fftpkg="xrft"
    )
    
    # Set up encoding dictionary to ensure everything gets saved as
    # float32 (with no compression to ensure dask chunking can be used on output)
    comp = dict(dtype="float32")
    encoding = {var: comp for var in zmd.data_vars}
    
    # Set up the output name/path
    output_file = f"{institution_id}_{sub_experiment_id}_{experiment_id}_{member}_zmd.nc"
    output_path = f"{str(output_dir)}/{output_file}"

    # Since we're using dask for everything, no actual computations 
    # are done until the to_netcdf method is called
    print(f"Saving to {output_path}")
    zmd.attrs['history'] = f'Created by zdlawren using pyzome on {datetime.utcnow()}'
    zmd.to_netcdf(output_path, encoding=encoding)

