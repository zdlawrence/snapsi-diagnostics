import argparse
import pathlib
from datetime import datetime

import intake
import numpy as np

from pyzome.recipes import create_zonal_mean_dataset

parser = argparse.ArgumentParser()
parser.add_argument("institution", type=str, help="institution_id")
parser.add_argument("subexperiment", type=str, help="sub_experiment_id")
parser.add_argument("experiment", type=str, help="experiment_id")
args = parser.parse_args()

institution_id = args.institution
experiment_id = args.experiment
sub_experiment_id = args.subexperiment

catalog = intake.open_esm_datastore("./test-snapsi-catalog.json")
subset = catalog.search(
    variable_id=["ua", "va", "ta", "zg", "wap"],
    institution_id=institution_id, 
    sub_experiment_id=sub_experiment_id,
    experiment_id=experiment_id
)

output_dir = pathlib.Path(f"/gws/nopw/j04/snapsi/zmd/{institution_id}/{sub_experiment_id}/{experiment_id}/")
output_dir.mkdir(parents=True, exist_ok=True)

ds = subset.to_dask()
print(ds)
ds = ds.rename({"ua":"u", "va":"v", "wap":"w", "ta":"T", "zg": "Z"})

for member in ds.member_id.values:
    print(f"Now working on {member} for {institution_id} {experiment_id} {sub_experiment_id}")
    output_file = f"{institution_id}_{sub_experiment_id}_{experiment_id}_{member}_zmd.nc"
    zmd = create_zonal_mean_dataset(ds.sel(member_id=member), verbose=True, include_waves=True, waves=[1,2,3], fftpkg="xrft")
    
    comp = dict(dtype="float32")
    encoding = {var: comp for var in zmd.data_vars}
    
    print(f"Saving to {str(output_dir)}/{output_file}")
    zmd.attrs['history'] = f'Created by zdlawren using pyzome on {datetime.utcnow()}'
    zmd.to_netcdf(f"{str(output_dir)}/{output_file}", encoding=encoding)

