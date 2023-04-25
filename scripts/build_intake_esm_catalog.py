import pickle
from datetime import datetime

from ecgtools import Builder
from ecgtools.parsers import parse_cmip6

# Location of SNAPSI data
root_path = "/badc/snap/data/post-cmip6/SNAPSI/"

# Set up the ecgtools builder; have to go to a depth of 9
# to get all of the available models. Theoretically, the 
# above root path could be modified to a specific model center
# directory (in which case a depth of 8 would be necessary).
# May need to do this when more SNAPSI data comes in but
# TODO: figure out how to combine catalogs (so that we do not
# have to repeat work
print(f"Initializing builder object with {root_path}")
builder = Builder(paths=[root_path], extension=".nc", depth=9, njobs=12)

# As far as I can tell, the get_assets method stores all the 
# potential files that will be iterated over to check and assign
# attributes (e.g., variable ids, experiment ids, etc)
assets_start = datetime.now()
print(f"Getting target assets, starting {assets_start}")
builder.get_assets()
assets_end = datetime.now()
print(f"Finished obtaining assets at {assets_end}; duration {assets_end-assets_start}")

# Build the catalog. This part takes a long time!
build_start = datetime.now()
print(f"Building the catalog, starting {build_start}")
builder.build(parsing_func=parse_cmip6)
build_end = datetime.now()
print(f"Finished building catalog at {build_end}; duration {build_end-build_start}")

# If we successfully build the catalog, pickle it so we
# can come back to the generated object without having to 
# redo all the work ...
print(f"Trying to pickle the finished build object")
with open("snapsi_built_catalog.pkl", "wb") as cat_pkl:
    try:
        pickle.dump(builder, cat_pkl)
    except Exception:
        print("Unable to pickle the build object!")

# Actually save the catalog in a form that intake
# will accept for reading
print(f"Trying to save the catalog to csv")
builder.save(
    name='test-snapsi-catalog.csv',
    path_column_name='path',
    variable_column_name='variable_id',
    data_format='netcdf',
    groupby_attrs=[
        'activity_id',
        'institution_id',
        'source_id',
        'experiment_id',
        'table_id',
        'grid_label',
    ],
    aggregations=[
        {'type': 'union', 'attribute_name': 'variable_id'},
        {
            'type': 'join_existing',
            'attribute_name': 'time_range',
            'options': {'dim': 'time', 'coords': 'minimal', 'compat': 'override'},
        },
        {
            'type': 'join_new',
            'attribute_name': 'member_id',
            'options': {'coords': 'minimal', 'compat': 'override'},
        },
    ],
)
