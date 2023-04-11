import numpy as np
import os

gws = '/gws/nopw/j04/snapsi/processed/'
archive = '/badc/snap/data/post-cmip6/SNAPSI/'

models = ['CNRM-CM61']
experiments = ['free', 'nudged', 'control', 'nudged-full', 'control-full']
start_dates = ['s20180125', 's20180208', 's20181213', 's20190108', 's20190829', 's20191001']

centers           = {'CNRM-CM61' : 'Meteo-France'}

variant_ids       = {'CNRM-CM61' : 'r{member}i1p1f1'}

default_grids     = {'CNRM-CM61' : 'gr'}
default_versions  = {'CNRM-CM61' : 'v20221123'}

tables = {
    '6hr'    : [ 'pr', 'prc', 'tauu', 'tauv', 'hfds', 'tasmin', 'tasmax', 'clt' ], \
    '6hrZ'   : [ 'utendnd', 'tntnd', 'tntmp', 'tntrl', 'tntrs', 'utendmp', 'utendogw', \
                 'utendnogw', 'vtendogw', 'vtendnogw', 'utendepfd', 'utendvtem', 'utendwtem' ], \
    '6hrPt'  : [ 'ta', 'ua', 'va', 'wap', 'zg', 'hus', 'ps', 'psl', 'tas', 'uas', 'vas', \
                 'rlut', 'tos', 'siconca', 'sithick', 'snw', 'snd', 'mrso', 'mrsos' ], \
    '6hrPtZ' : [ 'o3', 'epfy', 'epfz', 'vtem', 'wtem' ]
}

def get_variable_table(variable):
    for key, table in tables.items():
        if variable in table: return key

    raise ValueError('Unrecognized variable name %s' % variable)
    
def get_processed_base_path(model, experiment, start_date):
    keys = dict(root = gws, \
                model = model, \
                experiment = experiment, \
                start_date = start_date)
    template = '{root}/processed/{model}/{experiment}/{start_date}/'
    
    return template.format(**keys)

def get_archive_base_path(model, experiment, start_date, variable, member, grid = None, version = None):
    if grid == None: 
        grid = default_grids[model]
    if version == None: 
        version = default_versions[model]

    keys = dict(root = gws, \
                center = centers[model], \
                model = model, \
                experiment = experiment, \
                start_date = start_date, \
                variable = variable, \
                grid = grid, \
                version = version
               )
    
    keys['variant_id'] = variant_id_template[model].format(member)    
    keys['table']      = find_variable_table(variable)
    
    template = '{root}/{center}/{model}/{experiment}/{start_date}/{variant_id}/{table}/{var}/{grid}/{version}/'
    return template.format(**keys)

def open_archive_var(model, experiment, start_date, variable, member, grid = None, version = None):
    root = get_archive_base_path(model, experiment, start_date, variable, member, grid, version)

    

