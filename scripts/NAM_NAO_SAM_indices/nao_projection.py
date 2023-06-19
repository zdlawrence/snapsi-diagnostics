import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import os

from dask.distributed import Client
from nao_calculation import projection, lowpass


def area_selection(da,area):
    
    da['lon'] = xr.where(da['lon']>180,da['lon']-360,da['lon'])
    da = da.sortby('lon')
    da = da.sel(**area)
    
    return da


def log_pressure_interpolation(da,plev,ps=100000):
    
    tmp = da.copy()
    tmp['plev'] = -np.log(tmp.plev/ps)
    tmp = tmp.interp(plev=-np.log(plev/ps))
    tmp['plev'] = plev
    
    return tmp
    

if __name__ == '__main__':



    # configure computing environment
    #client = Client(n_workers=4,threads_per_worker=1,memory_limit='64GB',host=os.environ['HOSTNAME'])
    #print(client)
    #print(os.environ['HOSTNAME'])

    # choose area according to index
    area = dict(lat=slice(80,20),lon=slice(-90,40)) # NAO, Hurrel based definition
    #area = dict(lat=slice(90,20)) # NAM
    #area = dict(lat=slice(-20,90)) # SAM
    
    # load climatology and EOF pattern
    clim = xr.open_dataset('./reanalysis_climatology.nc')['Z']
    clim = area_selection(clim,area)
    
    # transform geopotential to geopotential height
    clim = clim / 9.81
    
    print('\n CLIMATOLOGY:')
    print(clim)
    
    eof = xr.open_dataset('./reanalysis_Z_winter_nao.nc')['eof']

    # transform geopotential to geopotential height
    eof = eof / 9.81
    
    print('\n EOF PATTERN:')
    print(eof)
    
    # compute climatological index value
    clim = projection(clim,eof)

    #############################
    # define file list for sample
    directory = '/badc/snap/data/post-cmip6/SNAPSI/UKMO/GloSea6/control/s20180125/r9i1p1f1/6hrPt/zg/gn/v20230403/'
    file = directory + zg_6hrPt_GloSea6_control_s20180125-r9i1p1f1_gn_201801250600-201803260000.nc

    sample = xr.open_dataset(file)['zg']
    sample = area_selection(sample,area)
    
    print('\n SAMPLE:')
    print(sample)
    
    # interpolate eof
    eof = eof.interp(lat=sample.lat,lon=sample.lon)
    eof = log_pressure_interpolation(eof,sample.plev)
    
    # linear projection of sample on pattern
    index = projection(sample,eof)

    print('\n SAMPLE INDEX:')
    print(index)

    index = index.compute()

    
    # remove climatological index value
    index = index.groupby('time.dayofyear') - clim
    
    # check sign convention
    sign = np.sign(eof.sel(lat=35,method='nearest').mean('lon') - eof.sel(lat=70,method='nearest').mean('lon'))
    print(sign)
    print(index)
    print(eof)
    
    index = index * sign
    eof = eof * sign
    
    print(index)
    print(eof)
    
    xr.Dataset(dict(series=index,pattern=eof)).to_netcdf('./example_index.nc')