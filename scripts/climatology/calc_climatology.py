import xarray as xr
import dask
import numpy as np
import scipy.signal
import matplotlib.pyplot as plt


def smooth_climatology(climatology, window_len=30):
    """
    Smooth an xarray DataArray representing climatology data using a triangular filter.

    Parameters:
    climatology (xarray.DataArray): The climatology data - has shape (365 days, lats, lons)
    window_len (int, optional): The length of the filter window. Default is 30.

    Returns:
    xarray.DataArray: The smoothed climatology data.
    """
    # Define a 30-day triangular window
    window = scipy.signal.windows.triang(window_len)

    # Pad your time series data at the beginning and end to deal with the 
    # edge effects during convolution.
    pad = np.pad(climatology.values, ((window_len//2, window_len//2 - 1), (0, 0), (0, 0)), 'wrap')

    # Apply the convolution (filter)
    filtered = scipy.signal.convolve(pad, window[:, None, None] / window.sum(), mode='valid')

    # Create a new DataArray for the filtered data
    climatology_smoothed = xr.DataArray(
        filtered,
        coords={
            'dayofyear': climatology.dayofyear,
            'lat': climatology.lat,
            'lon': climatology.lon         
        },
        dims=['dayofyear', 'lat', 'lon'],
    )

    return climatology_smoothed


def calc_daily_climatology(da):
   """
    Calculate daily mean climatology following SNAPSI protocol (remove 30 June on leap years)

    Parameters:
    da (xarray.DataArray): with dimensions (time, lat, lon)
    Returns:
    xarray.DataArray: The daily climatology data with dimensions (365 days, lat, lon).
    """
 
    # Climatology period defined in SNAPSI protocol paper
    da = da.sel(time=slice('1980-01-01', '2019-12-31'))
    # Remover 30 June on leap years
    da_noleap = da.sel(time=~((da.time.dt.month == 6) & (da.time.dt.day == 30) & (da.time.dt.year % 4 == 0)))
    # Make a noleap calendar
    noleap_cal = xr.cftime_range(start='1980-01-01',periods=len(da_noleap.time),freq='D',calendar='noleap')
    # Make new DataArray with the noleap calendar
    da_noleap = xr.DataArray(data=da_noleap.data, coords={"time":noleap_cal,"lat":da_noleap.lat,"lon":da_noleap.lon})
    # Average by day of year
    daily_clim = da_noleap.groupby('time.dayofyear').mean('time')

    return daily_clim


if __name__ == '__main__':

    fname = '/home/users/wseviour/snapsi/gws/processed/wg2/era5/2t_1979_2020_D_regrid.nc'
    variable = 't2m'

    da = xr.open_dataset(fname)[variable]

    daily_clim = calc_daily_climatology(da)

    daily_clim_smoothed = smooth_climatology(daily_clim)

