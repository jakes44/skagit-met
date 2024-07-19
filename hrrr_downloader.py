from herbie import Herbie, FastHerbie, wgrib2
from herbie.core import wgrib2_idx
import cfgrib

import pandas as pd
import xarray as xr
import geojson
import argparse
import os

# Parse command arguments from script run in the command line
def setupArgs() -> None:
    parser = argparse.ArgumentParser(description='Download HRRR data using Herbie, and segment to a specific geographic region')
    parser.add_argument('--model', 
                        default='hrrr',
                        type=str,
                        required=False,
                        help='Model of data to download, defaults to hrrr')
    parser.add_argument('--product', 
                        default='sfc',
                        type=str,
                        required=False,
                        help='Model product to download, defuaults to surface level features')
    parser.add_argument('--parameters', 
                        type=str,
                        required=True,
                        help='Comma seperated tring containing the variables and level of the vars that will be downloaded e.g. TMP:surface,RH:2 m above ground')
    parser.add_argument('--startDate', 
                        default='2020-01-01',
                        type=str,
                        required=True,
                        help='Start date of data to download e.g. 2020-10-01 for October 1, 2020')
    parser.add_argument('--endDate', 
                        default='2020-01-02',
                        type=str,
                        required=True,
                        help='End date of data to download e.g. 2020-10-01 for October 1, 2020')
    parser.add_argument('--geoJson', 
                        default='skagit_boundaries.json',
                        type=str,
                        required=False,
                        help='Path to/name of geo_json file that geogrpahically limits the downloaded data')
    return parser.parse_args()

def getFastHerbie(start_date: str, end_date: str, model: str, product: str ) -> FastHerbie:
    date_range = pd.date_range(
        start=start_date,
        end=end_date,
        freq="1h"
    )
    return FastHerbie(date_range, model=model, product=product)

# Parse GeoJson File into tuple containing boundaries
def parseGeoJson(geojson_path: str) -> tuple[float, float, float, float]:
    with open(geojson_path) as f:
        poly = geojson.load(f)
        bounds = list(geojson.utils.coords(poly))
        long, lat = zip(*bounds)
        return (min(long), max(long), min(lat), max(lat))

def limitGeographicRange(bounds: tuple[float, float, float, float], subsetFiles: list) -> list:
    return [wgrib2.region(f, bounds, name='skagit-basin') for f in subsetFiles]

# Use Fast herbie to subset and download parameters
def downloadParameters(parameters: list[str], fh: FastHerbie) -> list[Herbie]:
    fields = [f":{param}" for param in  parameters]
    print(fields)
    param_regex = fr"^(?:{'|'.join(fields)})"
    print(param_regex)
    return fh.download(param_regex)

def parseParameters(paramString: str) -> list[str]:
    return paramString.split(',')

def cleanUpFiles(subsetFiles:list) -> None:
    [os.unlink(f) for f in subsetFiles]

def mergeDatasets(regionSubsetGribFiles: list) -> xr.Dataset:
    datasets = []
    for f in regionSubsetGribFiles:
        unMergedDatasets = cfgrib.open_datasets(f)
        mergedDataset = xr.merge([ds.drop_vars(["surface", "heightAboveGround", "valid_time", "step"], errors="ignore") for ds in unMergedDatasets])
        mergedDataset.load()
        datasets.append(mergedDataset)
    return xr.concat(datasets, dim='time')

def write_to_zarr(dataset:xr.Dataset, path:str) -> None:
    dataset.to_zarr(path)

if __name__ == "__main__":
    # Get Arguments - model, variables, product, date range, and geo_json
    args = setupArgs()
    parameters = parseParameters(args.parameters)
    fh = getFastHerbie(args.startDate, args.endDate, args.model, args.product)
    fh_files = downloadParameters(parameters, fh)
    bounds = parseGeoJson(args.geoJson)
    geo_limited_files = limitGeographicRange(bounds, fh_files)
    mergedDs = mergeDatasets(geo_limited_files)
    write_to_zarr(mergedDs, args.startDate + '_' + args.endDate + '_dat.zarr')
    cleanUpFiles(fh_files)
    cleanUpFiles([f + '.idx' for f in geo_limited_files])
    cleanUpFiles(geo_limited_files)



    
