#!/usr/bin/env python

import pandas as pd
import astropy.units as u
import numpy as np

# because astropy is slow AF
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    from numpy import radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return np.degrees(c)

def utc_to_night(mjd, obscode='X03'):
    assert obscode == 'X03'
    localtime = mjd - 4./24.  ## hack to convert UTC to ~approx local time for Chile (need to do this better...)
    night = (localtime - 0.5).astype(int)
    return night

def compress(df, cheby_order = 4, observer_cheby_order = 7):
    # make sure the input is sorted by ObjID and time.
    df = df.sort_values(["ObjID", "FieldMJD_TAI"])
    objects = df["ObjID"].unique()
    nobj = len(objects)
    nobs = len(df) // len(objects)
    assert len(df) % nobj == 0, "All objects must have been observed at the same times"

    # extract times
    t = df["FieldMJD_TAI"].values[0:nobs]
    tmin, tmax = t.min(), t.max()
    t -= tmin
    assert np.all(np.round(t) == 0), "Hmmm... the adjusted times should span [0, 1) day range"

    #
    # extract & compress the topocentric observer vector
    #
    oxyz = np.empty((nobs, 3))
    oxyz[:, 0] = (df["Obs-Sun(J2000x)(km)"].values * u.km).to(u.au).value[0:nobs]
    oxyz[:, 1] = (df["Obs-Sun(J2000y)(km)"].values * u.km).to(u.au).value[0:nobs]
    oxyz[:, 2] = (df["Obs-Sun(J2000z)(km)"].values * u.km).to(u.au).value[0:nobs]
    op = np.polynomial.chebyshev.chebfit(t, oxyz, observer_cheby_order)

    # Check that the decompressed topocentric position makes sense
    oxyz2 = np.polynomial.chebyshev.chebval(t, op)
    assert np.all(np.abs(oxyz2/oxyz.T - 1) < 5e-7)

    #
    # Fit asteroid chebys
    #
    axyz = np.empty((nobs, 3, nobj))
    axyz[:, 0, :].T.flat = (df["Ast-Sun(J2000x)(km)"].values * u.km).to(u.au).value
    axyz[:, 1, :].T.flat = (df["Ast-Sun(J2000y)(km)"].values * u.km).to(u.au).value
    axyz[:, 2, :].T.flat = (df["Ast-Sun(J2000z)(km)"].values * u.km).to(u.au).value
    p = np.polynomial.chebyshev.chebfit(t, axyz.reshape(nobs, -1), cheby_order).reshape(cheby_order+1, 3, -1)

    # Check that the decompressed asteroid positions make sense
    axyz2 = np.polynomial.chebyshev.chebval(t, p)
    ra  = df['AstRA(deg)'].values
    dec = df['AstDec(deg)'].values

    xyz = axyz2 - oxyz2[:, np.newaxis, :] # --> (xyz, objid, nobs)
    x, y, z = xyz

    r = np.sqrt(x**2 + y**2 + z**2)
    lat = np.rad2deg( np.arcsin(z/r) ).flatten()
    lon = np.rad2deg( np.arctan2(y, x) ).flatten()

    dd = haversine(lon, lat, ra, dec)*3600
    assert dd.max() < 1

    #
    # return the results
    #
    return (tmin, tmax), op, p, objects

def cart_to_sph(xyz):
    x, y, z = xyz

    r = np.sqrt(x**2 + y**2 + z**2)
    ra = np.rad2deg( np.arctan2(y, x) )
    ra[ra < 0] = ra[ra < 0] + 360
    dec = np.rad2deg( np.arcsin(z/r) )

    return ra, dec

def decompress(t_mjd, comps, return_ephem=False):
    (tmin, tmax), op, p, objects = comps

    # adjust the time, and assert we're within the range of interpolation validity
    assert np.all((tmin <= t_mjd) & (t_mjd <= tmax)), f"The interpolation is valid from {tmin} to {tmax}"
    t = t_mjd - tmin

    oxyz2 = np.polynomial.chebyshev.chebval(t, op)  # Decompress topo position
    axyz2 = np.polynomial.chebyshev.chebval(t, p)   # Decompress asteroid position
    xyz = axyz2 - oxyz2[:, np.newaxis]              # Obs-Ast vector

    if not return_ephem:
        return objects, xyz
    else:
        return objects, xyz, cart_to_sph(xyz)

def merge_comps(compslist):
    # verify tmin/tmax are the same everywhere
    for i, comps in enumerate(compslist):
        assert comps[0] == compslist[0][0], f"Interpolation limits don't match, {comps[0]} != {compslist[0][0]} at index={i}"
        assert np.all(comps[1] == compslist[0][1]), f"Observer location chebys don't match, {comps[2]} != {compslist[0][2]} at index={i}"
    (tmin, tmax), op, _, _ = comps

    p = [ comps[2] for comps in compslist]
    p = np.concatenate(p, axis=2)

    # convert to a string ndarray
    from itertools import chain
    objects = [ comps[3] for comps in compslist ]
    objects = list(chain(*objects))
    objects = np.asarray(objects)

    return (tmin, tmax), op, p, objects

def write_comps(fp, comps):
    import pickle
    pickle.dump(comps, fp, protocol=pickle.HIGHEST_PROTOCOL)

def read_comps(fp):
    import pickle
    return pickle.load(fp)

def _single_thread_compress():
    #
    # Delete this at some point...
    #
    import time
    t0 = time.time()
    print("loading...", end='', flush=True)
    if False:
        night0 = 60218
        df_all = pd.read_csv("few_day_test.csv")
    else:
        night0 = 60851
        df_all = pd.read_hdf('/astro/store/epyc3/data3/jake_dp03/for_mario/mpcorb_eph_1.hdf')
        #import glob
        #from tqdm import tqdm
        #df_all = [ pd.read_hdf(fn) for fn in tqdm(glob.glob('/astro/store/epyc3/data3/jake_dp03/for_mario/mpcorb_eph_??.hdf')) ]
        #df_all = pd.concat(df_all)

    # Extract a dataframe only for the specific night,
    # or (if night hasn't been given) verify the input only has a single night
    nights = utc_to_night(df_all["FieldMJD_TAI"].values)
    m = nights == night0
    df = df_all[m]
    nights = nights[m]
    assert np.all(nights == night0), "All inputs must come from the same night"
    duration = time.time() - t0
    print(f" done [night={night0}, nobj x nobs = {len(df):,}] [{duration:.2f}sec]")

    print("compressing...", end='', flush=True)
    t0 = time.time()
    comps = compress(df)
    duration = time.time() - t0
    print(f" done [ tmin/max={comps[0]}, nobjects={len(comps[3]):,}] [{duration:.2f}sec]")

    print("serializing...", end='', flush=True)
    t0 = time.time()
    outfn = f'cache.mjd={night0}.pkl'
    with open(outfn, "wb") as fp:
        write_comps(fp, comps)
    duration = time.time() - t0
    import os
    print(f" done [ size={os.stat(outfn).st_size:,}] [{duration:.2f}sec]")

    # extract visit times for this night
    print("testing...", end='')
    t0 = time.time()
    m = utc_to_night(df_all["FieldMJD_TAI"].values) == night0
    df2 = df_all[m].sort_values(["ObjID", "FieldMJD_TAI"])
    t = df2["FieldMJD_TAI"].values[ df2["ObjID"] == df2["ObjID"].iloc[0] ]
    ra  = df2['AstRA(deg)'].values
    dec = df2['AstDec(deg)'].values
    objects, _, (ra2, dec2) = decompress(t, comps, return_ephem=True)
    ra2, dec2 = ra2.flatten(), dec2.flatten()
    dd = haversine(ra2, dec2, ra, dec)*3600
    assert dd.max() < 1
    duration = time.time() - t0
    print(f" done [max on-sky error={dd.max()*1000:.2f}mas] [{duration:.2f}sec]")


def _aux_compress(fn, night0, verify=True, tolerance_arcsec=1):
    df_all = pd.read_hdf(fn)

    # Extract a dataframe only for the specific night,
    # or (if night hasn't been given) verify the input only has a single night
    nights = utc_to_night(df_all["FieldMJD_TAI"].values)
    m = nights == night0
    df = df_all[m]
    nights = nights[m]
    assert np.all(nights == night0), "All inputs must come from the same night"

    comps = compress(df)

    if verify:
        # extract visit times for this night
        m = utc_to_night(df_all["FieldMJD_TAI"].values) == night0
        df2 = df_all[m].sort_values(["ObjID", "FieldMJD_TAI"])
        t = df2["FieldMJD_TAI"].values[ df2["ObjID"] == df2["ObjID"].iloc[0] ]
        ra  = df2['AstRA(deg)'].values
        dec = df2['AstDec(deg)'].values
        objects, _, (ra2, dec2) = decompress(t, comps, return_ephem=True)
        ra2, dec2 = ra2.flatten(), dec2.flatten()
        dd = haversine(ra2, dec2, ra, dec)*3600
        assert dd.max() < tolerance_arcsec

    return comps

def fit_many(fns, night0, ncores):
    from tqdm import tqdm
    from functools import partial
    from multiprocessing import Pool
    with Pool(processes=ncores) as pool:
        allcomps = [ comp for comp in tqdm(pool.imap(partial(_aux_compress, night0=night0), fns), total=len(fns)) ]

    return merge_comps(allcomps)

def cmd_compress(args):
    import time

    night0 = 60851
    outfn = args.output # f'cache.mjd={night0}.pkl'
    fns = args.ephem_file # '/astro/store/epyc3/data3/jake_dp03/for_mario/mpcorb_eph_*.hdf')
    ncores = args.j

    comps = fit_many(fns, night0, ncores=ncores)

    with open(outfn, "wb") as fp:
        write_comps(fp, comps)
    import os
    print(f"wrote {outfn} [ size={os.stat(outfn).st_size:,}]")

    # decompress for a single time
    print("single decompress (no ephem)...", end='')
    t0 = time.time()
    decompress(night0+1.1, comps, return_ephem=False)
    duration = time.time() - t0
    print(f" done [{duration:.2f}sec]")

    print("Success!")

def cmd_query(args):
    with open(args.cache, "rb") as fp:
        comps = read_comps(fp)

    import json

    # performance
    import time
    t0 = time.perf_counter()

    # decompress for a single time
    objects, xyz = decompress(args.t, comps, return_ephem=False)

    # turn to a unit vector
    r = np.sqrt((xyz*xyz).sum(axis=0))
    xyz /= r

    # query the position via dot-product
    ra_rad, dec_rad = np.radians(args.ra), np.radians(args.dec)
    pointing = np.asarray([ np.cos(dec_rad) * np.cos(ra_rad), np.cos(dec_rad) * np.sin(ra_rad), np.sin(dec_rad) ])
    cos_radius = np.cos(np.radians(args.radius))
    dotprod = (xyz.T*pointing).sum(axis=1)
    mask = dotprod > cos_radius

    # select the results
    name, (ra, dec) = objects[mask], cart_to_sph(xyz[:, mask])
    
    # Try JSON serialization
#    js = json.dumps({'name': name.tolist(), 'ra:': ra.tolist(), 'dec': dec.tolist()})

    duration = time.perf_counter() - t0

    # print the results
    dist = haversine(ra, dec, args.ra, args.dec)
    print("#   object            ra           dec          dist")
    for n, r, d, dd in zip(name, ra, dec, dist):
        print(f"{n:10s} {r:13.8f} {d:13.8f} {dd:13.8f}")
    assert np.all(dist <= args.radius)
    print(f"# objects: {len(name)}")
    print(f"# compute time: {duration:.2f}sec")
#    print(js)

#    print(f"{name.shape=} {ra.shape=} {dec.shape=}")
#    print(f"{xyz.shape=} {dotprod.shape=} {mask.sum()=} {cos_radius=}")

def main():
    import argparse

    # Create the top-level parser
    parser = argparse.ArgumentParser(description='Asteroid Checker.')
    subparsers = parser.add_subparsers(dest='command', required=True, help='Subcommands')

    # Create the parser for the "compress" command
    parser_compress = subparsers.add_parser('compress', help='Compress ephemerides files.')
    parser_compress.add_argument('ephem_file', type=str, nargs='+', help='T')
    parser_compress.add_argument('-j', type=int, default=1, help='Run multithreaded')
    parser_compress.add_argument('--output', type=str, required=True, help='Output file name.')

    # Create the parser for the "query" command
    parser_query = subparsers.add_parser('query', help='Query data')
    parser_query.add_argument('t', type=float, help='Time (MJD, UTC)')
    parser_query.add_argument('ra', type=float, help='Right ascension (degrees)')
    parser_query.add_argument('dec', type=float, help='Declination (degrees)')
    parser_query.add_argument('--radius', type=float, default=1, help='Search radius (degrees)')
    parser_query.add_argument('--cache', type=str, required=True, help='Cache file')

    # Parse the arguments
    args = parser.parse_args()

    # Check which command is being requested and call the appropriate function/handler
    if args.command == 'compress':
        cmd_compress(args)
    elif args.command == 'query':
        cmd_query(args)

if __name__ == '__main__':
    main()
