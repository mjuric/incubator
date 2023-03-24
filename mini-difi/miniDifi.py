#!/usr/bin/env python

import numpy as np
from numba import jit, njit

@njit
def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.

    Because SkyCoord is slow AF.

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return np.degrees(c)

# Construct a list of nights that have detectable tracklets
def hasTracklet(t, ra, dec, maxdt_minutes=90, minlen_arcsec=1.):
    """
    Given a set of observations in one night, calculate it has at least one
    detectable tracklet.
    
    Inputs: numpy arrays of t (time, days), ra (degrees), dec(degrees).
    
    Output: True or False
    """
    ## a tracklet must be longer than some minimum separation (1arcsec)
    ## and shorter than some maximum time (90 minutes). We find
    ## tracklets by taking all observations in a night and computing
    ## all of theirs pairwise distances, then selecting on that.
    if len(ra) < 2:
        return False

    sep = haversine_np(ra[None, :], dec[None, :], ra[:, None], dec[:, None]).flatten()
    longEnough = sep > minlen_arcsec/3600

    diff = (t[:, None] - t[None, :]).flatten()
    closeEnough = (diff > 0) & (diff < maxdt_minutes/(60*24))

    detectableTr = longEnough & closeEnough

    return detectableTr.any()

# Construct a list of nights that have detectable tracklets
@njit
def hasTracklet(t, ra, dec, maxdt_minutes=90, minlen_arcsec=1.):
    """
    Given a set of observations in one night, calculate it has at least one
    detectable tracklet.
    
    Inputs: numpy arrays of t (time, days), ra (degrees), dec(degrees).
    
    Output: True or False
    """
    ## a tracklet must be longer than some minimum separation (1arcsec)
    ## and shorter than some maximum time (90 minutes). We find
    ## tracklets by taking all observations in a night and computing
    ## all of theirs pairwise distances, then selecting on that.
    nobs = len(ra)
    if nobs < 2:
        return False

    maxdt = maxdt_minutes / (60*24)
    minlen = minlen_arcsec / 3600

    for i in range(nobs):
        for j in range(nobs):
            diff = t[i] - t[j]
            if diff > 0 and diff < maxdt:
                sep = haversine_np(ra[i], dec[i], ra[j], dec[j])
                if sep > minlen:
                    return True

    return False

def discoveryOpportunities(df2, window=14, nlink=3, p=0.95):
    # Find all nights where a trailing window of <window> nights
    # (including the current night) has at least <nlink> tracklets.
    #
    # algorithm: create an array of length [0 ... num_nights],
    #    representing the nights where there are tracklets.
    #    populate it with the tracklets (1 for each night where)
    #    there's a detectable tracklet. Then convolve it with a
    #    <window>-length window (we do this with .cumsum() and
    #    then subtracting the shifted array -- basic integration) 
    #    And then find nights where the # of tracklets >= nlink
    #
    n0, n1 = df2.index.min(), df2.index.max()
    nlen = n1 - n0 + 1
    arr = np.zeros(nlen, dtype=int)
    arr[df2.index.values - n0] = df2
    arr = arr.cumsum()
    arr[window:] -= arr[:-window]
    disc = (arr >= nlink).nonzero()[0] + n0

    # we're not done yet. the above gives us a list of nights when
    #    the object is discoverable, but this involves many duplicates
    #    (e.g., if there are tracklets on nights 3, 4, and 5, the object)
    #    will be discoverable on nights 5 through 17. What we really
    #    need is a list of nights with unique discovery opportunities.
    # algorithm: we essentially do the same as above, but instead of
    #    filling an array with "1", for each night with a tracklet, we
    #    fill it with a random number. The idea is that when we do the
    #    convolution, these random numbers will sum up to unique sums
    #    every time the same three (or more) tracklets make up for a
    #    discovery opportunity. We then find unique discovery
    #    opportunities by filtering on when the sums change.
    arr2 = np.zeros(nlen)
    arr2[df2.index.values - n0] = np.random.rand(len(df2))
    arr2 = arr2.cumsum()
    arr[window:] -= arr[:-window]
    arr2 = arr2[disc - n0]
    arr2[1:] -= arr2[:-1]
    disc = disc[arr2.nonzero()]
    
    # finally, at every discovery opportunity we have a probability <p>
    # to discover the object. Figure out when we'll discover it.
    discN = (np.random.rand(len(disc)) < p).nonzero()[0]
    discIdx = discN[0] if len(discN) else None

    return discIdx, disc

if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("test_obsv.csv")
    df["night"] = df["midPointTai"].astype(int)

    df2 = df.groupby("night").apply(lambda x: hasTracklet(x["midPointTai"].values, x["ra"].values, x["decl"].values))
    disc = discoveryOpportunities(df2)
    print(len(disc[1]))
