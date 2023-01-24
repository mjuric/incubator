#
# Nothing here must depend on:
#  - dask, pandas, ... or any other high-level packages
#
# Stick to numpy-level stuff. Astropy is allowed.
#
__version__ = "0.0.1dev"

import healpy as hp
import numpy as np

# UNIQ healpix indexing scheme (useful for multi-level maps)
# It packs both the order+ipix into the same 64bit integer (uniquely).
# There's a Gorski et al. paper describing this, somewhere.
def nest2uniq(order, ipix):
    uniq = (1 << 2*(1+order)) + ipix
    return uniq

def uniq2nest(uniq):
    order = (np.log2(uniq).astype(int) >> 1) - 1    
    ipix = uniq - (1 << 2*(1+order))
    return order, ipix

# healpix order
HCIDX_HPIX_ORDER = 20

# number of bits left over to store the rank
HCIDX_RANK_BITS  = 64 - (4 + 2*HCIDX_HPIX_ORDER)

def compute_hcidx(ra, dec):
    # the 64-bit index, viewed as a bit array, consists of two parts:
    #
    #    idx = |(pix)|(rank)|
    #
    # where pix is the healpix nest-scheme index of for given order,
    # and rank is a monotonically increasing integer for all objects
    # with the same value of pix.

    # compute the healpix pix-index of each object
    pix = hp.ang2pix(2**HCIDX_HPIX_ORDER, ra, dec, nest=True, lonlat=True)

    # shift order to high bits of idx, leaving room for rank
    idx = pix.astype(np.uint64) << HCIDX_RANK_BITS

    # sort
    orig_idx = np.arange(len(idx))
    sorted_idx = np.lexsort((dec, ra, idx))
    idx, ra, dec, orig_idx = idx[sorted_idx], ra[sorted_idx], dec[sorted_idx], orig_idx[sorted_idx]

    # compute the rank for each unique value of idx (== bitshifted pix, at this point)
    # the goal: given values of idx such as:
    #   1000, 1000, 1000, 2000, 2000, 3000, 5000, 5000, 5000, 5000, ...
    # compute a unique array such as:
    #   1000, 1001, 1002, 2000, 2001, 3000, 5000, 5001, 5002, 5003, ...
    # that is for the subset of nobj objects with the same pix, add
    # to the index an range [0..nobj)
    #
    # how this works:
    # * x are the indices of the first appearance of a new pix value. In the example above,
    # it would be equal to [0, 3, 5, 6, ...]. But note that this is also the total number
    # of entries before the next unique value (e.g. 5 above means there were 5 elements in
    # idx -- 1000, 1000, 1000, 2000, 2000 -- before the third unique value of idx -- 3000)
    # * i are the indices of each unique value of idx, starting with 0 for the first one
    # in the example above, i = [0, 0, 0, 1, 1, 2, 3, 3, 3, 3]
    # * we need construct an array such as [0, 1, 2, 0, 1, 0, 0, 1, 2, 3, ...], i.e.
    # a one that resets every time the value of idx changes. If we can construct this, we
    # can add this array to idx and achieve our objective.
    # * the way to do it: start with a monotonously increasing array
    #  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...] and subtract an array that looks like this:
    #  [0, 0, 0, 3, 3, 5, 6, 7, 7, 7, ...]. This is an array that at each location has
    #  the index where that location's pix value appeared for the first time. It's easy
    #  to confirm that this is simply x[i].
    #
    # And this is what the following four lines implement.
    _, x, i = np.unique(idx, return_inverse=True, return_index=True)
    x = x.astype(np.uint64)
    ii = np.arange(len(i), dtype=np.uint64)
    di = ii - x[i]
    idx += di

    # remap back to the old sort order
    idx[orig_idx] = idx.copy()

    return idx

# compute a partitioning, represented as a MOC, given a counts map
def compute_partitioning(m, max_counts_per_partition=1_000_000):
    npix = len(m)

    # the output
    orders = np.full(npix, -1) # healpix map of orders
    opix = {}  # dictionary of partitions used at each level

    # Top-down partitioning. Given a dataset partitioned at order k
    # bin it to higher orders (starting at 0, and working our way
    # down to k), and at each order find pixels whose count has
    # fallen below the threshold 'thresh' and record them to be
    # stored at this order.
    #
    # Outputs: opix: dict of order => pixel IDs
    #          orders: a k-order array storing the order at which this k-order pixel should be stored.
    #
    # There's a lot of fun numpy/healpix magic down below, but it all boils
    # down to two things:
    #
    # * In healpix NEST indexing scheme, when the order of the pixelization
    #   is raised by 1, each pixel is subdivided into four subpixels with
    #   pixel IDs [4*idx_o, 4*idx+1, 4*idx+2, 4*idx+3]. That means that if
    #   you need to find out in which _higher_ order pixel some pixel ID
    #   is, just integer-divide it by 4**(k-o) where k is your current order
    #   and o is the higher order. Example: pixel 49 at order 3 fall within
    #   pixel 12 at order 2, 3 at order 1, and 0 at order 0. Easy!
    # * How do you efficiently bin pixels _values_ to a higher order? To go
    #   one order up, you need to sum up groups of 4 pixels in the array
    #   (we're "reversing" the subdivision). If we go up by two orders, it's
    #   groups of 4*4... generally, it's 4**(k-o). This summing can be done
    #   very efficiently with a bit of numpy trickery: reshape the 1-D healpix
    #   array to a 2-d array where the 2nd dimension is equal to 4**(k-o),
    #   and then simply sum along that axis. The result leaves you with the
    #   array rebinned to level o.
    #
    k = hp.npix2order(npix)
    idx = np.arange(npix)
    for o in range(0, k+1):
        # the number of order-k pixels that are in one order-o pixel.
        # integer-dividing order-k pixel index (NEST scheme) with
        # this value will return the order-o index it falls within.
        k2o = 4**(k-o)

        # order o-sized bool mask listing pixels that haven't yet been
        # assigned a partition.
        active = (orders == -1).reshape(-1, k2o).any(axis=1)

        # rebin the image to order o
        imgo = m.reshape(-1, k2o).sum(axis=1)

        # find order o indices where pixel counts are below threshold.
        # These are the one which we will keep at this order.
        pixo, = np.nonzero(active & (imgo < max_counts_per_partition))

        if len(pixo):
            parts = pixo[imgo[pixo] > 0].astype(np.uint64) # store output
            if len(parts):
                opix[o] = parts

            # record the order-k indices which have been assigned to the
            # partition at this level (order o). This makes it easy to
            # check which ones are still left to process (see the 'active=...' line above)
            pixk = idx.reshape(-1, k2o)[pixo].flatten()  # this bit of magic generates all order-k 
                                                        # indices of pixels that fall into order-o
                                                        # pixels stored in pixo
            orders[pixk] = o

    assert not (orders == -1).any()
    orders = orders.astype(np.uint64)
    
    return opix, orders

# convert a MOC to a bitmap for fast lookup of orders
def moc2orders(opix):
    mapOrder = max(opix.keys())
    mm = np.full(hp.order2npix(mapOrder), -1)

    # record orders for pixels with data
    for o in opix.keys():
        k2o = 4**(mapOrder-o)
        mm.shape = (-1, k2o)
        mm[opix[o], :] = o

    # record partition orders for empty pixels
    for o in range(0, mapOrder+1):
        k2o = 4**(mapOrder-o)
        mm.shape = (-1, k2o)

        # find all pixels at this order which don't have any part of them assigned at higher orders.
        # those pixels will correspond to this order.
        mask = (mm == -1).all(1)
        mm[mask, :] = o

    mm.shape = (mm.size, )
    return np.asarray(mm, np.uint64)

def counts_histogram(lon, lat, order):
    #
    # Compute the histogram (ipix, counts) of (lon, lat) at the
    # requested order.
    #
    # (lon, lat) -- ndarray-like vectors, degrees
    # order -- healpix order
    #
    # returns:
    # (ipix) -- ndarray (the map of counts)
    #
    ipix = hp.ang2pix(2**order, lon, lat, lonlat=True, nest=True)
    ipix, counts = np.unique(ipix, return_counts=True)

    m = np.zeros(hp.order2npix(order), dtype=np.uint32)
    m[ipix] = counts
    return m

###################################
# Catalog metadata class

class HCMetadata:
    # dataframe metadata (serialized)
    opix   = None # dict of order => pixel IDs
    threshold = 1_000_000 # object threshold, per partition
    lon, lat = "ra", "dec"  # column names for positions

    # useful metadata (computed)
    orders = None # a k-order ndarray carrying the order at which this k-order pixel should be stored.

    def partitionIndex(self, lon, lat):
        # compute the UNIQ index of the partition where
        # (lat, long) is stored. (lat, lon) can be ndarrays,
        # in which case the output will be an ndarray as well.

        o = hp.npix2order(len(self.orders))
        ipix = hp.ang2pix(2**o, lon, lat, lonlat=True, nest=True)

        order = self.orders[ipix]
        ipix >>= 2*(o - order)    # equivalent to: ipix = hp.ang2pix(2**order, lon, lat, lonlat=True, nest=True)

        return nest2uniq(order, ipix)

    def hcidx2nest(self, hcidx):
        o = hp.npix2order(len(self.orders))
        ipix = hcidx >> np.asarray( 2*(HCIDX_HPIX_ORDER - o) + HCIDX_RANK_BITS, dtype=np.uint64)        # downsample to self.orders' resolution

        order = self.orders[ipix]
        ipix >>= np.asarray(2*(o - order), dtype=np.uint64)   # downsample further to the partition's order

        return order, ipix

    # TODO: Dask-specific, move out of this class
    def compute_divisions(self):
        divs = []
        for k, ipix in self.opix.items():
            # compute the smallest hcidx possible within each ipix
            hcidx = ipix << 2*(HCIDX_HPIX_ORDER - k)
            hcidx <<= HCIDX_RANK_BITS
            divs.append(hcidx)
        divs.append( [0xFFFF_FFFF_FFFF_FFFF] )  # placeholder for maximum; we'll recompute it after sorting

        divs = np.concatenate(divs)
        divs.sort()

        return divs

    def __repr__(self):
        dct = dict(opix=self.opix, threshold=self.threshold, lon=self.lon, lat=self.lat, orders=self.orders)
        return str(dct)

    def copy(self):
        import copy
        h = HCMetadata(copy.deepcopy(self.opix), self.threshold, self.lon, self.lat)
        return h

    def serialize(self, prefix):
        # write the moc to a YAML file
        import yaml, os
        meta = dict(
            coverage=self.opix,
            threshold=self.threshold,
            radec=[self.lon, self.lat]
        )
        fn = os.path.join(prefix, "_metadata.yaml")
        with open(fn, "w") as fp:
            yaml.dump(meta, fp, default_flow_style=None)

    def __init__(self, opix=None, threshold=1_000_000, lon="ra", lat="dec"):
        self.opix = opix
        self.threshold=threshold
        self.lon = lon
        self.lat = lat

        if self.opix is not None:
            self.orders = moc2orders(opix)

    @staticmethod
    def from_skymap(m, *args, **kwargs):
        opix, orders = compute_partitioning(m)
        kwargs["opix"] = opix

        hcmeta = HCMetadata(*args, **kwargs)
        return hcmeta

    @staticmethod
    def from_hipscat(prefix):
        import yaml, os

        # load metadata
        fn = os.path.join(prefix, "_metadata.yaml")
        with open(fn) as fp:
            meta = yaml.safe_load(fp)

        # construct HCMetadata
        opix = { k: np.asarray(v, dtype=np.uint64) for k, v in meta["coverage"].items() }

        ra, dec = meta['radec']
        hcmeta = HCMetadata(opix, meta['threshold'], ra, dec)

        return hcmeta
