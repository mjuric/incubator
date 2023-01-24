import hipscat as hc
import numpy as np
import os, os.path
import dask.dataframe as dd
from .dataframe import LDataFrame

def _writehips(df, prefix, hcmeta):
    if len(df) == 0:
        return ""

    # get the partition index
    if True: # this whole bit is for debugging
        order, ipix = hcmeta.hcidx2nest(df.index.values)
        uu = np.unique(ipix)
        assert(len(uu) == 1), uu
        uu = np.unique(order)
        assert(len(uu) == 1), uu
    order, ipix = hcmeta.hcidx2nest(df.index.values[0])

    # write the output file
    outfile = prefix + f'/Norder{order}/Npix{ipix}/catalog.parquet'
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    df.to_parquet(outfile, engine='pyarrow', compression='snappy', index=True)

    # return metadata about this partition
    return (outfile, (df.index.min(), df.index.max(), len(df)), (order, ipix))

## TODO: this should be moved to the hipscat package (in a filesystem independent way)
def _write_metadata(parts, prefix, hcmeta):
    from collections import defaultdict

    # construct an updated coverage map
    opix = defaultdict(list)
    for (outfile, (imin, imax, len_), (order, ipix)) in parts:
        opix[int(order)].append(int(ipix))
    opix = dict(opix)

    # FIXME: this feels hacky... there has to be a more elegant way...
    hcmeta = hcmeta.copy()
    hcmeta.opix = opix
    hcmeta.order = hc.moc2orders(opix)

    hcmeta.serialize(prefix)
    return hcmeta

##################################################################
# hipscat reading

# Let's reuse multi-file parquet reading routines from Dask. The only
# obstacle is that Dask will sort them by name, which will result in
# out-of-order partitions (the index won't be monotonically increasing)
# As a quick-and-dirty fix, we'll monkey-patch the sort function to
# recognize hipscat structure.
import dask.utils as du
try:
    _orig_natural_sort_key
except NameError:
    _orig_natural_sort_key = du.natural_sort_key

def hips_or_natural_sort_key(s: str) -> list[str | int]:
    import re, inspect
##    print(inspect.stack()[3].function,"::",inspect.stack()[2].function,"::",inspect.stack()[1].function)
    m = re.match(r"^(.*)/Norder(\d+)/Npix(\d+)/([^/]*)$", s)
    if m is None:
##        print("----", s)
        return _orig_natural_sort_key(s)
    
    root, order, ipix, leaf = m.groups()
    order, ipix = int(order), int(ipix)
    hcidx = ipix << 2*(20 - order)
    k = (root, hcidx, leaf)
##    print("HERE", order, ipix, k)
    return k
hips_or_natural_sort_key.__doc__ = _orig_natural_sort_key.__doc__

def read_hipscat(prefix, *argv, **kwargs):
    try:
        import dask.dataframe.io.parquet.arrow
        dd.io.parquet.core.natural_sort_key = hips_or_natural_sort_key
        dd.io.parquet.arrow.natural_sort_key = hips_or_natural_sort_key

        assert "calculate_divisions" not in kwargs and "gather_statistics" not in kwargs
    #    kwargs["calculate_divisions"] = True

        hcmeta = hc.HCMetadata.from_hipscat(prefix)
        ldf = ( dd
                .read_parquet(prefix, *argv, **kwargs)
                .map_partitions(LDataFrame)
              )
        ldf.hcmeta = hcmeta
        ldf.divisions = tuple(hcmeta.compute_divisions())

        return ldf
    finally:
        dd.io.parquet.core.natural_sort_key = _orig_natural_sort_key
        dd.io.parquet.arrow.natural_sort_key = _orig_natural_sort_key
