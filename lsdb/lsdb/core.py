import hipscat as hc
import pandas as pd
import dask.dataframe as dd
from . import io
from .dataframe import LSeries, LDataFrame

################
# Skymap building functions

def _hist_partition(df, order, lon, lat):
    # compute counts maps, and wrap them in a tuple so that Dask
    # concatenates them into a series which will be passed to the
    # _partial_sum routine. Otherwise Dask will concatenate the
    # map's numpy array itself (this is an unfortunate Dask design
    # choice, IMHO).
    return (hc.counts_histogram(df[lon].values, df[lat].values, order),)

def _partial_sum_count_maps_intermediate(s):
    # s is a series of tuple-wrapped count maps that we'll
    # add together here.
    msum = s.iloc[0][0]
    for i in range(1, len(s)):
        msum += s.iloc[i][0]
    return (msum, )

def _partial_sum_count_maps(s):
    # this just removes the tuple-wrap so a result of the reduction
    # is a nice map.
    msum, = _partial_sum_count_maps_intermediate(s)
    return msum

def compute_skymap(ddf, maxorder=9, lon="ra", lat="dec", compute=True):
    m = ddf.reduction(
            lambda df: _hist_partition(df, maxorder, lon, lat),
            _partial_sum_count_maps,
            _partial_sum_count_maps_intermediate,
            split_every = 4
        )
    if compute:
        m = m.compute()
    return m

################
# DataFrame classes

class _Frame(dd.core._Frame, dd.core.OperatorMethodMixin):
    """Superclass for DataFrame and Series
    Parameters
    ----------
    dsk : dict
        The dask graph to compute this DataFrame
    name : str
        The key prefix that specifies which keys in the dask comprise this
        particular DataFrame / Series
    meta : geopandas.GeoDataFrame, geopandas.GeoSeries
        An empty geopandas object with names, dtypes, and indices matching the
        expected output.
    divisions : tuple of index values
        Values along which we partition our blocks on the index
    """

    _partition_type = LDataFrame

    def __init__(self, dsk, name, meta, divisions, hcmeta=None):
        super().__init__(dsk, name, meta, divisions)
        self.hcmeta = hcmeta

    def __dask_postpersist__(self):
        return self._rebuild, ()

    def _rebuild(self, dsk, *, rename=None):
        # this is a copy of the dask.dataframe vebrsion, only with the addition
        # to pass self.partitioning
        name = self._name
        if rename:
            name = rename.get(name, name)
        return type(self)(
            dsk, name, self._meta, self.divisions, self.hcmeta
        )

    def __getitem__(self, key):
        """
        If the result is a new LSDB object (automatically
        determined by dask based on the meta), then pass through the
        HiPSCat metadata information.
        """
        result = super().__getitem__(key)
        if isinstance(result, _Frame):
            result.hcmeta = self.hcmeta
        return result

    def map_partitions(self, func, *args, **kwargs):
        result = super().map_partitions(func, *args, **kwargs)
        if isinstance(result, _Frame):
            result.hcmeta = self.hcmeta
        return result

class LSDBDaskDataFrame(_Frame, dd.core.DataFrame):
    """Parallel GeoPandas GeoDataFrame
    Do not use this class directly. Instead use functions like
    :func:`dask_geopandas.read_parquet`,or :func:`dask_geopandas.from_geopandas`.
    """

    _partition_type = LDataFrame

    def to_hipscat(self, prefix, compute=True):
        from dask.utils import apply
        from dask.highlevelgraph import HighLevelGraph
        from dask.dataframe.core import DataFrame, Scalar

        # scatter this in advance as HCMetadata.orders can get sizable
        from dask.distributed import default_client
        hcmeta = default_client().scatter(self.hcmeta)

        # partition writer
        data_write = ( self
                    .map_partitions(
                        io._writehips,
                            prefix=prefix, 
                            hcmeta=hcmeta, 
                            meta=pd.Series(dtype=object)
                    )
                )

        # metadata writer
        final_name = "metadata-" + data_write._name
        dsk = {
            (final_name, 0): (
                apply,
                    io._write_metadata,
                    [ data_write.__dask_keys__(), prefix, hcmeta],
                    {},
            )
        }
        graph = HighLevelGraph.from_collections(final_name, dsk, dependencies=(data_write,))
        out = Scalar(graph, final_name, "")

        if compute:
            out = out.compute()
        
        return out
    
from dask.dataframe.dispatch import make_meta_dispatch

@make_meta_dispatch.register(LDataFrame)
def make_meta_dataframe(df, index=None):
    df = df.iloc[:0]
    return df

from dask.dataframe.utils import meta_nonempty
from dask.dataframe.backends import meta_nonempty_dataframe
@meta_nonempty.register(LDataFrame)
def _nonempty_geodataframe(x):
    df = meta_nonempty_dataframe(x)
    return LDataFrame(df)


from dask.dataframe.core import get_parallel_type
get_parallel_type.register(LDataFrame, lambda _: LSDBDaskDataFrame);

def _add_index_column(df, lon, lat):
    df["_ID"] = hc.compute_hcidx(df[lon].values, df[lat].values)
    return df

def _construct_from_dask(ddf, hcmeta):
    divisions = hcmeta.compute_divisions().tolist()
    ldf = (
        ddf
            .map_partitions(_add_index_column, hcmeta.lon, hcmeta.lat)   ## FIXME: this will break if input partitions overlap
            .set_index("_ID", divisions=divisions)
            .map_partitions(LDataFrame)
    )
    ldf.hcmeta = hcmeta
    return ldf

def from_dask(ddf, *, lon="ra", lat="dec", threshold=None, counts_df=None, hcmeta=None):
    if hcmeta is None:
        if counts_df is None:
            counts_df = ddf

        # compute the skymap and convert it to partitioning
        m = compute_skymap(counts_df, lon=lon, lat=lat)
        hcmeta = hc.HCMetadata.from_skymap(m, lon=lon, lat=lat, threshold=threshold)

    return _construct_from_dask(ddf, hcmeta)