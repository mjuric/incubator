# HiPSCat: A Spatial Partitioning Format for Astronomical Catalogs (DRAFT)

Authors: Mario Juric, Sam Wyatt, ...add your name here...

## Desiderata (v1.0)

* The format will enable storage of large astronomical catalogs. It must
  scale to datasets of 1PB+ or more.
* The format will support compression

* The format will support partitioning, so that subsets can be
  downloaded/worked on in parallel.
* Compliant datasets will maintain a balanced partition size where no
  individual partition is larger than a given size threshold.

* Horizontal partitioning: The format will be partitioned spatially, and in 
  such a way that it is easy to compute the ID of the partition containing a
  certain object given its coordinates, with minimal file downloads/reads.
* The format will make it easy to compute the IDs of a set of partition that
  overlap a certain region in the sky.

* The format must enable parallel equi-joins between two dataset, on columns
  that are co-partitioned with the spatial partitioning key, without
  shuffling.
* The format must enable parallel cross-matching between two identically
  spatially partitioned datasets, in a way that doesn't access attempt to
  access partitions or rows that are not necessary.

* The format must work well with S3-like object storage
* The format must work well on POSIX file systems
* The format must allow for simple downloads over plan HTTP
* The format must be useful for bulk dataset offerings/downloads

* The format should maximally leverage existing community-adopted astronomical
  standards
* The format should maximally leverage existing industry standards for big
  data analytics
* The format should make it easy to re-use existing analytics tools and
  best practices.

## Basic idea

Imagine a large tabular astronomical dataset. Each row MUST have a
coordinate entry. As a model, we'll take GAIA: about ~2Bn rows, keyed on
(ra, dec), with highly unequal density on the sky.

To make it easy to download over HTTP, store on a filesystem, and download
in bulk, we will store the data in a series of files.  As large datasets
tend to contain hundreds of columns and millions of rows, and typical uses
use only subsets of columns (but frequently all rows), the file format in
which to store the data should support efficient access to only a subset of
columns -- i.e., be "columnar".  We choose Parquet as the file format, given
the broad support in the industry and commonly used tools.  Parquet also
supports transparent compression.  Another possible format could be HDF5,
but HDF5 suffers from less widespread support in big-data tools.  FITS,
unfortunately, is not ideal as it is row-based.

A single file for a catalog such as Gaia would be O(1TB) in size; upcoming
Rubin catalogs would be in the hundreds of TB.  This would make it extremely
inconvenient to download/manupulate with simple tools.  We therefore choose
to _partition_ this dataset, split it into multiple files.  We choose to do
this partitioning spatially: place all rows whose (ra, dec) coordinate fall
within a given Healpix pixel in the same file. Hypothetically, if we
partitioned the sky at order=2 (equivalent to NSIDE=xxx), we would have 48
files mapping to 48 possible pixels at order=2 (12 * 2^order):

     order=2,npix=0.parquet
     order=2,npix=1.parquet
     order=2,npix=2.parquet
     ...
     order=2,npix=47.parquet

This partitioning makes the individual file sizes smaller and easier to
manipulate.  It allows for downloads of spatial subsets of the dataset.  It
also allows for very simple parallel analytics schemes, where data in each
file can be worked on by its own CPU/thread/node, and in parallel.

Each partition in this scheme covers the same number of deg^2 on the sky. 
For datasets with highly variable on-sky density of objects (e.g., Gaia),
this leads to an imbalance in sizes.  Files containing the Galactic plane
contain hundreds of millions of rows, while pixels around the Galactic poles
having orders of magnitude fewer. This disables simple, per-file, parallel
analytics schemes -- nearly all the time would be spent waiting for the CPU
to finish processing the file containing the Galactic center (if it could
run at all: that particular file may run into 100s of GB in size with the
data not fitting into RAM). To preserve the simplicity of per-file
parallelization, it is therefore desirable to adjust our scheme to make
individual files roughly of the same size.

We modify our scheme to make it hierarchical. Rather than partitioning at a
fixed order (NSIDE), in areas where the density is higher we increase the
partitioning order to maintain the individual file size above a preset
threshold. For example, assuming the order=2,npix=1.parquet file above
excedes the threshold, we would split it into four files corresponding to
pixels at the next order (order=3); i.e.:

     order=2,npix=0.parquet
     order=3,npix=4.parquet
     order=3,npix=5.parquet
     order=3,npix=6.parquet
     order=3,npix=7.parquet
     order=2,npix=2.parquet
     ...
     order=2,npix=47.parquet

Such hierarchical subdivision continues until all files are below threshold.

This makes individual partitions balanced (i.e., roughly of the same size),
and still spatially partitioned (i.e., making it easy to compute which set
of files contain data corresponding to a direction or region on the sky).

We next recognize and address a few final issues. Very large datasets will
typically contain 1000+ partitions (e.g., NEOWISER dataset, with 1M rows per
file, results in about ~17000 parittions). This becomes unwieldy to
manipulate in a single directory (and can cause performance issues with
certain filesystems). We therefore recast the encoding of our partitioning
as a directory structure:

     Norder=2/Npix=0/catalog.parquet
             /Npix=2/catalog.parquet
     ...
             /Npix=47/catalog.parquet
     Norder=3/Npix=4/catalog.parquet
             /Npix=5/catalog.parquet
             /Npix=6/catalog.parquet
             /Npix=7/catalog.parquet

In the above, we've also changed the key names to Norder and Npix, to make
our directory structure analogous to that of the HiPS format. Because of the
= sign, it is not fuly identical to HiPS; however, the addition of the =
makes this a fully compliant partitioned parquet dataset -- usable out of the
box by tools such as Dask, Spark, Snowflake or others.

Finally, we recognize that while modern datasets have upwards of hundreds of
columns, only a small subset is typically utilized frequently.  Storing all
these columns in the same file leads to inefficiency for the most common
queries/colum subsets.  For example downloading (ra, dec, pmra, pmdec, Rp,
Bp) columns for Gaia requires accessing ~4000 files (or objects) with
250MB/file partitioning threshold. If instead only these columns were
stored in their own files, they would be partitioned to no more than ~400
partitions. If stored on S3, this equates to 10x fewer GET requests and
dramatically less data transferred by clients that can't do Byte-Range
fetches. Faster and lower cost.

We therefore further /vertically/ partition the dataset into "column
groups".  A column group is a set of columns stored together in a parquet
file.  Each column group is partitioned independently of others, so as not
to exceed a pre-set data size.  This leads to different hierarchies for
column groups of different widths, as groups with many columns will
generally be partitioned to higher order than groups with fewer columns. 
Taking our example above and assuming it has two column groups, named "main"
(for commonly used data such as astrometry+photometry) and "details"
(containing all other columns), a partitioning may look like:
```
     Norder=2/Npix=0/main.parquet
                    /details.parquet
             /Npix=2/main.parquet
             /Npix=2/main.parquet
                    /details.parquet
     ...
             /Npix=47/main.parquet
                     /details.parquet
     Norder=3/Npix=4/details.parquet
             /Npix=5/details.parquet
             /Npix=6/details.parquet
             /Npix=7/details.parquet
```
Note how main.parquet exists for Norder=2/Npix=2, as it's small enough to
fit under the partitioning threshold at that level. The details group is
too big, however, and has been partitioned into four pixels at Norder=3
level.

Column groups are similar to "column families" from Google's BigTable (see 
https://cloud.google.com/bigtable/docs/overview).

## Metadata

The schema above is "self-describing", in the sense that the partitoning can
be inferred by traversing all directories and parsing their names. In
practice, this would be slow and inefficient.

We therefore store the schema -- the available partitions, the thresholds,
the column groups, the datatypes, etc. -- in a .json-serialized file named
metadata.json at the top of the hierarchy.

## Supporting equijoins

Hipscat datasets as described above can be understood as spatially
partitioned (sharded) tables.  If two of these tables contain a column of
data that is guaranteed to be _co-partitioned_ with the spatial key,
straightforward parallel joins are possible.

An example may help: consider a common case of a table of objects, and a
table of observations of those objects (the "sources").  One row in the
object table can map to one or more rows in the sources table (the
individual observations).  Each object will typically have an "ObjectID"
column, used as a foreign key in the sources table.  In a traditional RDBMS,
these two tables would be joine as:
```
    SELECT .. FROM objects JOIN sources on objects.id = sources.object_id
```
In a Hipscat dataset, the objects table will be partitioned on their (ra,
dec). If the rows in the sources table are partitioned on the _same_ (ra,
dec), the equijoin above can be done in parallel, on a per-partition basis.
In pseudocode:

```
    - load a partition of 'object'
      - forall partitions of 'sources' overlapping the loaded partition of
        object
        - load partition of sources
        - compute the equijoin. With pandas, something like:
          object_df.set_index("id").join(sources_df, on='object_id')
```

This can be computed in parallal, and the result is a Hipscat-partitioned,
joined, catalog. It produces correct results as long as the columns on which
the join is being made are co-partitioned. For example, if a row with
id=12345 is found in Norder=2/Npix=3 pixel of the object table, but
there were entries with object_id=12345 in (say) Norder=2/Npix=4 of the
sources table, the join wouldn't be computed correctly. All rows with object_id=12345
must be in the source table's Norder=2/Npix=3 pixel (or any higher-order
subpixels of this pixel). This guarantee is what we mean by "co-partitioned".

Columns known to be co-partitioned with the spatial key will be marked as
such in Hipscat metadata (metadata.json). Hipscat-aware tools can use this
information to allow/disallow joins on specific columns.

## Time-series

Time-series storage and selection is a simple equijoin of two tables --
sources and objects -- as described above.

## Cross-matching support

Cross-matching can be seen as generalization of equi-joins discussed above,
where the join is now made by comparing the spatial (ra, dec) coordinates
between two (or more) catalogs.

The algorithm is nearly identical as for the equijoin case, with the only change
that the "join" is now a positional cross-match. For example:
```
    - load a partition of 'sdss'
      - forall partitions of 'gaia' overlapping the loaded partition of
        'sdss'
        - load partition of 'gaia'
        - positionally cross-match the rows in two tables, and return
          a table with the result
```

This only works correctly if it can be guaranteed that all nearest neighbors
of a certain source in catalog A will be in the corresponding partition of
catalog B: this is _not_ the case for objects found near the edges of
partitions.  With the as-presented format, correctly cross-matching those
objects would require loading data from adjacent partitions. But this would
significantly increase the amount of data needed to be downloaded by each
task to compute the crossmatch. 

To avoid it, we take all rows existing in a small margin around each pixel,
and store it with the pixel itself. We call this the "neigbor cache" -- it
allows the algorithm above to compute a correct cross-match even for the
object near the edge.

This neighbor cache can be stored in a separate file that hipscat tools can
load (DETAILS TBD).

## TODO
* Support for transaction

## Possible names

* Spatial Partitoning for Astronomical Catalogs (SPAC)
* Spatial Partitioning for Astronomical Tables (SPAT)
* HealPix Partitioned for Astronomical Catalogs (HealPAC)
* HealPix Partitioned Astronomical Dataset (HealPAD)

