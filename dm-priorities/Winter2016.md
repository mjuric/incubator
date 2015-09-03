# DM PS Priorities for Winter 2016

DM Project Scientist's priorities for the DM team in Winter 2016 (in
addition to the algorithmic ones in the development roadmap). These would:

 - advance our ability to verify that DM development is moving in the right direction from the science PoV.
 - enable the use of DM codes in scientific studies needed to validate LSST science cases
 - advance the scientific usability of the DM codes

## Priorities

* Unprivileged binary installs for Linux64 and Darwin64
  - not having a mechanism to exchange/install binary builds has made the stack more difficult
    to use by non-developers (incl. scientists on the project not involve with DM). DM developers
    would also benefit from not having to rebuild every time they need to try out a ticket
    (e.g., for a code review), or sync-up to master.

  - need binary distributions that a *fully unprivileged* user can use on 64-bit Linux systems with 
    glibc 2.11.3 (used on SLES 11 SP3 that’s running at NERSC) and 64-bit OS X 10.9 or
    later, without the need to install by hand many third party packages.

  - need an install as easy as:
    ```
    pip install eups      # or 'conda install eups’
    source eups-setups.sh
    eups distrib install lsst_distrib  # where this part installs the binaries
    setup lsst_distrib
    ```
    (and, in the future, potentially remove the need for explicit setups or sourcing of EUPS).

  - need to make it easy/transparent for developers to publish binaries, so they can push proposed
    bugfixes to users as binaries. E.g., as an end-user, I’d like to be able to do something like:
    ```
    eups distrib install -t master lsst_distrib
    ```
    and have the binaries from the current master installed. Then, when I discover
    a problem (and someone fixes it), I’d like to be able to get the fix by running:
    ```
    eups distrib install -t bNNNN fixed_package
    ```
    or something similar. In all cases, the binary should be installed.

  - While I'm agnostic as to whether we use `eups distrib` or something else to achieve this,
    I already have a partially working prototype of eups binary packaging at
    a level where getting it to a functional state should be no more than ~2
    wks of work.  Then everything above could be automatically enabled by
    adding an ‘eups distrib create’ afterburner to buildbot (or Jenkins) run
    (that can be either run for every build, or be enabled on request). 
    This does not depend on having all EUPS packages in the same EUPS_PATH
    (scalability should be less of a problem).  Alternatively, the same
    could be done with ‘conda’ (I have conda prototypes for that as well).

  - This does not preclude developments of longer-term solutions (RPM
    packaging, containers, etc.), but the binary distribution functionality
    is a high priority (I hope we could have it by ~October 2015).

* SExtractor-like command line drivers (a fully supported, usable, "daughter-of-processFile”)
  - We need this to make it easier to:
    - Use of the stack by non-developers (incl. me, other parts of LSST)
    - Make it easy to expand to other cameras, increasing the user base
    - Make it easy for developers to do quick, one-off, tests
  - Example mockups of what the command line may look like:

    ```
    lsst reduce raw.fits --flat flat.fits --dark dark.fits --bias bias.fits --output calexp.fits
    lsst process input.fits --outputCatalog det.fits --outputCalexp calexp.fits
    lsst diffim a.fits b.fits --outputCatalog det.fits —outputDiffim diffim.fits
    lsst coadd input*.fits --outputImage coadd.fits
    ```
* Documentation on how to run key pipelines (using both the simple command-line drivers, and the registry based ones)
  - I think IPython notebooks may be the easiest way to start.

* Documentation describing the overall architecture of the stack
  - This includes the `tasks`, `obs_*` packages, `butler`, and how it all hangs together.
  - Specific goal: the documentation should make it possible for an outside developer to know where to start to make the stack work with their camera.
  - Much of this already exists, but needs to be pulled together

* Documentation of the implementation
  - Papers describing the algorithms used, especially those that are novel

* API pythonification
  - Should to explicitly set aside time to begin working on this, with a concrete 
    goals (stories) for W’16. Go for the low-hanging fruit that's been identified at LSST 2015.

-- mjuric
