* what is conda
  - package manager (rpm, dpkg)
  - software distribution (yum, dnf, apt, brew)
  - environment manager (virtualenv)
* properties
  - cross-language (Python, R, C/C++, Rust, ...)
  - cross-platform (Linux, macOS, Windows)
  - binary distribution
  - strong dependency resolution guarantees
  - relocatable
* who
  - started as ContinuumIO Inc's Python package manager
  - generalized into multi-language support
  - now community supported (but Anaconda (nee ContinuumIO) still guides
    development)
* why conda
  - make it easy for users (== [data] scientists) to obtain /binary/ software
    (no building)
  - make it easy to /compose/ software (compare to docker)
  - make it easy to maintain multiple versions / environments
  - general philosophy is that the user comes 1st
    - emphasise ease of use
    - comes with guardrails (make it hard(er) for users to break their
      system)
* where
  - anaconda.org is the main package repository
  - anyone can open a "channel" -- example:
    - https://anaconda.org/mjuric
  - but, the common community repository is conda-forge:
    - https://anaconda.org/conda-forge
    - 23k software packages
    - allows to install practically anything with 'conda install -c conda-forge ....'
* how to package for conda
  * the package
    - just a tarball of binaries, that gets extracted into a destination
      environment's path (note: lying here a bit, but good enough).
  * the recipe
    - Write the "recipe": meta.yaml (think RPM's spec files)
      - Conda is not intrusive (i.e., you don't have to modify the source to
        package/distribute it).
    - Use a tool to write a recipe for you, say, from an existing github repo
      - https://github.com/conda/grayskull (conda install -c conda-forge grayskull)
    - anatomy of a recipe
  * building
    - Use conda-build to build the recipe
  * publishing
    - Publish to your channel, or...
    - .. publish to conda-forge
* tutorial
  * See tutorial.txt for hands-on example of how to create a basic Python
    conda package
  * Go to https://asciinema.org/a/628279 for a recording of this demo
* how to publish for conda-forge
  * read https://conda-forge.org/docs/maintainer/adding_pkgs.html
  * fork https://github.com/conda-forge/staged-recipes/
  * add your recipe to a subdirectory in staged-recipes/recipes
    - initiate a PR & have conda-forge's CI build your packages
    - ask for a review & inclusion into the main repo
  * once approved, your recipe will be moved to a "feedstock" repo of its own.
    - Example: https://github.com/conda-forge/sorcha-feedstock
  * conda-forge periodically looks at your source repo for any new
      releases/tags, tries to build a new release, and pre-generates the PR
      to publish the new version of the packae (all you usually do is hit
      'Merge')
* issues
  - conda can be /very/ slow (dependency resolution is costly)
    - use mamba (drop-in replacement for conda)
  - conda dev documentation is not great
    (https://docs.conda.io/projects/conda-build/en/stable/concepts/index.html)
  - conda-forge documentation is even less great
    - geting the initial build run can be a pain
  - however, for the user, the experience is generally good (that's what
      they're optimizing for)

