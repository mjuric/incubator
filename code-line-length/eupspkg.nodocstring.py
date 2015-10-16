#!/usr/bin/env python
# -*- python -*-
#
# Export a product and its dependencies as a package, or install a
# product from a package: a specialization for the "EupsPkg" mechanism
#
# Inspired by the builder.py and lssteups mechanisms by Robert Lupton, Ray
# Plante, and Paul Price.
#
# Maintainer:      Mario Juric <mjuric@lsst.org>
# Original author: Mario Juric <mjuric@lsst.org>
#

import sys, os, shutil, tarfile, tempfile, pipes, stat
import eups
import Distrib as eupsDistrib
import server as eupsServer
import eups.hooks


class Distrib(eupsDistrib.DefaultDistrib):
    """A class to implement product distribution based on packages
    ("EupsPkg packages") constructed by builder scripts implementing 
    verbs not unlike RPM's %xxxx macros.
    """

    NAME = "eupspkg"
    PRUNE = True

    def __init__(self, Eups, distServ, flavor=None, tag="current", options=None,
                 verbosity=0, log=sys.stderr):
        eupsDistrib.Distrib.__init__(self, Eups, distServ, flavor, tag, options,
                                     verbosity, log)

        self._msgs = {}

        self.nobuild = self.options.get("nobuild", False)
        self.noclean = self.options.get("noclean", False)

        # Allow the verbosity of eupspkg script to be set separately.
        if "verbose" not in self.options:
            self.options["verbose"] = str(Eups.verbose)

        # Prepare the string with all unrecognized options, to be passed to eupspkg on the command line
        # FIXME: This is not the right way to do it. -S options should be preserved in a separate dict()
        knownopts = set(['config', 'nobuild', 'noclean', 'noaction', 'exact', 'allowIncomplete', 'buildDir', 'noeups', 'installCurrent']);
        self.qopts = " ".join( "%s=%s" % (k.upper(), pipes.quote(str(v))) for k, v in self.options.iteritems() if k not in knownopts )

    # @staticmethod   # requires python 2.4
    def parseDistID(distID):
        """Return a valid package location if and only we recognize the 
        given distribution identifier

        This implementation return a location if it starts with "eupspkg:"
        """
        if distID:
            prefix = "eupspkg:"
            distID = distID.strip()
            if distID.startswith(prefix):
                return distID[len(prefix):]

        return None

    parseDistID = staticmethod(parseDistID)  # should work as of python 2.2

    def initServerTree(self, serverDir):
        """initialize the given directory to serve as a package distribution
        tree.
        @param serverDir    the directory to initialize
        """
        eupsDistrib.DefaultDistrib.initServerTree(self, serverDir)

        config = os.path.join(serverDir, eupsServer.serverConfigFilename)
        if not os.path.exists(config):
            configcontents = """\
# Configuration for a EupsPkg-based server
DISTRIB_CLASS = eups.distrib.eupspkg.Distrib
EUPSPKG_URL = %(base)s/products/%(path)s
LIST_URL = %(base)s/tags/%(tag)s.list
TAGLIST_DIR = tags
"""
            cf = open(config, 'a')
            try:
                cf.write(configcontents)
            finally:
                cf.close()

        # Create the tags storage directory
        tagsDir = os.path.join(serverDir, 'tags')
        if not os.path.exists(tagsDir):
                os.mkdir(tagsDir)


    def getTaggedReleasePath(self, tag, flavor=None):
        """get the file path relative to a server root that will be used 
        store the product list that makes up a tagged release.
        @param tag        the name of the tagged release of interest
        @param flavor         the target flavor for this release.  An 
                                  implementation may ignore this variable.  
        """
        return "tags/%s.list" % tag

    def getManifestPath(self, serverDir, product, version, flavor=None):
        """return the path where the manifest for a particular product will
        be deployed on the server.  In this implementation, all manifest 
        files are deployed into a subdirectory of serverDir called "manifests"
        with the filename form of "<product>-<version>.manifest".  Since 
        this implementation produces generic distributions, the flavor 
        parameter is ignored.

        @param serverDir      the local directory representing the root of 
                                 the package distribution tree.  In this 
                                 implementation, the returned path will 
                                 start with this directory.
        @param product        the name of the product that the manifest is 
                                for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform for the 
                                manifest.  This implementation ignores
                                this parameter.
        """
        return os.path.join(serverDir, "manifests", 
                            "%s-%s.manifest" % (product, version))

    def createPackage(self, serverDir, product, version, flavor=None, overwrite=False):
        """Write a package distribution into server directory tree and 
        return the distribution ID 
        @param serverDir      a local directory representing the root of the 
                                  package distribution tree
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation
        @param overwrite      if True, this package will overwrite any 
                                previously existing distribution files even if Eups.force is false
        """
        distid = self.getDistIdForPackage(product, version)
        distid = "eupspkg:%s-%s.eupspkg" % (product, version)

        # Make sure it's an absolute path
        serverDir = os.path.abspath(serverDir)

        (baseDir, productDir) = self.getProductInstDir(product, version, flavor)
        eupspkg = os.path.join(baseDir, productDir, "ups", "eupspkg")
        if not os.path.exists(eupspkg):
            # Use the defalt build file
            eupspkg = os.path.join(os.environ["EUPS_DIR"], 'lib', 'eupspkg.sh')

        # Construct the package in a temporary directory
        pkgdir0 = tempfile.mkdtemp(suffix='.eupspkg')
        prodSubdir = "%s-%s" % (product, version)
        pkgdir = os.path.join(pkgdir0, prodSubdir)
        os.mkdir(pkgdir)

        q = pipes.quote
        try:
            # Execute 'eupspkg <create>'
            cmd = ("cd %(pkgdir)s && " + \
                "%(eupspkg)s   PREFIX=%(prefix)s PRODUCT=%(product)s VERSION=%(version)s FLAVOR=%(flavor)s %(qopts)s" + \
                " create") % \
                    {
                      'pkgdir':   q(pkgdir),
                      'prefix':   q(os.path.join(baseDir, productDir)),
                      'product':  q(product),
                      'version':  q(version),
                      'flavor':   q(flavor),
                      'eupspkg':  q(eupspkg),
                      'qopts':    self.qopts,
                    }
            eupsServer.system(cmd)

            # Tarball the result and copy it to $serverDir/products
            productsDir = os.path.join(serverDir, "products")
            if not os.path.isdir(productsDir):
                try:
                    os.makedirs(productsDir)
                except:
                    raise RuntimeError, ("Failed to create %s" % (productsDir))

            tfn = os.path.join(productsDir, "%s-%s.eupspkg" % (product, version))
            if os.path.exists(tfn) and not (overwrite or self.Eups.force):
                if self.Eups.verbose > 1:
                    print >> self.log, "Not recreating", tfn
                return distid

            if not self.Eups.noaction:
                if self.verbose > 1:
                    print >> self.log, "Writing", tfn

                try:
                    cmd = 'cd %s && tar czf %s %s' % (q(pkgdir0), q(tfn), q(prodSubdir))
                    eupsServer.system(cmd)
                except OSError, e:
                    try:
                        os.unlink(tfn)
                    except OSError:
                        pass                        # probably didn't exist
                    raise RuntimeError ("Failed to write %s: %s" % (tfn, e))
        finally:
            shutil.rmtree(pkgdir0)

        return distid

    def getDistIdForPackage(self, product, version, flavor=None):
        """return the distribution ID that for a package distribution created
        by this Distrib class (via createPackage())
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation.  None means
                                that a non-flavor-specific ID is preferred, 
                                if supported.
        """
        return "eupspkg:%s-%s.eupspkg" % (product, version)

    def packageCreated(self, serverDir, product, version, flavor=None):
        """return True if a distribution package for a given product has 
        apparently been deployed into the given server directory.  
        @param serverDir      a local directory representing the root of the 
                                  package distribution tree
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation.  None means
                                that the status of a non-flavor-specific package
                                is of interest, if supported.
        """
        location = self.parseDistID(self.getDistIdForPackage(product, version, flavor))
        return os.path.exists(os.path.join(serverDir, "products", location))

    def installPackage(self, location, product, version, productRoot, 
                       installDir, setups=None, buildDir=None):
        """Install a package with a given server location into a given
        product directory tree.
        @param location     the location of the package on the server.  This 
                               value is a distribution ID (distID) that has
                               been stripped of its build type prefix.
        @param productRoot  the product directory tree under which the 
                               product should be installed
        @param installDir   the preferred sub-directory under the productRoot
                               to install the directory.  This value, which 
                               should be a relative path name, may be
                               ignored or over-ridden by the pacman scripts
        @param setups       a list of EUPS setup commands that should be run
                               to properly build this package.  This is usually
                               ignored by the pacman scripts.
        """

        pkg = location
        if self.Eups.verbose >= 1:
            print >> self.log, "[dl]",; self.log.flush()
        tfname = self.distServer.getFileForProduct(pkg, product, version,
                                                   self.Eups.flavor,
                                                   ftype="eupspkg", 
                                                   noaction=self.Eups.noaction)

        logfile = os.path.join(buildDir, "build.log") # we'll log the build to this file
        uimsgfile = os.path.join(buildDir, "build.msg") # messages to be shown on the console go to this file

        # Determine temporary build directory
        if not buildDir:
            buildDir = self.getOption('buildDir', 'EupsBuildDir')
        if self.verbose > 0:
            print >> self.log, "Building package: %s" % pkg
            print >> self.log, "Building in directory:", buildDir
            print >> self.log, "Writing log to: %s" % (logfile)

        if self.Eups.noaction:
            print >> self.log, "skipping [noaction]"
            return

        # Make sure the buildDir is empty (to avoid interference from failed builds)
        shutil.rmtree(buildDir)
        os.mkdir(buildDir)

        # Construct the build script
        q = pipes.quote
        try:
            buildscript = os.path.join(buildDir, "build.sh")
            fp = open(buildscript, 'w')
            try:
                fp.write("""\
#!/bin/bash
# ----
# ---- This script has been autogenerated by 'eups distrib install'.
# ----

set -xe
cd %(buildDir)s

# make sure the EUPS environment is set up
. "$EUPS_DIR/bin/setups.sh"

# sanitize the environment: unsetup any packages that were setup-ed
#
# NOTE: this has been disabled as there are legitimate reasons to have EUPS
# packages other than the explicit dependencies set up (i.e., compilers,
# different version of git, etc.)
#
# for pkg in $(eups list -s | cut -d' ' -f 1); do
#     unsetup -j "$pkg"
# done

# Unpack the eupspkg tarball
tar xzvf %(eupspkg)s

# Enter the directory unpacked from the tarball
PKGDIR="$(find . -maxdepth 1 -type d ! -name ".*" | head -n 1)"
test ! -z $PKGDIR
cd "$PKGDIR"

# If ./ups/eupspkg is not present, symlink in the default
if [[ ! -e ./ups/eupspkg ]]; then
    mkdir -p ./ups
    ln -s "$EUPS_DIR/lib/eupspkg.sh" ups/eupspkg
fi

# eups setup the dependencies
%(setups)s

# show what we're running with (for the log file)
eups list -s

# fetch package source
( ./ups/eupspkg %(qopts)s fetch ) || exit -1

# prepare for build (e.g., apply platform-specific patches)
( ./ups/eupspkg %(qopts)s prep  ) || exit -2

# setup the package being built. note we're using -k
# to ensure setup-ed dependencies aren't overridden by
# the table file. we could've used -j instead, but then
# 'eups distrib install -j ...' installs would fail as 
# these don't traverse and setup the dependencies.
setup --type=build -k -r .

# configure, build, and install
( ./ups/eupspkg %(qopts)s config  ) || exit -3
( ./ups/eupspkg %(qopts)s build   ) || exit -4
( ./ups/eupspkg %(qopts)s install ) || exit -5
"""                 % {
                        'buildDir' : q(buildDir),
                        'eupspkg' : q(tfname),
                        'setups' : "\n".join(setups),
                        'product' : q(product),
                        'version' : q(version),
                        'qopts' : self.qopts,
                      }
                )
            finally:
                fp.close()

            # Make executable (equivalent of 'chmod +x $buildscript')
            st = os.stat(buildscript)
            os.chmod(buildscript, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

            #
            # Did they ask to have group permissions honoured?
            #
            self.setGroupPerms(buildDir + "*")

            # Run the build
            cmd = "(%s) >> %s 2>&1 4>%s" % (q(buildscript), q(logfile), q(uimsgfile))
            if not self.nobuild:
                if self.Eups.verbose >= 1:
                    print >> self.log, "[build]",; self.log.flush()
                eupsServer.system(cmd, self.Eups.noaction)

                # Copy the build log into the product install directory. It's useful to keep around.
                installDirUps = os.path.join(self.Eups.path[0], self.Eups.flavor, product, version, 'ups')
                if os.path.isdir(installDirUps):
                    shutil.copy2(logfile, installDirUps)
                    if self.verbose > 0:
                        print >> self.log, "Build log file copied to %s/%s" % (installDirUps, os.path.basename(logfile))
                else:
                    print >> self.log, "Build log file not copied as %s does not exist (this shouldn't happen)." % installDirUps

                # Echo any lines from "messages" file
                # TODO: This should be piped in real time, not written out to a file and echoed.
                if os.path.getsize(uimsgfile) > 0:
                    print >> self.log, ""
                    fp = open(uimsgfile)
                    for line in fp:
                        self.log.write("             %s" % line)
                    fp.close()

        except OSError, e:
            if self.verbose >= 0 and os.path.exists(logfile):
                try: 
                    print >> self.log, "\n\n***** error: from %s:" % logfile
                    eupsServer.system("tail -20 %s 1>&2" % q(logfile))
                except:
                    pass
            raise RuntimeError("Failed to build %s: %s" % (pkg, str(e)))

        if self.verbose > 0:
            print >> self.log, "Install for %s successfully completed" % pkg
