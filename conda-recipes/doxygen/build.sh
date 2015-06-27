#!/bin/bash
#
# This recipe:
#
# - Installs EUPS into $PREFIX/opt/eups,
# - Symlinks the EUPS setup scripts into $PREFIX/bin, as eups-setups.{sh,zsh,csh,...},
# - Points the default EUPS_PATH to $PREFIX/var/opt/eups
#
# To use it, the user should source one of the eups-setups.XX scripts.
#

mkdir -p $PREFIX

# initialize EUPS
source eups-setups.sh

# prepare
eupspkg PREFIX="$PREFIX" PRODUCT="$PKG_NAME" VERSION="$PKG_VERSION" FLAVOR=generic prep

# setup dependencies (just for the environmental variables, really)
# FIXME: a command should be added to eupspkg to just get the envvars
setup -r .

echo "SETUPED:"
eups list -s

# build
eupspkg PREFIX="$PREFIX" PRODUCT="$PKG_NAME" VERSION="$PKG_VERSION" FLAVOR=generic config
eupspkg PREFIX="$PREFIX" PRODUCT="$PKG_NAME" VERSION="$PKG_VERSION" FLAVOR=generic build
eupspkg PREFIX="$PREFIX" PRODUCT="$PKG_NAME" VERSION="$PKG_VERSION" FLAVOR=generic install

# declare to EUPS
eupspkg PREFIX="$PREFIX" PRODUCT="$PKG_NAME" VERSION="$PKG_VERSION" FLAVOR=generic decl

# TODO: explicitly append SHA1 to pkginfo
# .........
