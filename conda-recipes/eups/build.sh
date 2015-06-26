#!/bin/bash

#export PREFIX="$PREFIX/bbb"
mkdir -p $PREFIX

#./configure --prefix="$PREFIX" --with-eups="$PREFIX/stack" --with-python="$PYTHON"
./configure --prefix="$PREFIX" --with-eups="$PREFIX/opt/eups-stack" --with-python="$PYTHON"
make

#make install
mkdir -p "$PREFIX/bin"
mkdir -p "$PREFIX"/share/eups

cp -a bin/{eups,eups_impl.py,eups_setup,eups_setup_impl.py,eupspkg,pkgautoversion}  "$PREFIX"/bin
cp -a bin/setups* "$PREFIX"/share/eups

# Install architecture-independent files
cp -a README Release_Notes "$PREFIX"/share/eups
cp -a doc "$PREFIX"/share/eups
cp -a etc "$PREFIX"/share/eups; rm -f "$PREFIX"/share/eups/etc/Makefile
cp -a lib "$PREFIX"/share/eups
cp -a ups "$PREFIX"/share/eups
cp -a site "$PREFIX"/share/eups

# Install Python modules
mkdir -p "$SP_DIR"
cp -a python/eups "$SP_DIR/"

# Create default stack directory
mkdir -p "$PREFIX/opt/eups-stack/ups_db"

# Hardcode EUPS_DIR into scripts
insert_eups_dir()
{
	# Insert an explicit EUPS_DIR export

	head -n 1 "$1" > "$1.tmp"
	echo "export EUPS_DIR='$PREFIX'" >> "$1.tmp"
	tail -n +2 "$1" >> "$1.tmp"
	mv "$1.tmp" "$1"
	
	echo "Injected EUPS_DIR into $1"
}
for script in eups eups_setup eupspkg pkgautoversion; do
	insert_eups_dir "$PREFIX/bin/$script"
done

# Fix hardcoded paths
#sed -i .backup "s|#!$PYTHON|#!/usr/bin/env python|" "$PREFIX"/bin/*
#sed -i .backup "s|$PYTHON|python|" "$PREFIX"/bin/{eups,eups_setup}
#rm "$PREFIX"/bin/*.backup

export
#exit -1
#ls -l $PREFIX/python
#ls -lRt $SP_DIR
