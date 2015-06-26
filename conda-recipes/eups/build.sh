#!/bin/bash

#export PREFIX="$PREFIX/bbb"
mkdir -p $PREFIX

#
# configure & build
#
./configure --prefix="$PREFIX/opt/eups" --with-eups="$PREFIX/var/opt/eups" --with-python="$PYTHON"
make

#
# install
#
make install
chmod -R u+w "$PREFIX/opt/eups"

# Link binaries into bin
#for script in "$PREFIX/opt/eups/bin"/{eups,eupspkg,pkgautoversion,eups_setup}; do
#	ln -s "$script" "$PREFIX/bin/"
#done

# Link setup scripts into bin
for script in "$PREFIX/opt/eups/bin"/setups.*; do
	name=$(basename $script)
	ln -s "$script" "$PREFIX/bin/eups-$name"
done

# Link Python modules into python library dir
mkdir -p "$SP_DIR"
ln -s "$PREFIX/python/eups" "$SP_DIR/"

# Hardcode EUPS_DIR into scripts
insert_eups_dir()
{
	# Insert an explicit EUPS_DIR export

	head -n 1 "$1" > "$1.tmp"
	echo "export EUPS_DIR='$PREFIX/opt/eups'" >> "$1.tmp"
	tail -n +2 "$1" >> "$1.tmp"
	touch -r "$1" "$1.tmp"
	test -x "$1" && chmod +x "$1.tmp"
	mv "$1.tmp" "$1"
	
	echo "Injected EUPS_DIR into $1"
}
#for script in eups eups_setup eupspkg pkgautoversion; do
#	insert_eups_dir "$PREFIX/opt/eups/bin/$script"
#done

# Fix hardcoded paths
#sed -i .backup "s|#!$PYTHON|#!/usr/bin/env python|" "$PREFIX"/bin/*
#sed -i .backup "s|$PYTHON|python|" "$PREFIX"/bin/{eups,eups_setup}
#rm "$PREFIX"/bin/*.backup

export
exit -1
#ls -l $PREFIX/python
#ls -lRt $SP_DIR
