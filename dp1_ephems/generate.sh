#!/bin/bash

for MJD in $(cat mjd.txt); do
	echo "Processing MJD=$MJD"

	# run sorcha & mpsky to generate the cache file
	# replace the echos with real command lines
	echo "running sorcha..."
	echo "running mpsky build..."

	# run mpsky in the background to serve
	# (replace sleep with real mpsky invocation)
	echo -n "launching mpsky serve..."
	sleep 10000 &
	MPID=$!
	echo " done (PID=$MPID)."

	# run pipetasks (replace with real command line)
	echo "running pipetasks..."
	sleep 2;

	# shut down mpsky
	echo "shutting down mpsky serve..."
	kill -s INT $MPID
	echo "done."
	echo
done
