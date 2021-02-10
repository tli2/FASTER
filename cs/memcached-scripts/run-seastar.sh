#!/bin/bash
#
# Runs a seastar based Memcache server. Takes in the number of cores to boot the
# server on as a command line argument.

# Check for the number of arguments. This script takes in one arg - the number
# of cores to boot the server on.
if [ $# -ne 1 ]; then
    echo "usage: $0 [number of cores]"
    echo "$0: error: incorrect number of arguments"
    exit 1
fi

# Seastar requires that we configure AIO for a large number of IOs. This is
# needed when running on a large number of cores (ex: 64).
o=$(echo 4194304 | sudo tee /proc/sys/fs/aio-max-nr)
echo "Set max AIO requests to $o"

# All server output will be dropped in here.
d="seastar-logs/server"
mkdir -p "$d"

# Launch the server. We're going to use the posix networking stack since
# we're planning to compare against SoFASTER's posix TCP stack. SoFASTER
# currently polls continuously, so we're going to do the same here too.
# We enable AIO NOWAIT because seastar uses AIO and we don't want it to
# block on calls to io_submit().
cores=$1
echo "Running seastar on $cores cores"
~/seastar/build/release/apps/memcached/memcached --smp $cores \
                                              --network-stack posix \
                                              --linux-aio-nowait on \
                                              --poll-mode \
                                              2>&1 | \
                                              tee "$d"/seastar-"$cores"cores.log
