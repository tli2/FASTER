#/bin/bash
#
# Runs memaslap against a seastar based memcache server. Takes in two arguments,
# the server's IP address and the number of cores it was configured to run on.

# We accept two positional arguments - the server's IP address and the number
# of threads that the server is running.
if [ $# -ne 2 ]; then
    echo "usage: $0 [server ip address] [number of threads on server]"
    echo "$0: error: incorrect number of arguments"
    exit 1
fi

saddr="$1"
cores="$2"

# We're going to dump all output logs in here.
d="~/seastar-logs/client"
mkdir -p $d

# We're first going to fill 250 million records into the server.
# echo "Filling data into $cores server threads"
# memaslap -s $saddr \
#          -T 16 \
#          -c 160 \
#          -F ~/memcached-scripts/memaslap.fill.cnf \
#          -x 250000000 \
#          2>&1 | \
#          tee $d/seastar-"$cores"cores-fill.log

# Configure number of clients based on the number of running server threads.
# Taken from https://github.com/scylladb/seastar/wiki/Memcached-Benchmark
# (Accessed 24th March 2020)
procs=12
if [ $cores -ge 6 ]; then
    procs=50
fi

# For cases where our server has more than 56 threads, using more than 38
# clients does not work. The remaining threads just stall for some reason.
if [ $cores -ge 56 ]; then
    procs=38
fi

# Run clients against the server. Requests are 100-key wide multigets. Ideally
# we'd want CAS operations but no benchmark out there seems to support them.
# We're going to benchmark for 2 minutes. I have a feeling that the distribution
# used by memaslap is random. This is OK because it benefits seastar which
# shards across cores; a zipf/skewed distribution would end up overloading a
# single core, limiting throughput.
echo "Running Memaslap for $cores server threads"
taskset -c $i memaslap -s $saddr \
                       -T 16 \
                       -F ~/memcached-scripts/memaslap.run.cnf \
                       -d 1024 \
                       -t 30s
