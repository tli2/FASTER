# basics
apt-get update
apt-get install -y build-essential libevent-dev g++-8

# download and install libmemcached from source to enable memaslap
wget https://launchpad.net/libmemcached/1.0/1.0.18/+download/libmemcached-1.0.18.tar.gz
tar xvf libmemcached-1.0.18.tar.gz
cd libmemcached-1.0.18
./configure --enable-memaslap CXXFLAGS=-fpermissive LDFLAGS='-L/lib64 -lpthread'
make -j 16
make install

# download and install seastar
cd ~
git clone https://github.com/scylladb/seastar.git
# These people need to learn what CI is
cd seastar
git fetch
git checkout seastar-20.05-branch

./install-dependencies.sh
./configure.py --disable-dpdk --mode release --c++-dialect=c++17 --compiler=g++-8 --cook fmt --cflags='-Wno-error'
ninja -C build/release
