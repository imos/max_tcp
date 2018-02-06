#!/bin/bash
set -e -u

server() {
	./target/release/max_tcp --server 216.239.32.21:80 127.0.0.1:8888
}

client() {
	./target/release/max_tcp 127.0.0.1:8888 127.0.0.1:8889
}

cargo build --release
RUST_BACKTRACE=1 RUST_LOG=info ./target/release/max_tcp --server 216.239.32.21:80 127.0.0.1:8888 &
SERVER="$!"
RUST_BACKTRACE=1 RUST_LOG=info ./target/release/max_tcp 127.0.0.1:8888 127.0.0.1:8889 &
CLIENT="$!"
ACTUAL="$(curl -H 'Host: imoz.jp' http://127.0.0.1:8889/img/profile.png | openssl md5)"
EXPECTED='eec15e3d1e47bfd46c19dc7a32a8d4c0'
sleep 0.3
kill -TERM "${SERVER}"
kill -TERM "${CLIENT}"
wait
if [ "${ACTUAL}" == "${EXPECTED}" ]; then
	echo 'Passed.'
else
	echo "Actual: ${RESULT}"
	echo "Expected: eec15e3d1e47bfd46c19dc7a32a8d4c0"
	exit 1
fi
