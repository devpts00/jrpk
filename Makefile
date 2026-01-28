docker-clean:
	docker compose down -v --rmi all --remove-orphans

docker-pull:
	docker compose --profile env pull

docker-build:
	docker compose build rst trc

docker-env:
	docker compose --profile env up

tree:
	docker compose run --rm rst cargo tree

clean:
	docker compose run --rm rst cargo clean

build-debug:
	docker compose run --rm --remove-orphans rst cargo build

build-release:
	docker compose run --rm rst cargo build --release

server-debug: build-debug
	docker compose run --rm -it --remove-orphans -e HEAPPROFILE=/jrpk/out/heap \
		--name jrpk rst ./target/debug/jrpk \
		serve \
		--jrp-bind=0.0.0.0:1133 \
		--jrp-max-frame-size=1mib \
		--jrp-queue-len=1 \
		--http-bind=0.0.0.0:1134 \
		--kfk-brokers=kfk:9092 \
		--kfk-compression=lz4 \
		--tcp-send-buf-size=32kib \
		--tcp-recv-buf-size=32kib

heaptrack-server-debug: build-debug
	docker compose run --rm -it --remove-orphans \
		--name jrpk rst heaptrack --output ./out/heap ./target/debug/jrpk \
		serve \
		--jrp-bind=0.0.0.0:1133 \
		--jrp-max-frame-size=1mib \
		--jrp-queue-len=8 \
		--http-bind=0.0.0.0:1134 \
		--kfk-brokers=kfk:9092 \
		--tcp-send-buf-size=32kib \
		--tcp-recv-buf-size=32kib
	heaptrack --analyze ./out/heap.gz &

server-release: build-release
	docker compose run --rm -it --remove-orphans \
		--name jrpk rst ./target/release/jrpk \
		serve \
		--jrp-bind=0.0.0.0:1133 \
		--jrp-max-frame-size=1mib \
		--jrp-queue-len=1 \
		--http-bind=0.0.0.0:1134 \
		--kfk-brokers=kfk:9092 \
		--kfk-compression=lz4 \
		--tcp-send-buf-size=32kib \
		--tcp-recv-buf-size=32kib

client-debug-consume: build-debug
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-consume.sh debug result posts 1 1

client-release-consume: build-release
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-consume.sh release result posts 32 1

client-debug-produce: build-debug
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-produce.sh debug values posts 1 1

client-release-produce: build-release
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-produce.sh release values posts 32 10

http-consume:
	docker compose run --rm -it --remove-orphans rst ./scripts/http-consume.sh result posts 32 1000000 100mib

http-produce:
	docker compose run --rm -it --remove-orphans rst ./scripts/http-produce.sh result posts 32

trace:
	docker compose run --rm -it --rm --use-aliases --remove-orphans trc

test:
	docker compose run --rm rst cargo test -- --nocapture

version:
	docker compose run --rm rst rustc --version

bash:
	docker compose run --rm rst bash

pprof:
	docker compose run --rm --remove-orphans -p 127.0.0.1:8888:8080 rst /root/go/bin/pprof -http=0.0.0.0:8080 /jrpk/target/debug/jrpk /jrpk/out/heap.0001.heap
