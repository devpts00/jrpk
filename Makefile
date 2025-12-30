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
	docker compose run --rm -it --remove-orphans \
		-p 127.0.0.1:9999:9090 -p 127.0.0.1:1133:1133 -p 127.0.0.1:1134:1134 \
		--name jrpk rst ./target/debug/jrpk \
		server --brokers=kfk:9092 --jsonrpc-bind=0.0.0.0:1133 --http-bind=0.0.0.0:1134

heaptrack-server-debug: build-debug
	docker compose run --rm -it --remove-orphans \
		-p 127.0.0.1:9999:9090 -p 127.0.0.1:1133:1133 -p 127.0.0.1:1134:1134 \
		--name jrpk rst heaptrack --output ./out/heap ./target/debug/jrpk \
		server --brokers=kfk:9092 --jsonrpc-bind=0.0.0.0:1133 --http-bind=0.0.0.0:1134
	#echo 123
	#heaptrack --analyze ./out/heap.gz

server-release: build-release
	docker compose run --rm -it --remove-orphans \
		-p 127.0.0.1:9999:9090 -p 127.0.0.1:1133:1133 -p 127.0.0.1:1134:1134 \
		--name jrpk rst ./target/release/jrpk \
		server --brokers=kfk:9092 --jsonrpc-bind=0.0.0.0:1133 --http-bind=0.0.0.0:1134 --max-frame-byte-size=32kib

client-debug-consume: build-debug
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-consume.sh debug result posts 1

client-release-consume: build-release
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-consume.sh release result posts 32

client-debug-produce: build-debug
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-produce.sh debug result posts 1

client-release-produce: build-release
	docker compose run --rm -it --remove-orphans rst ./scripts/jsonrpc-produce.sh release result posts 32

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

