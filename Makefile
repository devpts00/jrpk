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
	docker compose run --rm -it --remove-orphans -p 127.0.0.1:9999:9090 --name jrpk rst ./target/debug/jrpk \
		server \
		--brokers=kfk:9092 \
		--bind=0.0.0.0:1133 \
		--metrics-bind=0.0.0.0:9090

server-release: build-release
	docker compose run --rm -it --remove-orphans -p 127.0.0.1:9999:9090 --name jrpk rst ./target/release/jrpk \
		server \
		--brokers=kfk:9092 \
		--bind=0.0.0.0:1133 \
		--metrics-bind=0.0.0.0:9090

client-debug-consume: build-debug
	./consume.sh debug result posts 1

client-release-consume: build-release
	./consume.sh release result posts 32

client-debug-produce: build-debug
	./produce.sh debug people_40mb.json posts 32

client-release-produce: build-release
	./produce.sh release people_40mb.json posts 32

http-fetch:
	time curl -v --output ./json/result.json "localhost:9999/kafka/fetch/posts/0?from=earliest&until=latest&min_size=1kib&max_size=16kib&max_wait=50ms"

http-send:
	time curl -v -X POST --data-binary "@./json/result.json" "localhost:9999/kafka/send/posts/0?max_batch_size=1mib"

trace:
	docker compose run --rm -it --rm --use-aliases --remove-orphans trc

test:
	docker compose run --rm rst cargo test -- --nocapture

version:
	docker compose run --rm rst rustc --version

bash:
	docker compose run --rm rst bash

