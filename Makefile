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
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/release/jrpk \
		server \
		--brokers=kfk:9092 \
		--bind=0.0.0.0:1133 \
		--metrics-bind=0.0.0.0:9090

client-debug-consume: build-debug
	docker compose run --rm -it --remove-orphans rst ./target/debug/jrpk \
 		client \
 		--path=./json/result.json \
 		--address=jrpk:1133 \
 		--topic=posts \
 		--partition=0 \
		--metrics-uri=http://pmg:9091/metrics/job/jrpk \
		--metrics-period=1s \
 		consume \
 		--from=earliest \
 		--until=latest \
 		--max-batch-byte-size=128KiB \
 		--max-wait-ms=100

client-release-consume: build-release
	docker compose run --rm -it --remove-orphans rst ./target/release/jrpk \
		client \
		--path=./json/result.json \
		--address=jrpk:1133 \
		--topic=posts \
		--partition=0 \
		--metrics-uri=http://pmg:9091/metrics/job/jrpk \
		--metrics-period=1s \
		consume \
		--from=earliest \
		--until=latest \
		--max-batch-byte-size=128KiB \
		--max-wait-ms=100

client-debug-produce: build-debug
	docker compose run --rm -it --remove-orphans rst ./target/debug/jrpk \
		client \
 		--path=/jrpk/json/people_40mb.json \
 		--address=jrpk:1133 \
 		--topic=posts \
 		--partition=0 \
 		--max-frame-byte-size=1m \
		--metrics-uri=http://pmg:9091/metrics/job/jrpk \
		--metrics-period=1s \
 		produce

client-release-produce: build-release
	docker compose run --rm -it --remove-orphans rst ./target/release/jrpk \
		client \
 		--path=/jrpk/json/people_40mb.json \
 		--address=jrpk:1133 \
 		--topic=posts \
 		--partition=0 \
 		--max-frame-byte-size=1m \
		--metrics-uri=http://pmg:9091/metrics/job/jrpk \
		--metrics-period=1s \
 		produce

trace:
	docker compose run --rm -it --rm --use-aliases --remove-orphans trc

test:
	docker compose run --rm rst cargo test -- --nocapture

version:
	docker compose run --rm rst rustc --version

bash:
	docker compose run --rm rst bash

