docker-clean:
	docker compose down -v --rmi all --remove-orphans

docker-pull:
	docker compose pull

docker-build:
	docker compose build

docker-kafka:
	docker compose --profile kafka up

tree:
	docker compose run --rm rst cargo tree

clean:
	docker compose run --rm rst cargo clean

build-debug:
	docker compose run --rm rst cargo build

build-release:
	docker compose run --rm rst cargo build --release

server-debug: build-debug
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/debug/jrpk server --brokers kfk:9092 --bind 0.0.0.0:1133

client-debug-consume: build-debug
	docker compose run --rm -it --remove-orphans rst ./target/debug/jrpk client --path=result.json --address=jrpk:1133 --topic=posts --partition=0 consume --from=earliest --until=latest --max-batch-byte-size=64KiB --max-wait-ms=100

client-release-consume: build-release
	docker compose run --rm -it --remove-orphans rst ./target/release/jrpk client --path=result.json --address=jrpk:1133 --topic=posts --partition=0 consume --from=earliest --until=latest --max-batch-byte-size=64KiB --max-wait-ms=100

client-debug-produce: build-debug
	docker compose run --rm -it --remove-orphans rst ./target/debug/jrpk client --path=json/people_40mb.json --address=jrpk:1133 --topic=posts --partition=0 --max-frame-size=1m produce

server-release: build-release
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/release/jrpk --brokers kfk:9092 --bind 0.0.0.0:1133 --queue-size 8

trace:
	docker compose run --rm -it --rm --use-aliases --remove-orphans trc

test:
	docker compose run --rm rst cargo test -- --nocapture

version:
	docker compose run --rm rst rustc --version

bash:
	docker compose run --rm rst bash

