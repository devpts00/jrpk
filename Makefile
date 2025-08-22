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

client-debug: build-debug
	docker compose run --rm -it --remove-orphans --use-aliases rst ./target/debug/jrpk client --file /jrpk/files/requests.json --target jrpk:1133

server-release: build-release
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/release/jrpk server --brokers kfk:9092 --bind 0.0.0.0:1133

client-release: build-release
	docker compose run --rm -it --remove-orphans --use-aliases rst ./target/release/jrpk client --file /jrpk/files/requests.json --target jrpk:1133

trace:
	docker compose run --rm -it --rm --use-aliases --remove-orphans trc

test:
	docker compose run --rm rst cargo test -- --nocapture

produce:
	docker compose run --rm rst cargo test test_produce -- --nocapture

consume:
	docker compose run --rm rst cargo test test_consume -- --nocapture

version:
	docker compose run --rm rst rustc --version

bash:
	docker compose run --rm rst bash

