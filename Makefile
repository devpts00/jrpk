tree:
	docker compose run --rm rst cargo tree

build-debug:
	docker compose run --rm rst cargo build

build-release:
	docker compose run --rm rst cargo build --release

server-debug: build-debug
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/debug/jrpk server --brokers kfk1:9092,kfk2:9092,kfk3:9092 --bind 0.0.0.0:1133

client-debug: build-debug
	docker compose run --rm -it --remove-orphans --use-aliases rst ./target/debug/jrpk client --file /jrpk/data.json --target jrpk:1133

server-release: build-release
	docker compose run --rm -it --remove-orphans --name jrpk rst ./target/release/jrpk server --brokers kfk1:9092,kfk2:9092,kfk3:9092 --bind 0.0.0.0:1133

client-release: build-release
	docker compose run --rm -it --remove-orphans --use-aliases rst ./target/release/jrpk client --file /jrpk/data.json --target jrpk:1133

trace:
	docker compose run --rm -it --rm --use-aliases trc

test:
	docker compose run --rm rst cargo test -- --nocapture
