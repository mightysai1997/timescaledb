IMAGE_NAME = timescaledb
CONTAINER_NAME = timescaledb
TEST_CONTAINER_NAME = $(CONTAINER_NAME)_testing
MAKE = make
PGPORT = 5432
TEST_PGPORT = 6543
TEST_TABLESPACE_PATH = /tmp/tspace1
TEST_INSTANCE_OPTS = # clear this setting when running against existing Docker instance

build-image: Dockerfile
	@docker build . -t $(IMAGE_NAME)

start-container:
	@IMAGE_NAME=$(IMAGE_NAME) CONTAINER_NAME=$(CONTAINER_NAME) PGPORT=$(PGPORT) ./scripts/docker-run.sh

start-test-container:
	@IMAGE_NAME=$(IMAGE_NAME) CONTAINER_NAME=$(TEST_CONTAINER_NAME) \
	PGPORT=$(TEST_PGPORT) TEST_TABLESPACE_PATH=$(TEST_TABLESPACE_PATH) ./scripts/start-test-docker.sh

test: build-image start-test-container installcheck
	@docker rm -f $(TEST_CONTAINER_NAME)

run: build-image start-container

include Makefile

.PHONY: test start-container start-test-container build-image
