#!/bin/bash
#
# This script builds a development TimescaleDB image from the
# currently checked out source on the host.
#
SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PG_IMAGE_TAG=${PG_IMAGE_TAG:-9.6.3-alpine}
BUILD_CONTAINER_NAME=${BUILD_CONTAINER_NAME:-pgbuild}
BUILD_IMAGE_NAME=${BUILD_IMAGE_NAME:-$USER/pgbuild}
IMAGE_NAME=${IMAGE_NAME:-$USER/timescaledb}
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD ${BASE_DIR} | awk '{print $1; exit}' | sed -e "s|/|_|g")
TAG_NAME=${TAG_NAME:-$GIT_BRANCH}

# Clean previous containers
docker rm -f $(docker ps -a -q -f name=${BUILD_CONTAINER_NAME} 2>/dev/null) 2>/dev/null

if docker image ls -f "reference=${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}" | grep "$BUILD_IMAGE_NAME" 2>/dev/null; then
    echo "Using existing build image ${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}"
    docker run -d --name ${BUILD_CONTAINER_NAME} -v ${BASE_DIR}:/src ${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}
else
    echo "Creating new build image ${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}"
    # Run a Postgres container
    docker run -d --name ${BUILD_CONTAINER_NAME} -v ${BASE_DIR}:/src postgres:${PG_IMAGE_TAG}

    # Install build dependencies
    docker exec -u root -it ${BUILD_CONTAINER_NAME} /bin/bash -c "apk add --no-cache --virtual .build-deps gdb git coreutils dpkg-dev gcc libc-dev make util-linux-dev diffutils && mkdir -p /build"

    docker commit -a $USER -m "TimescaleDB build base image version $PG_IMAGE_TAG" ${BUILD_CONTAINER_NAME} ${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}
fi

# Build and install the extension with debug symbols and assertions
docker exec -u root -it ${BUILD_CONTAINER_NAME} /bin/bash -c "cp -a /src/{src,sql,test,Makefile,timescaledb.control,extra_extension_files.txt} /build/ &&  make -C /build clean && make -C /build install USE_ASSERT_CHECKING=1 CFLAGS=-g && echo \"shared_preload_libraries = 'timescaledb'\" >> /usr/local/share/postgresql/postgresql.conf.sample"

docker commit -a $USER -m "TimescaleDB development image" ${BUILD_CONTAINER_NAME} ${IMAGE_NAME}:${TAG_NAME}

# Clean build containers
docker rm -f ${BUILD_CONTAINER_NAME}

echo
echo "Built image ${IMAGE_NAME}:${TAG_NAME}"
echo "Run 'docker run -d --name some-timescaledb -p 5432:5432 ${IMAGE_NAME}:${TAG_NAME}' to launch"
