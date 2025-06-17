#!/usr/bin/env bash

if [ -z "$1" ]; then
    TAG=latest
else
    TAG="$1"
fi

DIR=$(dirname "$0")
IMAGE=vitorenesduarte/vcd:${TAG}
DOCKERFILE=${DIR}/../Dockerfiles/vcd

# release vcd
cd "${DIR}"/..
if [ "$2" == "debug" ]; then
  echo
  echo "BUILDING A DEBUG VERSION OF VCD!"
  echo
  DOCKER_BUILD_FOLDER="_build/test"
  make debug-rel
else
  DOCKER_BUILD_FOLDER="_build/default"
  make rel
fi
cd -

cp ${DOCKERFILE} ${DOCKER_BUILD_FOLDER}/vcd.dockerfile

# build image
docker build \
  --no-cache \
  --build-arg profile=${PROFILE} \
  -t "${IMAGE}" \
  -f ${DOCKER_BUILD_FOLDER}/vcd.dockerfile "${DOCKER_BUILD_FOLDER}"


# push image
docker push "${IMAGE}"
