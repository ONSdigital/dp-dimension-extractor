#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dimension-extractor
  make test
popd
