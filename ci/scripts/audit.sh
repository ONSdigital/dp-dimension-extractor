#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-dimension-extractor
  make audit
popd