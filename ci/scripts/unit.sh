#!/bin/bash -eux

export GOPATH=$(pwd)/go

pushd $GOPATH/src/github.com/ONSdigital/dp-dimension-extractor
  make test
popd
