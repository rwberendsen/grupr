#!/bin/bash
set -eu

# extremely bare-bones, eventually, we'll probably use something like go-release.
# but for now, just some quick commands

pushd ../..
unformatted="$( gofmt -l ./ )"
if [ -n "$unformatted" ] 
then
	echo "unformatted files in ../.." >2
	exit 2
fi
popd

CGO_ENABLED=0 GOOS="linux"  GOARCH="amd64" go build -o "grupr__linux_amd64"
CGO_ENABLED=0 GOOS="linux"  GOARCH="arm64" go build -o "grupr__linux_arm64"
CGO_ENABLED=0 GOOS="darwin" GOARCH="amd64" go build -o "grupr__darwin_amd64"
CGO_ENABLED=0 GOOS="darwin" GOARCH="arm64" go build -o "grupr__darwin_arm64"
