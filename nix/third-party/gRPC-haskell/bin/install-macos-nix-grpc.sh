#!/usr/bin/env bash
set -e

function purge_grpc {
  echo "Purging old grpc references in /usr/local..."
  if brew list grpc; then
      echo "Removing brew-installed grpc..."
      brew uninstall grpc
  else
    echo "No brew-installed grpc detected."
  fi
  rm -rf /usr/local/include/grpc
  rm -f /usr/local/lib/libgrpc.dylib
}

read -p "This script nukes brew-installed grpc libs and destructively updates /usr/local. Cool? [yN] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Okay, aborting."
    exit 1
fi

purge_grpc

echo "Building grpc from release.nix..."
grpc=$(nix-build release.nix -A grpc)
echo "Nix store path for grpc is ${grpc}."

echo "Creating symlinks into /usr/local/include and /usr/local/lib..."
ln -sf "${grpc}/include/grpc" /usr/local/include/grpc
ln -sf "${grpc}/lib/libgrpc.dylib" /usr/local/lib/libgrpc.dylib

echo "Creating the following symlinks:"
ls -ld /usr/local/include/grpc
ls -l /usr/local/lib/libgrpc.dylib

echo "All done."
