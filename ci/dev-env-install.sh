#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

echo "TODO: ci/dev-env-install.sh only exists because release-trigger calls it; remove it once release-trigger installs bazelisk and gcloud itself and stops passing GNU-only flags to tar on macOS" >&2

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

bazelisk_version=1.29.0
gcloud_version=592.0.0
gcloud_beta_asset=google-cloud-sdk-beta-20261030071842.tar.gz
gcloud_beta_sha256=0034138f7ee8d13690dae0a1443a09ecbdae1e45497360c14dabd2c57a79c8a2

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    bazelisk_asset=bazelisk-linux-amd64
    bazelisk_sha256=5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992
    gcloud_asset=google-cloud-cli-${gcloud_version}-linux-x86_64.tar.gz
    gcloud_sha256=2d841a23a681ab94486af6841e5f1cd123cd300fb8c4be6ba5b51350fca3d50b
    ;;
  Linux-aarch64|Linux-arm64)
    bazelisk_asset=bazelisk-linux-arm64
    bazelisk_sha256=e20e8b0f4f240091b7a55bf17b9398bd4f40ee70ae0208dff95dd4c445fb4010
    gcloud_asset=google-cloud-cli-${gcloud_version}-linux-arm.tar.gz
    gcloud_sha256=ba588759c4c42122a8c14536009738c0f0030989ca75788235f9956555e44f4d
    ;;
  Darwin-x86_64)
    bazelisk_asset=bazelisk-darwin-amd64
    bazelisk_sha256=16c3d7aa15323a9fb69f56c7ec5733ed18bedb786680d0ba13bb12a3c8083007
    gcloud_asset=google-cloud-cli-${gcloud_version}-darwin-x86_64.tar.gz
    gcloud_sha256=cf80fb6e63435cd4f6e9fb36f7361b98e133d108eda16addc04ce26f366dcfd3
    ;;
  Darwin-arm64)
    bazelisk_asset=bazelisk-darwin-arm64
    bazelisk_sha256=cee851f726789227d5561004e9904a52be45c3efb56f8b38b6993d6adbaa0409
    gcloud_asset=google-cloud-cli-${gcloud_version}-darwin-arm.tar.gz
    gcloud_sha256=a0b1f5bae5d036ade6570b7c9d10bbdf8c1f4b319714a5b4a6f04a3af10548e0
    ;;
  *)
    echo "Unsupported platform: $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

bin_dir="$HOME/.local/bin"
bazelisk="$bin_dir/bazelisk"
gcloud_root="$HOME/.local/google-cloud-sdk-${gcloud_version}"
gcloud_complete_marker="$gcloud_root/.installed"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

verify_sha256() {
  local expected=$1 file=$2
  if command -v sha256sum >/dev/null 2>&1; then
    echo "${expected}  ${file}" | sha256sum -c -
  else
    echo "${expected}  ${file}" | shasum -a 256 -c -
  fi
}

download() {
  local url=$1 expected=$2 out=$3
  curl -sSfL --retry 5 "$url" -o "$out"
  verify_sha256 "$expected" "$out"
}

on_macos() {
  [ "$(uname -s)" = Darwin ]
}

mkdir -p "$bin_dir"

download "https://github.com/bazelbuild/bazelisk/releases/download/v${bazelisk_version}/${bazelisk_asset}" "$bazelisk_sha256" "$tmp/bazelisk"
install -m 0755 "$tmp/bazelisk" "$bazelisk"
ln -sf "$bazelisk" "$bin_dir/bazel"

cd "$repo_root/sdk"

jq_target=//bazel_tools:jq
bsdtar_target=@bsd_tar_toolchains//:resolved_target_toolchain
bazel_tool_targets=("$jq_target")
if on_macos; then
  bazel_tool_targets+=("$bsdtar_target")
fi

"$bazelisk" build "${bazel_tool_targets[@]}"
output_base="$("$bazelisk" info output_base)"
execution_root="$("$bazelisk" info execution_root)"

absolute_output_path() {
  case "$1" in
    external/*) echo "$output_base/$1" ;;
    *) echo "$execution_root/$1" ;;
  esac
}

jq_output="$("$bazelisk" cquery --output=files "$jq_target")"
install -m 0755 "$(absolute_output_path "$jq_output")" "$bin_dir/jq"

if on_macos; then
  bsdtar_output="$("$bazelisk" cquery --output=files "$bsdtar_target")"
  install -m 0755 "$(absolute_output_path "$bsdtar_output")" "$bin_dir/bsdtar"
  install -m 0755 "$repo_root/ci/tar-without-force-local.sh" "$bin_dir/tar"

  CLOUDSDK_PYTHON="$("$bazelisk" run --noshow_progress --ui_event_filters=-info @rules_python//python/bin:python -- -c 'import sys; print(sys.executable)')"
  if [ ! -x "$CLOUDSDK_PYTHON" ]; then
    echo "Bazel's Python interpreter not found, got: $CLOUDSDK_PYTHON" >&2
    exit 1
  fi
  export CLOUDSDK_PYTHON
  echo "##vso[task.setvariable variable=CLOUDSDK_PYTHON]$CLOUDSDK_PYTHON"
fi

if [ -f "$gcloud_complete_marker" ]; then
  echo "gcloud ${gcloud_version} already installed in $gcloud_root"
else
  rm -rf "$gcloud_root"
  mkdir -p "$gcloud_root"
  download "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${gcloud_asset}" "$gcloud_sha256" "$tmp/gcloud.tar.gz"
  tar -xzf "$tmp/gcloud.tar.gz" -C "$gcloud_root" --strip-components=1
  download "https://dl.google.com/dl/cloudsdk/channels/rapid/components/${gcloud_beta_asset}" "$gcloud_beta_sha256" "$tmp/gcloud-beta.tar.gz"
  tar -xzf "$tmp/gcloud-beta.tar.gz" -C "$gcloud_root"
  touch "$gcloud_complete_marker"
fi

CLOUDSDK_CONFIG="$tmp/gcloud-config" CLOUDSDK_CORE_DISABLE_PROMPTS=1 \
  "$gcloud_root/bin/gcloud" beta auth activate-service-account --help >/dev/null

echo "##vso[task.prependpath]$gcloud_root/bin"
echo "##vso[task.prependpath]$bin_dir"
