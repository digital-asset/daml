# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

error_echo () {
  echo "$@"
  exit 1
}

echo_eval () {
  echo "$*"
  eval "$*"
  return $?
}

check_daml_version_indicates_correct () {
  target_version=$1
  daml_version_output=$(daml version | grep -ve "^SDK versions:$" -e 'not installed')
  output_line_count=$(echo "$daml_version_output" | wc -l)
  if echo "$daml_version_output" | grep -qv -e "0.0.0" -e "$target_version"; then
    error_echo -e "ERROR! \`daml version\` output a version that isn't 0.0.0 or the input version $target_version.\n$daml_version_output"
  fi

  if ! echo "$daml_version_output" | grep -q "$target_version"; then
    error_echo -e "ERROR! \`daml version\` did not output the version.\n$daml_version_output"
  fi
}

check_daml_init_creates_daml_yaml_with () {
  daml init
  if ! grep -q "sdk-version: $1" daml.yaml; then
    error_echo "ERROR! \`daml init\` did not create a daml.yaml with 'sdk-version: $1'"
  fi
}

check_dar_has_correct_metadata_version () {
  unzip .daml/dist/test-daml-yaml-install-1.0.0.dar META-INF/MANIFEST.MF
  if ! grep -q "Sdk-Version: $1" META-INF/MANIFEST.MF; then
    error_echo "ERROR! \`daml build\` produced a dar whose META-INF/MANIFEST.MF contains the wrong SDK version"
    error_echo "ERROR! This likely means it was compiled with the wrong daml version"
    grep "Sdk-Version:" META-INF/MANIFEST.MF
  fi
}

daml_install_from_tarball_should_succeed () {
  tarball_path=$1
  version_cache_behaviour=$2
  if [[ "$version_cache_behaviour" == "no_cache_override_github_endpoint" ]]; then
    return 1
  elif [[ "$version_cache_behaviour" == "init_new_cache" ]]; then
    return 0
  elif [[ $tarball_path == "v2.7.1/daml-sdk-2.7.1-linux.tar.gz" ]]; then
    return 0
  else
    return 1
  fi
}

check_recommend_cache_reload () {
  output_file=$1
  if ! grep -q 'Possible fix: `daml version --force-reload yes`' "$output_file"; then
    error_echo -e "ERROR: Output of \`daml install\` does not mention 'Possible fix: \`daml version --force-reload yes\`' despite failure\n$(cat "$output_file")"
  fi
}

# If failure occurred under old cache, try updating the cache then retrying
# install
update_cache () {
  if [[ $version_cache_behaviour == init_old_cache ]]; then
    no_cache_override_github_endpoint $1
    echo_eval daml version --force-reload yes
    if daml install --install-assistant yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1; then
      echo_eval init_daml_package $tarball_release_version
      if echo_eval daml build; then
        echo_eval check_daml_version_indicates_correct $tarball_release_version
        echo_eval check_dar_has_correct_metadata_version $tarball_release_version
      else
        error_echo "ERROR! \`daml build\` on version installed from path $tarball_path failed"
      fi
    else
      error_echo "ERROR: Tried to install version from tarball '$tarball_path' with cache forcibly reloaded, but \`daml install\` failed."
    fi
  fi
}

allow_nonrelease () {
  if echo_eval daml install --install-assistant yes --allow-install-non-release yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1; then
    echo_eval init_daml_package $tarball_sdk_version
    if echo_eval daml build; then
      echo_eval check_daml_version_indicates_correct $tarball_sdk_version
      echo_eval check_dar_has_correct_metadata_version $tarball_sdk_version
    else
      error_echo "ERROR! \`daml build\` failed for version installed from path $tarball_path"
    fi
  else
    error_echo "ERROR: Tried to install version from tarball '$tarball_path' with cache forcibly reloaded, but \`daml install\` failed."
  fi
}

init_daml_package () {
echo """
sdk-version: $1
name: test-daml-yaml-install
version: 1.0.0
source: Main.daml
scenario: Main:main
parties:
- Alice
- Bob
dependencies:
- daml-prim
- daml-stdlib
""" > daml.yaml

echo """
module Main where
""" > Main.daml
}

do_post_failed_tarball_install_behaviour () {
  behaviour=$1
  shift
  case "$behaviour" in
    allow_nonrelease)
      allow_nonrelease "$@"
      ;;
    update_cache)
      update_cache "$@"
      ;;
    do_nothing)
      ;;
    *)
      error_echo "ERROR: Unrecognized caching behaviour '$1'"
      ;;
  esac
}

do_version_cache_behaviour () {
  behaviour=$1
  shift
  case "$behaviour" in
    init_new_cache)
      init_new_cache "$@"
      ;;
    init_old_cache)
      init_old_cache "$@"
      ;;
    no_cache_override_github_endpoint)
      no_cache_override_github_endpoint "$@"
      ;;
    *)
      error_echo "ERROR: Unrecognized caching behaviour '$1'"
      ;;
  esac
}

init_new_cache () {
  cp $(realpath new_cache) $DAML_CACHE/versions.txt
}

init_old_cache () {
  cp $(realpath old_cache) $DAML_CACHE/versions.txt
}

no_cache_override_github_endpoint () {
  rm $DAML_CACHE/versions.txt || true # don't fail if file doesn't exist
  mkdir -p releases-endpoint
  cp $1 "releases-endpoint/releases"
  echo "releases-endpoint: $(realpath releases-endpoint/releases)" >> $DAML_HOME/daml-config.yaml
}

# serve a mirror of github's API to avoid usage limits
absolute_github_api_file=$(realpath releases-github-api.json)
if [[ ! -e "$absolute_github_api_file" ]]; then
  error_echo "ERROR: You must supply a file to be used to resolve API requests in no_cache_override_github_endpoint"
  exit 1
fi

# Serve a mirror directory of github for more speed
mkdir -p github_mirror_directory/{v2.7.1,v2.7.4,v2.7.5,v2.8.0-snapshot.20231101.0}
cp --no-dereference $(rlocation daml-sdk-2.7.5-tarball)/file/downloaded github_mirror_directory/v2.7.5/daml-sdk-2.7.5-linux.tar.gz
cp --no-dereference $(rlocation daml-sdk-2.7.4-tarball)/file/downloaded github_mirror_directory/v2.7.4/daml-sdk-2.7.4-linux.tar.gz
cp --no-dereference $(rlocation daml-sdk-2.7.1-tarball)/file/downloaded github_mirror_directory/v2.7.1/daml-sdk-2.7.1-linux.tar.gz
cp --no-dereference $(rlocation daml-sdk-2.8.0-snapshot.20231026.12262.0.vb12eb2ad-tarball)/file/downloaded github_mirror_directory/v2.8.0-snapshot.20231101.0/daml-sdk-2.8.0-snapshot.20231026.12262.0.vb12eb2ad-linux.tar.gz
absolute_github_mirror_directory=$(realpath github_mirror_directory)
alternate_download_line="alternate-download: $absolute_github_mirror_directory"

# Create sandbox with a daml root and daml cache
export SANDBOX_ROOT="$PWD"
export DAML_CACHE="$PWD/cache"
mkdir "$DAML_CACHE"
export DAML_HOME="$PWD/daml_home"
mkdir -p "$DAML_HOME"
"$(rlocation head_sdk)"/daml install --install-assistant yes "$(rlocation head_sdk)"/sdk-release-tarball-ce.tar.gz
export PATH="$DAML_HOME/bin:$PATH"
echo "$alternate_download_line" >> $DAML_HOME/daml-config.yaml

[[ "$#" -gt 0 ]] || error_echo "No command to run supplied via args"
command_to_run=$1
shift
case "$command_to_run" in
  install_from_version)
    [[ "$#" -gt 0 ]] || error_echo "No install_version supplied via args"
    install_version=$1
    shift
    [[ "$#" -gt 0 ]] || error_echo "No version_cache_behaviour supplied via args"
    version_cache_behaviour=$1
    shift
    do_version_cache_behaviour $version_cache_behaviour $absolute_github_api_file
    if echo_eval daml install --install-assistant yes $install_version; then
      if [[ "$install_version" != "0.0.0" && "$install_version" != "latest" ]]; then
        echo_eval check_daml_version_indicates_correct $install_version
        echo_eval check_daml_init_creates_daml_yaml_with $install_version
      fi
    else
      if [[ "$1" != "0.0.0" ]]; then
        error_echo "ERROR! Exit code for \`daml install $1\` is $2"
      fi
    fi
    ;;
  build_from_version)
    [[ "$#" -gt 0 ]] || error_echo "No build_version supplied via args"
    build_version=$1
    shift
    [[ "$#" -gt 0 ]] || error_echo "No version_cache_behaviour supplied via args"
    version_cache_behaviour=$1
    shift
    do_version_cache_behaviour $version_cache_behaviour $absolute_github_api_file
    echo_eval init_daml_package $build_version
    if echo_eval daml build; then
      echo_eval check_daml_version_indicates_correct $build_version
      echo_eval check_dar_has_correct_metadata_version $build_version
    else
      error_echo "ERROR! Exit code for \`daml build\` on version $1 is $2"
    fi
    ;;
  install_and_build_from_tarball)
    [[ "$#" -gt 0 ]] || error_echo "No tarball_path supplied via args"
    tarball_path=$1
    shift
    [[ "$#" -gt 0 ]] || error_echo "No version_cache_behaviour supplied via args"
    version_cache_behaviour=$1
    shift
    [[ "$#" -gt 0 ]] || error_echo "No post_failed_tarball_install_behaviour supplied via args"
    post_failed_tarball_install_behaviour=$1
    shift

    tarball_release_version=${tarball_path%%/*}
    tarball_release_version=${tarball_release_version#v}
    tarball_sdk_version=${tarball_path%-linux.tar.gz}
    tarball_sdk_version=${tarball_sdk_version#*/daml-sdk-}

    do_version_cache_behaviour $version_cache_behaviour $absolute_github_api_file
    if echo_eval daml install --install-assistant yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1; then
      cat daml_install_output
      if ! echo_eval daml_install_from_tarball_should_succeed $tarball_path $version_cache_behaviour; then
        error_echo "ERROR: Tried to install version from tarball '$tarball_path' with cache behaviour $version_cache_behaviour, but \`daml install\` succeeded where it should have failed."
      fi
      echo_eval init_daml_package $tarball_release_version
      if echo_eval daml build; then
        echo_eval check_daml_version_indicates_correct $tarball_release_version
        echo_eval check_dar_has_correct_metadata_version $tarball_release_version
      else
        error_echo "ERROR! Exit code for \`daml build\` on version installed from path $1 is $2"
      fi
    else
      cat daml_install_output
      if echo_eval daml_install_from_tarball_should_succeed $tarball_path $version_cache_behaviour; then
        error_echo "ERROR: Tried to install version from tarball '$tarball_path' with cache behaviour $version_cache_behaviour, but \`daml install\` failed."
      fi
      echo_eval check_recommend_cache_reload daml_install_output
      echo_eval do_post_failed_tarball_install_behaviour $post_failed_tarball_install_behaviour $absolute_github_api_file
    fi
    ;;
  *)
    error_echo "ERROR: Unrecognized command $1"
    exit 1
    ;;
esac
