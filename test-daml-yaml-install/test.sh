# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# cachedVersions <- [", with up to date cache", ", with no cache", ", with out of date cache from prior version"]
# installedAlready <- [", with target version not yet installed", ", with target version already installed"]
# installationStyle <-
#     -- Check if installs produce correct daml version and correct daml.yaml on daml init
#   [ "`daml install latest`"
#   , "`daml install <old version>`"
#   , "`daml install <new version>`"
#   , "`daml install 0.0.0` (should fail)"
#     -- Check if builds install correct daml version and build artifacts contain correct version
#   , "`daml build` with old version in sdk-version: field in daml.yaml"
#   , "`daml build` with new version in sdk-version: field in daml.yaml"
#   , "`daml build` with 0.0.0 in sdk-version: field in daml.yaml"
#   ]
# pure $ concat [installationStyle, installedAlready, cachedVersions]

# TODO Other checks:
# - Install from source SDK - does it work with up to date cache, and does it
#   fail coherently when cache is out of date
# - Is the error message reasonable for trying to install an SDK version instead of a release version

echo_eval () {
  echo "$*"
  eval "$*"
  return $?
}

check_daml_install_nonzero () {
  if [[ "$1" != "0.0.0" && "$2" != "0" ]]; then
    echo "ERROR! Exit code for \`daml install $1\` is $2"
  fi
}

check_daml_version_indicates_correct () {
  daml_version_output=$(daml version)
  output_line_count=$(echo "$daml_version_output" | wc -l)
  if [[ "$output_line_count" -lt "3" || "$output_line_count" -gt 4 ]]; then
    echo 'ERROR! `daml version` did not output the correct number of lines.'
    echo "$daml_version_output"
  fi

  if ! echo "$daml_version_output" | grep -q "$1"; then
    echo 'ERROR! `daml version` did not output the version.'
    echo "$daml_version_output"
  fi
}

check_daml_init_creates_daml_yaml_with () {
  daml init
  if ! grep -q "sdk-version: $1" daml.yaml; then
    echo "ERROR! \`daml init\` did not create a daml.yaml with 'sdk-version: $1'"
  fi
}

check_daml_build_nonzero () {
  if [[ "$2" != "0" ]]; then
    echo "ERROR! Exit code for \`daml build\` on version $1 is $2"
  fi
}

check_dar_has_correct_metadata_version () {
  unzip .daml/dist/test-daml-yaml-install-1.0.0.dar META-INF/MANIFEST.MF
  if ! grep -q "Sdk-Version: $1" META-INF/MANIFEST.MF; then
    echo "ERROR! \`daml build\` produced a dar whose META-INF/MANIFEST.MF contains the wrong SDK version"
    echo "ERROR! This likely means it was compiled with the wrong daml version"
    grep "Sdk-Version:" META-INF/MANIFEST.MF
  fi
}

daml_install_from_tarball_should_succeed () {
  tarball_path=$1
  version_cache_behaviour=$2
  if [[ "$version_cache_behaviour" == init_old_cache && $tarball_path != "v2.7.1/daml-sdk-2.7.1-linux.tar.gz" ]]; then
    return 1
  else
    return 0
  fi
}

check_daml_install_from_tarball () {
  tarball_path=$1
  exit_code=$2
  version_cache_behaviour=$3
  if echo_eval daml_install_from_tarball_should_succeed $tarball_path $version_cache_behaviour; then
    if [[ "$exit_code" != "0" ]]; then
      echo "ERROR: Tried to install version from tarball '$tarball_path' with cache behaviour $version_cache_behaviour, but \`daml install\` failed and gave nonzero exit code $exit_code."
      return 1
    fi
  else
    if [[ "$exit_code" == "0" ]]; then
      echo "ERROR: Tried to install version from tarball '$tarball_path' with cache behaviour $version_cache_behaviour, but \`daml install\` succeeded where it should have failed."
      return 1
    fi
  fi
}

check_daml_install_from_tarball_after_cache_reload () {
  tarball_path=$1
  exit_code=$2
  if [[ "$exit_code" != "0" ]]; then
    echo "ERROR: Tried to install version from tarball '$tarball_path' with cache forcibly reloaded, but \`daml install\` failed and gave nonzero exit code $exit_code."
    return 1
  fi
}

check_daml_build_from_tarball_nonzero () {
  if [[ "$2" != "0" ]]; then
    echo "ERROR! Exit code for \`daml build\` on version installed from path $1 is $2"
  fi
}

check_recommend_cache_reload () {
  output_file=$1
  exit_code=$2
  if [[ $exit_code == "1" ]] && ! grep -q 'Possible fix: `daml version --force-reload yes`' "$output_file"; then
    echo "ERROR: Output of \`daml install\` does not mention 'Possible fix: \`daml version --force-reload yes\`' despite failure"
    cat "$output_file"
  fi
}

run_composable_checks () {
  # clean up any potentially leftover processes from previous invocations
  kill "$GITHUB_MIRROR_MINISERVE"
  kill "$RELEASES_ENDPOINT_MINISERVE"
  export GITHUB_MIRROR_MINISERVE=""
  export RELEASES_ENDPOINT_MINISERVE=""

  # serve a mirror directory of github for more speed
  github_api_file=$1
  if [[ -z "$github_api_file" ]]; then
    echo "ERROR: You must supply a file to be used to resolve API requests in no_cache_override_github_endpoint"
    return 1
  fi
  absolute_github_api_file=$(realpath $github_api_file)

  # serve a mirror directory of github for more speed
  github_mirror_directory=$2
  if [[ -n "$github_mirror_directory" ]]; then
    absolute_github_mirror_directory=$(realpath $github_mirror_directory)
    miniserve -p 9000 "$github_mirror_directory" &
    export GITHUB_MIRROR_MINISERVE=$!
    sleep 2
    alternate_download_line="alternate-download: http://localhost:9000"
  else
    alternate_download_line=""
  fi

  for version_cache_behaviour in no_cache_override_github_endpoint init_old_cache init_new_cache; do
    # Pick an install version
    # latest, split (new) version, unsplit (old) version, 0.0.0 (should fail)
    for install_version in 2.8.0-snapshot.20231101.0 0.0.0 latest 2.7.1; do
      setup_sandbox "$alternate_download_line"
      echo_eval $version_cache_behaviour $absolute_github_api_file
      echo_eval daml install --install-assistant yes $install_version
      echo_eval check_daml_install_nonzero $install_version $?
      if [[ "$install_version" != "0.0.0" && "$install_version" != "latest" ]]; then
        echo_eval check_daml_version_indicates_correct $install_version
        echo_eval check_daml_init_creates_daml_yaml_with $install_version
      fi
      [[ -z "$RELEASES_ENDPOINT_MINISERVE" ]] || kill $RELEASES_ENDPOINT_MINISERVE
      reset_sandbox
    done

    # Pick a build version
    # split (new) version, unsplit (old) version, 0.0.0
    for build_version in 2.8.0-snapshot.20231101.0 2.7.1 0.0.0; do
      setup_sandbox "$alternate_download_line"
      echo_eval $version_cache_behaviour $absolute_github_api_file
      echo_eval init_daml_package $build_version
      echo_eval daml build
      echo_eval check_daml_build_nonzero $build_version $?
      echo_eval check_daml_version_indicates_correct $build_version
      echo_eval check_dar_has_correct_metadata_version $build_version
      [[ -z "$RELEASES_ENDPOINT_MINISERVE" ]] || kill $RELEASES_ENDPOINT_MINISERVE
      reset_sandbox
    done

    # Build from tarball
    # snapshot (new) version, release (new) version, release (old) version
    for tarball_path in \
      v2.8.0-snapshot.20231101.0/daml-sdk-2.8.0-snapshot.20231026.12262.0.vb12eb2ad-linux.tar.gz \
      v2.7.5/daml-sdk-2.7.5-linux.tar.gz \
      v2.7.1/daml-sdk-2.7.1-linux.tar.gz ; do
      tarball_release_version=${tarball_path%%/*}
      tarball_release_version=${tarball_release_version#v}
      tarball_sdk_version=${tarball_path%-linux.tar.gz}
      tarball_sdk_version=${tarball_sdk_version#*/daml-sdk-}

      for post_failed_tarball_install_behaviour in allow_nonrelease update_cache do_nothing; do
        setup_sandbox "$alternate_download_line"
        echo_eval $version_cache_behaviour $absolute_github_api_file
        daml install --install-assistant yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1
        daml_install_exit_code=$?
        cat daml_install_output
        echo_eval check_daml_install_from_tarball $tarball_path $daml_install_exit_code $version_cache_behaviour
        echo_eval check_recommend_cache_reload daml_install_output $daml_install_exit_code
        if [[ "$daml_install_exit_code" == 0 ]]; then
          echo_eval init_daml_package $tarball_release_version
          echo_eval daml build
          echo_eval check_daml_build_from_tarball_nonzero $tarball_path $?
          echo_eval check_daml_version_indicates_correct $tarball_release_version
          echo_eval check_dar_has_correct_metadata_version $tarball_release_version
        else
          echo_eval $post_failed_tarball_install_behaviour
        fi
        [[ -z "$RELEASES_ENDPOINT_MINISERVE" ]] || kill $RELEASES_ENDPOINT_MINISERVE
        reset_sandbox
      done
    done
  done

  kill $GITHUB_MIRROR_MINISERVE
}

# If failure occurred under old cache, try updating the cache
update_cache () {
  if [[ $version_cache_behaviour == init_old_cache ]]; then
    no_cache_override_github_endpoint
    daml version --force-reload yes
    daml install --install-assistant yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1
    daml_install_exit_code=$?
    echo_eval check_daml_install_from_tarball_after_cache_reload $tarball_path $daml_install_exit_code $version_cache_behaviour
    echo_eval check_recommend_cache_reload daml_install_output $daml_install_exit_code
    if [[ "$daml_install_exit_code" == 0 ]]; then
      echo_eval init_daml_package $tarball_release_version
      echo_eval daml build
      echo_eval check_daml_build_from_tarball_nonzero $tarball_path $?
      echo_eval check_daml_version_indicates_correct $tarball_release_version
      echo_eval check_dar_has_correct_metadata_version $tarball_release_version
    fi
  fi
}

allow_nonrelease () {
  echo_eval daml install --install-assistant yes --allow-install-non-release yes $absolute_github_mirror_directory/$tarball_path >daml_install_output 2>&1
  daml_install_exit_code=$?
  cat daml_install_output
  echo_eval check_daml_install_from_tarball_after_cache_reload $tarball_path $daml_install_exit_code $version_cache_behaviour
  echo_eval check_recommend_cache_reload daml_install_output $daml_install_exit_code
  if [[ "$daml_install_exit_code" == 0 ]]; then
    echo_eval init_daml_package $tarball_sdk_version
    echo_eval daml build
    #echo_eval check_daml_build_from_tarball_nonzero $tarball_path $?
    #echo_eval check_daml_version_indicates_correct $tarball_sdk_version
    #echo_eval check_dar_has_correct_metadata_version $tarball_sdk_version
  fi
}

setup_sandbox () {
  alternate_download_line=$1

  # Create sandbox with a daml root and daml cache
  cd $(mktemp -d)
  export SANDBOX_ROOT="$PWD"
  export DAML_CACHE="$PWD/cache"
  mkdir "$DAML_CACHE"
  export DAML_HOME="$PWD/daml_home"
  mkdir -p "$DAML_HOME/sdk"
  mkdir -p "$DAML_HOME/bin"

  # Install 0.0.0 to DAML_HOME, set as assistant version and add to path
  ln -s "../sdk/0.0.0/daml/daml" "$DAML_HOME/bin/daml"
  cp -r "$HOME/.daml/sdk/0.0.0" "$DAML_HOME/sdk/"
  export LOCAL_DAML_HEAD=$DAML_HOME/sdk/0.0.0/daml/daml
  export PATH="$DAML_HOME/bin:$PATH"
  daml install 0.0.0 --install-assistant yes

  # Set alternate download location if available
  echo $alternate_download_line >> $DAML_HOME/daml-config.yaml
}

reset_sandbox () {
  # Remove sandbox
  cd - || cd "$HOME"
  chmod -R +rw "$SANDBOX_ROOT"
  rm -r "$SANDBOX_ROOT"
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

init_new_cache () {
  echo "2.8.0-snapshot.20231101.0 2.8.0-snapshot.20231026.12262.0.vb12eb2ad
2.8.0-snapshot.20231018.0 2.8.0-snapshot.20231016.12209.0.v28eadc6e
2.8.0-snapshot.20231011.0 2.8.0-snapshot.20231009.12181.0.v623a064c
2.8.0-snapshot.20231005.2 2.8.0-snapshot.20230929.12167.0.v44c0c51b
2.8.0-snapshot.20231005.1 2.8.0-snapshot.20230929.12167.0.v44c0c51b
2.8.0-snapshot.20230929.12167.0.v44c0c51b
2.8.0-snapshot.20230928.12164.0.ve6bf0171
2.8.0-snapshot.20230927.12158.0.vb863e65a
2.8.0-snapshot.20230925.12146.0.v5ac74bc8
2.7.5
2.7.4
2.7.3
2.3.17
2.3.16
2.8.0-snapshot.20230913.12113.0.vf71b764f
2.8.0-snapshot.20230908.12101.0.v1bee52e4
2.8.0-snapshot.20230830.12083.0.v20330329
2.8.0-snapshot.20230828.12078.0.v99f466d4
2.7.2-rc1
2.7.1
2.7.1-rc2
2.8.0-snapshot.20230821.12054.0.v9f9ecb20
2.8.0-snapshot.20230811.12033.0.v2aad9b4e
2.7.0
2.7.0-snapshot.20230808.11990.0.vb6c93df1
2.8.0-snapshot.20230807.12017.0.v7ba1e675
2.7.0-snapshot.20230719.11983.0.vd999a21a
2.7.0-snapshot.20230717.11972.0.vc71b781c
2.7.0-snapshot.20230710.11943.0.v20c0149b
2.7.0-snapshot.20230703.11931.0.vc04c7ac9
2.7.0-snapshot.20230626.11909.0.v5979774e
2.7.0-snapshot.20230619.11890.0.vedd1a5f6
2.7.0-snapshot.20230612.11864.0.v3514a4a0
2.7.0-snapshot.20230604.11848.0.v9f55d679
2.7.0-snapshot.20230529.11827.0.v3fbe7d01
2.6.5
2.7.0-snapshot.20230518.11799.0.50b2c595
2.7.0-snapshot.20230515.11783.0.36293a4e
2.7.0-snapshot.20230427.11728.0.1f3c22cf
2.7.0-snapshot.20230424.11710.0.2f27f987
2.6.4
2.3.15
2.3.14
2.3.13
2.3.12
2.6.3
2.6.2
2.7.0-snapshot.20230417.11682.0.ac68bdf4
2.7.0-snapshot.20230403.11641.0.1a58e9aa
2.7.0-snapshot.20230331.11634.0.c20fc05d
2.6.1
2.3.11
2.7.0-snapshot.20230327.11615.0.9aa586fb
2.7.0-snapshot.20230317.11591.0.4df21fe6
2.7.0-snapshot.20230310.11563.0.861d9175
2.5.5
2.3.10
2.7.0-snapshot.20230302.11525.0.15e10bbc
2.6.0
2.6.0-snapshot.20230302.11477.0.f98e37e8
2.3.9
2.6.0-snapshot.20230222.11475.0.ec147a82
2.6.0-snapshot.20230222.11471.0.d1a33784
2.6.0-snapshot.20230216.11444.0.59bc473b
2.5.4
2.6.0-snapshot.20230210.11415.0.5c00481a
2.6.0-snapshot.20230206.11376.0.2713e2b4
2.5.3
2.6.0-snapshot.20230126.11324.0.a87412b6
2.5.2
2.4.3
2.6.0-snapshot.20230123.11292.0.b3f84bfc
2.6.0-snapshot.20230116.11260.0.94327f5d
2.5.1
2.6.0-snapshot.20221226.11190.0.71548477
2.6.0-snapshot.20221218.11169.0.9d007b31
2.3.8
2.6.0-snapshot.20221212.11134.0.1ac41995
2.5.0
2.5.0-snapshot.20221208.11077.0.53b7db71
2.6.0-snapshot.20221205.11091.0.c24c17d5
2.5.0-snapshot.20221201.11065.0.caac1d10
2.5.0-snapshot.20221120.10983.0.218a6a8a
2.5.0-snapshot.20221111.10938.0.bb67fb7f
2.4.2
2.5.0-snapshot.20221107.10906.0.346bb489
2.3.7
2.5.0-snapshot.20221028.10865.0.1b726fe8
2.5.0-snapshot.20221024.10827.0.c8adc54a
2.5.0-snapshot.20221017.10775.0.61e85b19
2.5.0-snapshot.20221010.10736.0.2f453a14
2.5.0-snapshot.20221003.10705.0.a55ccc38
2.3.6
2.5.0-snapshot.20220925.10653.0.0db37eca
2.5.0-snapshot.20220919.10619.0.86cff50d
2.4.0
2.4.0-snapshot.20220914.10592.0.cf7c2b5c
2.5.0-snapshot.20220912.10598.0.f801cd46
2.4.0-snapshot.20220909.10591.0.33f2ea59
2.4.0-snapshot.20220905.10544.0.a2ea3ce6
2.4.0-snapshot.20220830.10494.0.4622de48
2.4.0-snapshot.20220823.10466.0.1ab16059
2.4.0-snapshot.20220809.10365.0.7d59e3d4
2.4.0-snapshot.20220805.10346.0.454fc397
2.4.0-snapshot.20220801.10312.0.d2c7be9d
2.4.0-snapshot.20220722.10254.0.9744c4ed
2.3.4
2.3.3
2.3.2
2.3.1
2.4.0-snapshot.20220712.10212.0.0bf28176
2.4.0-snapshot.20220708.10196.0.7dfbc582
2.3.0
2.3.0-snapshot.20220707.10108.0.f4098846
2.2.1
2.4.0-snapshot.20220703.10163.0.89b068ec
1.18.3
2.4.0-snapshot.20220628.10133.0.6f31303a
2.3.0-snapshot.20220621.10121.0.2ea1c740
1.18.3-snapshot.20220620.8442.0.c7424302
2.3.0-snapshot.20220619.10104.0.253b0b18
2.3.0-snapshot.20220611.10066.0.458cfc43
2.3.0-snapshot.20220606.10031.0.ce98be86
2.3.0-snapshot.20220528.9973.0.012e3ac6
2.3.0-snapshot.20220519.9931.0.385504fd
2.0.1
2.3.0-snapshot.20220517.9906.0.de6da710
2.2.0
2.3.0-snapshot.20220509.9874.0.0798fe15
2.2.0-snapshot.20220504.9851.0.4c8e027d
2.2.0-snapshot.20220425.9780.0.f4d60375
2.2.0-snapshot.20220420.9744.0.e193f421
2.0.1-snapshot.20220419.9374.0.44df8f12
2.1.0-snapshot.20220411.9707.0.f6fed6ea
2.1.1
2.1.0
2.1.0-snapshot.20220404.9660.0.4b71431b
2.1.0-snapshot.20220331.9659.0.4bf0de89
2.0.1-snapshot.20220331.9373.0.4a445be6
2.1.0-snapshot.20220325.9626.0.4a483381
1.18.2
1.18.2-snapshot.20220324.8440.0.5cfb1a21
2.0.1-snapshot.20220322.9371.1.d9f46c43
2.1.0-snapshot.20220318.9568.0.d37a63f5
2.1.0-snapshot.20220311.9514.0.ab6e085f
1.18.2-snapshot.20220310.8437.1.5221a5d1
2.1.0-snapshot.20220308.9472.0.1f24348e
2.1.0-snapshot.20220228.9413.0.ef8a0e66
2.0.0
2.0.0-snapshot.20220225.9368.0.b291a57e
2.0.0-snapshot.20220222.9362.0.1af680cd
2.0.0-snapshot.20220215.9307.0.55fef9cf
2.0.0-snapshot.20220209.9212.0.b7fc9f57
2.0.0-snapshot.20220204.9165.0.225c58f4
2.0.0-snapshot.20220201.9108.0.aa2494f1
2.0.0-snapshot.20220127.9042.0.4038d0a7
2.0.0-snapshot.20220118.8919.0.d0813e61
1.18.1
1.18.1-snapshot.20220113.8434.1.1724373f
1.18.1-snapshot.20220113.8434.0.1724373f
2.0.0-snapshot.20220110.8812.0.3a08380b
1.18.1-snapshot.20220110.8433.0.e8e25a11
1.18.1-snapshot.20220106.8429.0.ea344666
2.0.0-snapshot.20220104.8767.0.d3101e01
2.0.0-snapshot.20211220.8735.0.606a8ef0
2.0.0-snapshot.20211207.8608.0.c4d82f72
1.18.0
1.18.0-snapshot.20211206.8423.0.2c945fcb
2.0.0-snapshot.20211201.8538.0.1d0ff3cc
1.18.0-snapshot.20211124.8419.0.39d9afcd
1.17.2-snapshot.20211124.7857.0.b33b94b8
2.0.0-snapshot.20211123.8463.0.bd2a6852
1.11.3
1.11.3-snapshot.20211123.6448.0.c679b4ab
1.18.0-snapshot.20211117.8399.0.a05a40ae
1.18.0-snapshot.20211111.8349.0.d938a44c
1.11.3-snapshot.20211111.6441.0.eed4ad32
1.18.0-snapshot.20211110.8337.0.c5a1f0bb
1.18.0-snapshot.20211109.8328.0.92181161
1.18.0-snapshot.20211102.8257.0.7391a3cd
1.18.0-snapshot.20211026.8179.0.e474b2d1
1.18.0-snapshot.20211019.8113.0.8ff347d8
1.18.0-snapshot.20211013.8071.0.514e8b50
1.17.1
1.17.1-snapshot.20211006.7853.0.e05be365
1.17.1-snapshot.20211006.7852.0.fbc4bc00
1.18.0-snapshot.20211006.8003.0.cfcdc13c
1.18.0-snapshot.20210928.7948.0.b4d00317
1.18.0-snapshot.20210922.7908.0.ced4a272
1.17.0
1.17.0-snapshot.20210922.7849.0.49a75801
1.17.0-snapshot.20210915.7841.0.b4328b3d
1.17.0-snapshot.20210910.7786.0.976ca400
1.17.0-snapshot.20210831.7702.0.f058c2f1
1.17.0-snapshot.20210824.7647.0.640fb683
1.17.0-snapshot.20210817.7604.0.0c187853
1.17.0-snapshot.20210811.7565.0.f1a55aa4
1.16.0
1.16.0-snapshot.20210805.7501.0.48050ad7
1.16.0-snapshot.20210802.7499.0.5157ad6d
1.16.0-snapshot.20210727.7476.0.b5e9d861
1.16.0-snapshot.20210720.7404.0.b7cf42d1
1.15.0
1.16.0-snapshot.20210713.7343.0.1f35db17
1.14.2
1.14.2-snapshot.20210708.7135.0.aa297840
1.14.1-snapshot.20210706.7133.0.8ec16e94
1.15.0-snapshot.20210705.7286.0.62aabcc4
1.15.0-snapshot.20210630.7261.0.84e1f3a7
1.15.0-snapshot.20210623.7217.0.5b73813d
1.15.0-snapshot.20210615.7169.0.adeba206
1.14.0
1.14.0-snapshot.20210615.7124.0.d1b54ff0
1.11.2
1.11.2-snapshot.20210610.6433.1.a08f6ea2
1.14.0-snapshot.20210608.7123.0.3cb8d5c2
1.14.0-snapshot.20210602.7086.0.f36f556b
1.14.0-snapshot.20210526.7024.0.aedb9a82
1.14.0-snapshot.20210518.6953.0.a6c7b86a
1.13.1
1.13.1-snapshot.20210512.6834.1.5f532380
1.14.0-snapshot.20210511.6892.0.ca9e89b3
1.13.0
1.13.0-snapshot.20210504.6833.0.9ae787d0
1.13.0-snapshot.20210503.6809.0.ca012c3b
1.13.0-snapshot.20210426.6770.0.ca66061b
1.13.0-snapshot.20210419.6730.0.8c3a8c04
1.13.0-snapshot.20210413.6706.0.2dc09ba2
1.12.0
1.12.0-snapshot.20210406.6646.0.631db446
1.12.0-snapshot.20210331.6640.0.4b807899
1.12.0-snapshot.20210323.6567.0.90c5ce70
1.12.0-snapshot.20210317.6528.0.493e2154
1.12.0-snapshot.20210312.6498.0.707c86aa
1.11.1
1.11.1-snapshot.20210312.6429.1.7cd6380e
1.12.0-snapshot.20210309.6463.0.f7abca91
1.11.0
1.11.0-snapshot.20210304.6422.0.d3d5042a
1.11.0-snapshot.20210303.6421.0.145ddaa8
1.10.2
1.8.0-snapshot.20210303.5843.0.d443707c
1.11.0-snapshot.20210225.6390.0.0617fbde
1.10.1
1.11.0-snapshot.20210217.6338.0.ba6ba901
1.11.0-snapshot.20210212.6300.0.ad161d7f
1.10.0
1.10.0-snapshot.20210209.6265.0.19bf4031
1.10.0-snapshot.20210208.6257.0.61feb5bf
1.10.0-snapshot.20210202.6218.0.c0573678
1.10.0-snapshot.20210201.6207.0.7cf1914d
1.10.0-snapshot.20210125.6143.0.550aa48f
1.10.0-snapshot.20210120.6106.0.58ef725a
1.9.0
1.9.0-snapshot.20210119.6062.0.5b3663a5
1.8.1
1.8.1-snapshot.20210115.5842.0.59f5d407
1.9.0-snapshot.20210113.6060.0.9ed787cb
1.9.0-snapshot.20210112.6040.0.7171cb38
1.9.0-snapshot.20210111.6034.0.7855b023
1.9.0-snapshot.20210106.5986.0.c6995a9c
1.8.0
1.9.0-snapshot.20201215.5907.0.a6ed34c5
1.6.1
1.6.1-snapshot.20201215.5318.0.547abc97
1.8.0-snapshot.20201214.5841.0.a8ae8e4a
1.8.0-snapshot.20201208.5840.0.38455e8c
1.8.0-snapshot.20201201.5776.0.4b91f2a6
1.8.0-snapshot.20201124.5709.0.dabd55d0
1.8.0-snapshot.20201117.5661.0.76fae40c
1.8.0-snapshot.20201110.5615.0.b35c9fcb
1.7.0
1.7.0-snapshot.20201103.5565.0.e75d42dd
1.7.0-snapshot.20201027.5530.0.bdbf8977
1.7.0-snapshot.20201023.5508.0.9dec6689
1.6.1-snapshot.20201021.5317.0.aafe46a5
1.7.0-snapshot.20201013.5418.0.bda13392
1.7.0-snapshot.20201012.5405.0.af92198d
1.6.0
1.6.0-snapshot.20201012.5316.0.d21cb496
1.6.0-snapshot.20201007.5314.0.b4a47d0b
1.7.0-snapshot.20201006.5358.0.0c1cadcf
1.6.0-snapshot.20200930.5312.0.b9a1905d
1.6.0-snapshot.20200922.5258.0.cd4a06db
1.6.0-snapshot.20200915.5208.0.09014dc6
1.6.0-snapshot.20200908.5166.0.1623baec
1.5.0
1.5.0-snapshot.20200907.5151.0.eb68e680
1.5.0-snapshot.20200902.5118.0.2b3cf1b3
1.5.0-snapshot.20200825.5071.0.d33e130f
1.5.0-snapshot.20200818.5027.0.1b33d374
1.5.0-snapshot.20200811.4959.0.bbc2fe56
1.5.0-snapshot.20200804.4902.0.de2fef6b
1.4.0
1.4.0-snapshot.20200729.4851.0.224ab362
1.4.0-snapshot.20200724.4812.0.818a52b0
1.4.0-snapshot.20200722.4800.0.21a16eef
1.4.0-snapshot.20200722.4796.0.28ab504b
1.4.0-snapshot.20200715.4733.0.d6e58626
1.3.0
1.3.0-snapshot.20200714.4687.0.8e10c7a7
1.3.0-snapshot.20200708.4686.0.95dfa18e
1.3.0-snapshot.20200706.4664.0.5db06051
1.3.0-snapshot.20200610.4413.0.11b5c362
1.3.0-snapshot.20200701.4616.0.bdbefd11
1.3.0-snapshot.20200623.4546.0.4f68cfc4
1.3.0-snapshot.20200617.4484.0.7e0a6848
1.3.0-snapshot.20200617.4474.0.53bddb54
1.3.0-snapshot.20200610.4412.0.0544323d
1.3.0-snapshot.20200603.4345.0.1386abc0
1.2.0
1.2.0-snapshot.20200602.4310.0.1c18058f
1.2.0-snapshot.20200528.4309.0.f619dea3
1.2.0-snapshot.20200527.4268.0.acc5a21c
1.2.0-snapshot.20200520.4228.0.595f1e27
1.2.0-snapshot.20200520.4224.0.2af134ca
1.2.0-snapshot.20200513.4172.0.021f4af3
1.1.1
1.1.0-snapshot.20200506.4107.0.7e448d81
1.1.0-snapshot.20200430.4057.0.681c862d
1.0.1
1.0.1-snapshot.20200424.3917.0.16093690
1.1.0-snapshot.20200422.3991.0.6391ee9f
1.0.1-snapshot.20200417.3908.1.722bac90
1.0.0
0.13.56-snapshot.20200411.3905.0.f050da78
0.13.56-snapshot.20200408.3877.0.1ddcd3c0
0.13.56-snapshot.20200408.3871.0.b3ccacc0
0.13.56-snapshot.20200407.3859.0.b488b353
0.13.56-snapshot.20200407.3843.0.10bac143
0.13.56-snapshot.20200404.3816.0.30f2c742
0.13.56-snapshot.20200331.3729.0.b43b8d86
0.13.56-snapshot.20200325.3626.0.a3ddde3a
0.13.56-snapshot.20200318.3529.0.6ea118d6
0.13.55
0.13.55-snapshot.20200309.3401.0.6f8c3ad8
0.13.55-snapshot.20200304.3329.6a1c75cf
0.13.55-snapshot.20200226.3267.c9b9293d
0.13.54
0.13.53
0.13.52
0.13.51
0.13.50
0.13.46
0.13.43
0.13.42
0.13.41
0.13.40
0.13.39
0.13.38
0.13.37
0.13.36
0.13.34
0.13.33
0.13.32
0.13.31
0.13.30
0.13.29
0.13.27
0.13.25
0.13.24
0.13.23
0.13.22
0.13.21
0.13.20
0.13.19
0.13.18
0.13.16
0.13.15
0.13.14
0.13.13
0.13.12
0.13.10
0.13.5
0.13.0
0.12.25
0.12.24
0.12.22
0.12.21
0.12.20
0.12.19
0.12.18
0.12.17
0.12.16
0.12.15
0.12.12
0.12.11
0.12.3" > $DAML_CACHE/versions.txt
}

init_old_cache () {
  echo "2.7.4
2.7.3
2.3.17
2.3.16
2.7.1
2.7.0
2.6.5
2.6.4
2.3.15
2.3.14
2.3.13
2.3.12
2.6.3
2.6.2
2.6.1
2.3.11
2.5.5
2.3.10
2.6.0
2.5.3
2.5.2
2.4.3
2.5.1
2.3.8
2.5.0
2.4.2
2.3.7
2.3.6
2.4.0
2.3.4
2.3.3
2.3.2
2.3.1
2.3.0
1.18.3
2.0.1
2.2.0
2.1.1
2.1.0
1.18.2
2.0.0
1.18.1
1.18.0
1.11.3
1.17.1
1.17.0
1.16.0
1.15.0
1.14.2
1.14.0
1.11.2
1.13.1
1.13.0
1.12.0
1.11.1
1.11.0
1.10.0
1.9.0
1.8.1
1.8.0
1.6.1
1.7.0
1.6.0
1.5.0
1.4.0
1.3.0
1.2.0
1.1.1
1.0.1
1.0.0
0.13.55
0.13.54
0.13.53
0.13.52
0.13.51
0.13.50
0.13.46
0.13.43
0.13.42
0.13.41
0.13.40
0.13.39
0.13.38
0.13.37
0.13.36
0.13.34
0.13.33
0.13.32
0.13.31
0.13.30
0.13.29
0.13.27
0.13.25
0.13.24
0.13.23
0.13.22
0.13.21
0.13.20
0.13.19
0.13.18
0.13.16
0.13.15
0.13.14
0.13.13
0.13.12
0.13.10
0.13.5
0.13.0
0.12.25
0.12.24
0.12.22
0.12.21
0.12.20
0.12.19
0.12.18
0.12.17
0.12.16
0.12.15
0.12.12
0.12.11
0.12.3" > $DAML_CACHE/versions.txt
}

no_cache_override_github_endpoint () {
  rm $DAML_CACHE/versions.txt
  export RELEASES_ENDPOINT=releases-endpoint
  echo "releases-endpoint: http://localhost:8080/releases" >> $DAML_HOME/daml-config.yaml
  mkdir -p $RELEASES_ENDPOINT
  miniserve "$RELEASES_ENDPOINT" &
  export RELEASES_ENDPOINT_MINISERVE=$!
  sleep 5
  cp $1 "$RELEASES_ENDPOINT/releases"
}

