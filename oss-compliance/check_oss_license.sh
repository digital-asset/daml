#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

javapoms=(
    "../sdk/templates/example-ping-pong-grpc-java"
    "../sdk/templates/example-ping-pong-reactive-java"
    "../sdk/templates/example-ping-pong-reactive-components-java"
)

sbtprojects=(
    "../ledger-api"
    "../ledger-client"
    "../ledger"
    "../reference-apps/bond-trading"
    "../sdk/ledger-manager"
#    "../sdk/docs/app-arch-guide/source/code/app-arch-guide"
    "../solutions/cdm/app"
    "../solutions/cdm/script"
    "../testing"
    "../navigator/backend"
    "../navigator/integration-test"
)

DUMMYPOM="<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <name>dummy-pom</name>
  <groupId>com.digitalasset</groupId>
  <artifactId>dummy-pom</artifactId>
  <version>9999.9.9-SNAPSHOT</version>
  <description>dummy-pom</description>
  <scm>
    <url>http://github.com/DACH-NY/da</url>
    <connection>scm:git:git://github.com/digital-asset/daml.git</connection>
    <developerConnection>scm:git:ssh://git@github.com/digital-asset/daml.git</developerConnection>
    <tag>HEAD</tag>
  </scm>
  <dependencies>
  </dependencies>
</project>
"

function check_maven_projects {
    javapom=$1
    java_licenses=${javapom}/target/generated-resources/licenses.xml

    echo "Generate the ${javapom} dependencies"
    cd ${javapom}
    MAVEN_OPTS="-Xmx2048m" mvn package license:aggregate-download-licenses -DskipTests -Dlicense.excludedScopes=test

    cd -

    FAIL_BUILD=${SHOULD_FAIL_BUILD:-yes}

    echo "Generate OSS report"
    pipenv run python3 check-oss-licenses.py --java-deps ${java_licenses}\
                                    --java-deps-format XML\
                                    --fail-build $FAIL_BUILD
    check_oss_result=$?
    if [ $check_oss_result -eq 1 ]
    then
        exit $check_oss_result
    fi
}

function check_sbt_projects {
    sbtproject=$1
    sbt_project_root=${sbtproject}
    sbt_licenses=target/license-reports/aggregate-licenses.csv

    echo "Generate the ${sbtproject} dependencies"
    cd ${sbt_project_root}
    sbt dumpLicenseReport
    #aggregate all sub-modules license csv into one file
    find . -name "*licenses.csv" | xargs cat | sort | uniq | grep -v "Category,License,Dependency,Notes"> ${sbt_licenses}

    cd -

    FAIL_BUILD=${SHOULD_FAIL_BUILD:-yes}

    echo "Generate OSS report"
    pipenv run python check-oss-licenses.py   --java-deps ${sbt_project_root}/${sbt_licenses}\
                                    --java-deps-format CSV\
                                    --fail-build $FAIL_BUILD
    check_oss_result=$?
    if [ $check_oss_result -eq 1 ]
    then
        exit $check_oss_result
    fi
}

#check for maven projects
for javapom in "${javapoms[@]}"
    do
        check_maven_projects ${javapom}
    done

#check for sbt projects
for sbtproject in "${sbtprojects[@]}"
    do
        check_sbt_projects ${sbtproject}
    done

