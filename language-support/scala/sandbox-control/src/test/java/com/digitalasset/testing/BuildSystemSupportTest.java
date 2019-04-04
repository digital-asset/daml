// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(JUnitPlatform.class)
public class BuildSystemSupportTest {
    @Rule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    final static String TEST_BINARY = "ledger-client/sandbox/BinaryTestIT";

    void setBazelEnvVars() {
        environmentVariables.set("TEST_SRCDIR", "bazel-out/k8-fastbuild/bin/ledger-client/sandbox.runfiles");
        environmentVariables.set("TEST_WORKSPACE", "com_github_digital_asset_daml");
        environmentVariables.set("TEST_BINARY", TEST_BINARY);
    }

    void unsetBazelEnvVars() {
        environmentVariables.clear("TEST_SRCDIR", "TEST_WORKSPACE", "TEST_BINARY");
    }

    @Test
    public void detectBazel() {
        setBazelEnvVars();
        assertThat("We are running under Bazel", BuildSystemSupport.underBazel());
    }

    @Test
    public void detectNoBazel() {
        unsetBazelEnvVars();
        assertThat("We are not running under Bazel", !BuildSystemSupport.underBazel());
    }

    @Test
    public void returnTheSamePathWhenUnderSbt() {
        unsetBazelEnvVars();
        String str_path = "somepath/somewhere";
        Path path = Paths.get(str_path);

        Path under_sbt = BuildSystemSupport.path(str_path);
        Path under_sbt_from_path = BuildSystemSupport.path(path);

        assertThat("Path should be the same", path.compareTo(under_sbt) == 0);
        assertThat("Path should be the same", path.compareTo(under_sbt_from_path) == 0);
    }

    @Test
    public void returnModifiedPathWhenUnderBazel() {
        setBazelEnvVars();
        String str_path = "somepath/somewhere";
        Path path = Paths.get(str_path);

        Path test_binary = Paths.get(TEST_BINARY);

        Path under_bazel = BuildSystemSupport.path(str_path);
        Path under_bazel_from_path = BuildSystemSupport.path(path);

        assertThat("Path ends with str_path", under_bazel.endsWith(path));
        assertThat("Path ends with str_path", under_bazel_from_path.endsWith(path));

        assertThat("Path (from string) begins with TEST_BINARY", under_bazel.startsWith(test_binary.getParent()));
        assertThat("Path (from path) begins with TEST_BINARY", under_bazel_from_path.startsWith(test_binary.getParent()));
    }
}
