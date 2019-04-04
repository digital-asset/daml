// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;

import java.lang.System;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Support for relative paths for resources in tests, when run under different build systems.
 *
 * The problem with different build systems is that they might store resource files differently, and having a single
 * hard-coded path does not work. Thus, we detect known build systems (currently, Bazel and SBT), and mangle the path
 * so we have the right location.
 */
public class BuildSystemSupport {
    /**
     * Mangle the given path, taking into account how different build systems handle them.
     * @param path path relative to sub-project root
     * @return correct path with prefix used by the build system
     */
    public static Path path(Path path) {
        if (underBazel()) {
            Path binary_path = Paths.get(System.getenv("TEST_BINARY"));
            if (binary_path.getNameCount() > 1) {
                Path parent = binary_path.getParent();
                return parent.resolve(path);
            } else {
                // just return path
                return path;
            }
        } else {
            // under SBT, so we return unmangled path
            return path;
        }
    }

    /**
     * A helper method that forwards the call to {@see path(Path path)}.
     * @param path A string representing a path.
     * @return correct path with prefix used by the build system
     */
    public static Path path(String path) {
        return path(Paths.get(path));
    }

    /**
     * Returns true if we're under Bazel.
     */
    public static boolean underBazel() {
        String srcdir = System.getenv("TEST_SRCDIR");
        String workspace = System.getenv("TEST_WORKSPACE");

        return srcdir != null && workspace != null;
    }
}
