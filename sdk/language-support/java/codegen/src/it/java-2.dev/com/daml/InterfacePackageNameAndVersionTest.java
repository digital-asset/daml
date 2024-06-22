// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;

import com.daml.ledger.javaapi.data.PackageVersion;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.simpleinterface.SimpleInterface;

// TODO: Merge with PackageNameAndVersionTest.java once interfaces are marked stable
@RunWith(JUnitPlatform.class)
public class InterfacePackageNameAndVersionTest {
  @Test
  void packageName() {
    assertEquals(SimpleInterface.PACKAGE_NAME, "integration-tests-model");
  }

  @Test
  void packageVersion() {
    assertEquals(SimpleInterface.PACKAGE_VERSION, PackageVersion.unsafeFromString("1.2.3"));
  }
}
