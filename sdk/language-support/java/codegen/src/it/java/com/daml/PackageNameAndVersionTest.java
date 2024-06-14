// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.template1.SimpleTemplate;

@RunWith(JUnitPlatform.class)
public class PackageNameAndVersionTest {
  @Test
  void packageName() {
    assertEquals(SimpleTemplate.PACKAGE_NAME, "integration-tests-model");
  }

  @Test
  void packageVersion() {
    assertEquals(SimpleTemplate.PACKAGE_VERSION, "1.2.3");
  }
}
