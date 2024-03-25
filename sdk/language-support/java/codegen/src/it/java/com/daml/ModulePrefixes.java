// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import v1.a.T1;
import v2.a.T2;

// We’re only testing that modules get remapped correctly
// while preserving the original module name.
public class ModulePrefixes {
  @Test
  public void packageIds() {
    assertEquals(T1.TEMPLATE_ID.getModuleName(), "A");
    assertEquals(T2.TEMPLATE_ID.getModuleName(), "A");
  }
}
