// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;

import com.daml.ledger.javaapi.data.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.simpleinterface.SimpleInterface;
import tests.template1.SimpleTemplate;

@RunWith(JUnitPlatform.class)
public class TemplateAndInterfaceIdTest {
  @Test
  void templateId() {
    assertEquals(
        SimpleTemplate.TEMPLATE_ID,
        new Identifier("#integration-tests-model", "Tests.Template1", "SimpleTemplate"));
  }

  @Test
  void templateIdWithPackageId() {
    assertEquals(
        SimpleTemplate.TEMPLATE_ID_WITH_PACKAGE_ID,
        new Identifier(
            "c517179aab1a9c9d56295b504fe9355ecf264da2932b0b3b234c7872083fe077",
            "Tests.Template1",
            "SimpleTemplate"));
  }

  @Test
  void interfaceId() {
    assertEquals(
        SimpleInterface.TEMPLATE_ID,
        new Identifier("#integration-tests-model", "Tests.SimpleInterface", "SimpleInterface"));
    assertEquals(SimpleInterface.TEMPLATE_ID, SimpleInterface.INTERFACE_ID);
  }

  @Test
  void interfaceIdWithPackageId() {
    assertEquals(
        SimpleInterface.TEMPLATE_ID_WITH_PACKAGE_ID,
        new Identifier(
            "c517179aab1a9c9d56295b504fe9355ecf264da2932b0b3b234c7872083fe077",
            "Tests.SimpleInterface",
            "SimpleInterface"));
    assertEquals(
        SimpleInterface.TEMPLATE_ID_WITH_PACKAGE_ID, SimpleInterface.INTERFACE_ID_WITH_PACKAGE_ID);
  }
}
