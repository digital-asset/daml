// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Update;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import templatewithchoice.TemplateWithChoice;

@RunWith(JUnitPlatform.class)
public class TemplateWithChoiceTest {

  // Test static initialization of the generated template to fix:
  // https://github.com/DACH-NY/canton/issues/31699
  @Test
  void createDoesNotThrow() {
    assertDoesNotThrow(
        () -> {
          Update<Created<TemplateWithChoice.ContractId>> update =
              TemplateWithChoice.create("Alice");
        });
  }
}
