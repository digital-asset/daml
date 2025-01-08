// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Exercises;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.google.protobuf.ByteString;
import da.internal.template.Archive;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.template1.SimpleTemplate;
import tests.template1.TestTemplate_Int;

@RunWith(JUnitPlatform.class)
public class TemplateMethodTest {

  // Note that for the most part these tests should are designed to "fail"
  // at compilation time in case any of the methods are generated differently
  // or not at all

  private static final DamlRecord simpleTemplateRecord =
      new DamlRecord(new DamlRecord.Field(new Party("Bob")));

  @Test
  void templateHasCreateMethods() {
    Update<Created<SimpleTemplate.ContractId>> fromStatic = SimpleTemplate.create("Bob");
    Update<Created<SimpleTemplate.ContractId>> fromInstance = new SimpleTemplate("Bob").create();

    assertEquals(
        1, fromStatic.commands().size(), "There are not exactly one command from static method");
    assertEquals(
        1, fromInstance.commands().size(), "There are not exactly one command from method");
  }

  @Test
  void contractIdHasInstanceExerciseMethods() {
    SimpleTemplate.ContractId cid = new SimpleTemplate.ContractId("id");
    var fromSplattedInt = cid.exerciseTestTemplate_Int(42L);
    var fromRecordInt = cid.exerciseTestTemplate_Int(new TestTemplate_Int(42L));
    var fromSplattedUnit = cid.exerciseTestTemplate_Unit();

    assertEquals(
        1,
        fromSplattedInt.commands().size(),
        "There are not exactly one command from Update<R> from splatted choice");
    assertEquals(
        1,
        fromRecordInt.commands().size(),
        "There are not exactly one command from Update<R> from record choice");
    assertEquals(
        1,
        fromSplattedUnit.commands().size(),
        "There are not exactly one command from Update<R> from splatted choice");
  }

  @Test
  void templateHasCreateAndExerciseMethods() {
    SimpleTemplate simple = new SimpleTemplate("Bob");
    var fromSplatted = simple.createAndExerciseTestTemplate_Int(42L);
    var fromRecord = simple.createAndExerciseTestTemplate_Int(new TestTemplate_Int(42L));

    assertNotNull(fromSplatted, "Update<R> from splatted choice was null");
    assertNotNull(fromRecord, "Update<R> from record choice was null");
    assertEquals(
        fromRecord.commands(),
        fromSplatted.commands(),
        "Update<R> commands from both methods are not the same");

    assertEquals(
        1,
        fromSplatted.commands().size(),
        "There are not exactly one command from Update<R> from splatted choice");
    assertEquals(
        1,
        fromRecord.commands().size(),
        "There are not exactly one command from Update<R> from record choice");
  }

  @Test
  void templateHasArchive() {
    SimpleTemplate.ContractId cid = new SimpleTemplate.ContractId("id");
    Exercises.Archivable<?> wideCid = cid;
    assertEquals(
        wideCid.exerciseArchive().commands(), cid.exerciseArchive(new Archive()).commands());
  }

  @Test
  void templateHasGetContractTypeId() {
    assertEquals(new SimpleTemplate("Bob").getContractTypeId(), SimpleTemplate.TEMPLATE_ID);
  }

  private static final CreatedEvent createdEvent =
      new CreatedEvent(
          Collections.emptyList(),
          1L,
          0,
          SimpleTemplate.TEMPLATE_ID,
          "package-name",
          "cid",
          simpleTemplateRecord,
          ByteString.EMPTY,
          Collections.emptyMap(),
          Collections.emptyMap(),
          Optional.empty(),
          Collections.emptySet(),
          Collections.emptySet(),
          Instant.ofEpochMilli(0));

  @Test
  void contractHasCompanion() {
    var companion = SimpleTemplate.COMPANION;
    SimpleTemplate.Contract contract = companion.fromCreatedEvent(createdEvent);
    SimpleTemplate data = contract.data;
    assertEquals(new SimpleTemplate("Bob"), data);
  }

  @Test
  void contractHasGetContractTypeId() {
    var contract = SimpleTemplate.Contract.fromCreatedEvent(createdEvent);
    assertEquals(contract.getContractTypeId(), SimpleTemplate.TEMPLATE_ID);
  }

  @Test
  void contractHasToString() {
    assertEquals(
        "tests.template1.SimpleTemplate.Contract(ContractId(cid), "
            + "tests.template1.SimpleTemplate(Bob), [], [])",
        SimpleTemplate.Contract.fromCreatedEvent(createdEvent).toString());
  }
}
