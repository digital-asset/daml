// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.data.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.template1.SimpleTemplate;
import tests.template1.TestTemplate_Int;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class TemplateMethodTest {

    // Note that for the most part these tests should are designed to "fail"
    // at compilation time in case any of the methods are generated differently
    // or not at all

    private static Record simpleTemplateRecord = new Record(new Record.Field(new Party("Bob")));

    @Test
    void templateHasCreateMethods() {
        CreateCommand fromStatic = SimpleTemplate.create("Bob");
        CreateCommand fromInstance = new SimpleTemplate("Bob").create();

        assertNotNull(fromStatic, "CreateCommand from static method was null");
        assertNotNull(fromInstance, "CreateCommand from method was null");
    }

    @Test
    void contractIdHasInstanceExerciseMethods() {
        SimpleTemplate.ContractId cid = new SimpleTemplate.ContractId("id");
        ExerciseCommand fromSplattedInt = cid.exerciseTestTemplate_Int(42L);
        ExerciseCommand fromRecordInt = cid.exerciseTestTemplate_Int(new TestTemplate_Int(42L));
        ExerciseCommand fromSplattedUnit = cid.exerciseTestTemplate_Unit();

        assertNotNull(fromSplattedInt, "ExerciseCommand from splatted choice was null");
        assertNotNull(fromRecordInt, "ExerciseCommand from record choice was null");
        assertNotNull(fromSplattedUnit, "ExerciseCommand from splatted unit choice was null");
    }

    @Test
    void templateHasCreateAndExerciseMethods() {
        SimpleTemplate simple = new SimpleTemplate("Bob");
        CreateAndExerciseCommand fromSplatted = simple.createAndExerciseTestTemplate_Int(42L);
        CreateAndExerciseCommand fromRecord = simple.createAndExerciseTestTemplate_Int(new TestTemplate_Int(42L));

        assertNotNull(fromSplatted, "CreateAndExerciseCommand from splatted choice was null");
        assertNotNull(fromRecord, "CreateAndExerciseCommand from record choice was null");
        assertEquals(fromRecord, fromSplatted, "CreateAndExerciseCommands from both methods are not the same");
    }

    @Test
    void contractHasDeprecatedFromIdAndRecord() {
        SimpleTemplate.Contract contract = SimpleTemplate.Contract.fromIdAndRecord("SomeId", simpleTemplateRecord);
        assertFalse(contract.agreementText.isPresent(), "Field agreementText should not be present");
    }

    @Test
    void contractHasFromIdAndRecord() {
        SimpleTemplate.Contract emptyAgreement = SimpleTemplate.Contract.fromIdAndRecord("SomeId", simpleTemplateRecord, Optional.empty(), Collections.emptySet(), Collections.emptySet());
        assertFalse(emptyAgreement.agreementText.isPresent(), "Field agreementText should not be present");

        SimpleTemplate.Contract nonEmptyAgreement = SimpleTemplate.Contract.fromIdAndRecord("SomeId", simpleTemplateRecord, Optional.of("I agree"), Collections.emptySet(), Collections.emptySet());
        assertTrue(nonEmptyAgreement.agreementText.isPresent(), "Field agreementText should be present");
        assertEquals(nonEmptyAgreement.agreementText, Optional.of("I agree"), "Unexpected agreementText");
    }

    @Test
    void contractHasFromCreatedEvent() {
        CreatedEvent agreementEvent = new CreatedEvent(Collections.emptyList(), "eventId", SimpleTemplate.TEMPLATE_ID, "cid", simpleTemplateRecord, Optional.of("I agree"), Optional.empty(), Collections.emptySet(), Collections.emptySet());
        CreatedEvent noAgreementEvent = new CreatedEvent(Collections.emptyList(), "eventId", SimpleTemplate.TEMPLATE_ID, "cid", simpleTemplateRecord, Optional.empty(), Optional.empty(), Collections.emptySet(), Collections.emptySet());

        SimpleTemplate.Contract withAgreement = SimpleTemplate.Contract.fromCreatedEvent(agreementEvent);
        assertTrue(withAgreement.agreementText.isPresent(), "AgreementText was not present");

        SimpleTemplate.Contract withoutAgreement = SimpleTemplate.Contract.fromCreatedEvent(noAgreementEvent);
        assertFalse(withoutAgreement.agreementText.isPresent(), "AgreementText was present");
    }
}
