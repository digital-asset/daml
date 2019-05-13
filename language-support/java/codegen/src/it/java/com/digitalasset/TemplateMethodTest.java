// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.ExerciseCommand;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.template1.SimpleTemplate;
import tests.template1.TestTemplate_Int;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@RunWith(JUnitPlatform.class)
public class TemplateMethodTest {

    // Note that for the most part these tests should are designed to "fail"
    // at compilation time in case any of the methods are generated differently
    // or not at all

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
    }
}
