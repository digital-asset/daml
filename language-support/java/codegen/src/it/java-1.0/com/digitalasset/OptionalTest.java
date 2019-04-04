// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset;

import com.daml.ledger.javaapi.data.*;
import da.internal.prelude.optional.Some;
import da.internal.prelude.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.optionaltest.MyListOfOptionalsRecord;
import tests.optionaltest.MyOptionalListRecord;
import tests.optionaltest.MyOptionalRecord;
import tests.optionaltest.NestedOptionalRecord;
import tests.optionaltest.optionalvariant.OptionalParametricVariant;
import tests.optionaltest.optionalvariant.OptionalPrimVariant;

import java.util.Arrays;
import java.util.List;


@RunWith(JUnitPlatform.class)
public class OptionalTest {

    @Test
    void optionalFieldAsPreludeOptional() {
        Record record = new Record(Arrays.asList(
                new Record.Field(
                        new Variant("Some", new Int64(42))),
                new Record.Field(
                        new Variant("Some", Unit.getInstance())

                ))
        );
        MyOptionalRecord fromValue = MyOptionalRecord.fromValue(record);
        MyOptionalRecord expected = new MyOptionalRecord(
                new Some<Long>(42L),
                new Some<Unit>(Unit.getInstance())
        );
        Assertions.assertEquals(expected, fromValue);
    }

    @Test
    void nestedOptional() {
        Record record = new Record(
                new Record.Field(
                        new Variant("Some",
                                new Variant("Some",
                                        new Int64(42)))));
        NestedOptionalRecord fromValue = NestedOptionalRecord.fromValue(record);
        NestedOptionalRecord expected = new NestedOptionalRecord(
                new Some<>(new Some<>(42L))
        );
        Assertions.assertEquals(expected, fromValue);
    }

    @Test
    void optionalList() {

        Record record = new Record(
                new Record.Field(
                        new Variant("Some",
                                new DamlList(new Int64(42))
                        )
                )
        );

        MyOptionalListRecord fromValue = MyOptionalListRecord.fromValue(record);
        MyOptionalListRecord expected = new MyOptionalListRecord(new Some<List<Long>>(Arrays.<Long>asList(42L)));

        Assertions.assertEquals(expected, fromValue);
    }

    @Test
    void listOfOptionals() {
        Record record = new Record(
                new Record.Field(new DamlList(new Variant("Some", new Int64(42))))
        );

        MyListOfOptionalsRecord fromValue = MyListOfOptionalsRecord.fromValue(record);
        MyListOfOptionalsRecord expected = new MyListOfOptionalsRecord(Arrays.<Optional<Long>>asList(new Some<>(42L)));

        Assertions.assertEquals(expected, fromValue);

    }

    @Test
    void parametricOptionalVariant() {
        Variant variant = new Variant("OptionalParametricVariant", new Variant("Some", new Int64(42)));

        OptionalParametricVariant<Long> fromValue = OptionalParametricVariant.<Long>fromValue(variant, f -> f.asInt64().get().getValue());
        OptionalParametricVariant<Long> fromConstructor = new OptionalParametricVariant<>(new Some<>(42L));


        Assertions.assertEquals(fromValue, fromConstructor);
        Assertions.assertEquals(fromConstructor.toValue(Int64::new), variant);
    }

    @Test
    void primOptionalVariant() {
        Variant variant = new Variant("OptionalPrimVariant", new Variant("Some", new Int64(42)));

        OptionalPrimVariant<?> fromValue = OptionalPrimVariant.fromValue(variant);
        OptionalPrimVariant<?> fromConstructor = new OptionalPrimVariant(new Some<>(42L));


        Assertions.assertEquals(fromValue, fromConstructor);
        Assertions.assertEquals(fromConstructor.toValue(), variant);
    }
}
