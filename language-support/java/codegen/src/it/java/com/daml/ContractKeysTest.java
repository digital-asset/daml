// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import da.types.Tuple2;
import da.types.Tuple3;
import da.types.Tuple4;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.contractkeys.*;

@RunWith(JUnitPlatform.class)
public class ContractKeysTest {

  // NOTE: the tests are mostly here to make sure the code compiles

  NoKey.Contract noKey =
      new NoKey.Contract(
          new NoKey.ContractId("no-key"),
          new NoKey("Alice"),
          Collections.emptySet(),
          Collections.emptySet());
  PartyKey.Contract partyKey =
      new PartyKey.Contract(
          new PartyKey.ContractId("party-key"),
          new PartyKey("Alice"),
          Optional.of("Alice"),
          Collections.emptySet(),
          Collections.emptySet());
  RecordKey.Contract recordKey =
      new RecordKey.Contract(
          new RecordKey.ContractId("record-key"),
          new RecordKey("Alice", 42L),
          Optional.of(new PartyAndInt("Alice", 42L)),
          Collections.emptySet(),
          Collections.emptySet());
  TupleKey.Contract tupleKey =
      new TupleKey.Contract(
          new TupleKey.ContractId("tuple-key"),
          new TupleKey("Alice", 42L),
          Optional.of(new Tuple2<>("Alice", 42L)),
          Collections.emptySet(),
          Collections.emptySet());
  NestedTupleKey.Contract nestedTupleKey =
      new NestedTupleKey.Contract(
          new NestedTupleKey.ContractId("nested-tuple-key"),
          new NestedTupleKey("Alice", 42L, "blah", 47L, true, "foobar", 0L),
          Optional.of(
              new Tuple2<>(
                  new Tuple3<>("Alice", 42L, "blah"), new Tuple4<>(47L, true, "foobar", 0L))),
          Collections.emptySet(),
          Collections.emptySet());

  @Test
  void noKeyHasNoKey() {
    assertFalse(ReflectionUtils.readFieldValue(NoKey.Contract.class, "key", noKey).isPresent());
  }

  @Test
  void allOthersHaveKeys() {
    assertTrue(partyKey.key.isPresent());
    assertTrue(recordKey.key.isPresent());
    assertTrue(tupleKey.key.isPresent());
    assertTrue(nestedTupleKey.key.isPresent());
  }

  @Test
  void partyKeyIsString() {
    assertEquals(partyKey.key.get(), "Alice");
  }

  @Test
  void recordKeyIsPartyAndInt() {
    assertEquals(recordKey.key.get().party, "Alice");
    assertEquals(recordKey.key.get().int$.longValue(), 42L);
  }

  @Test
  void tupleKeyIsPartyAndInt() {
    assertEquals(tupleKey.key.get()._1, "Alice");
    assertEquals(tupleKey.key.get()._2.longValue(), 42L);
  }

  @Test
  void nestedTupleKeyIsPartyAndInt() {
    assertEquals(nestedTupleKey.key.get()._1._1, "Alice");
    assertEquals(nestedTupleKey.key.get()._1._2.longValue(), 42L);
    assertEquals(nestedTupleKey.key.get()._1._3, "blah");
    assertEquals(nestedTupleKey.key.get()._2._1.longValue(), 47L);
    assertEquals(nestedTupleKey.key.get()._2._2.booleanValue(), true);
    assertEquals(nestedTupleKey.key.get()._2._3, "foobar");
    assertEquals(nestedTupleKey.key.get()._2._4.longValue(), 0L);
  }

  @Test
  void roundTripKeyThroughJson() throws JsonLfDecoder.Error {
    assertEquals(partyKey.key.get(), PartyKey.Contract.keyFromJson(partyKey.keyToJson()));
    assertEquals(recordKey.key.get(), RecordKey.Contract.keyFromJson(recordKey.keyToJson()));
    assertEquals(tupleKey.key.get(), TupleKey.Contract.keyFromJson(tupleKey.keyToJson()));
    assertEquals(
        nestedTupleKey.key.get(), NestedTupleKey.Contract.keyFromJson(nestedTupleKey.keyToJson()));
  }
}
