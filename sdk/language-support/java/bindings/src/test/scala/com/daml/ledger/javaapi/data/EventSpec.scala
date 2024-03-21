// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class EventSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "Event.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    eventGen
  ) { event =>
    val converted = Event.fromProtoEvent(event)
    Event.fromProtoEvent(converted.toProtoEvent) shouldEqual converted
  }

  "ExercisedEvent.fromProto" should "roundtrip with protoc" in forAll(exercisedEventGen) { event =>
    val converted = ExercisedEvent fromProto event
    ExercisedEvent fromProto converted.toProto shouldEqual converted
  }

  "CreatedEvents" should "be protected from mutations of the parameters" in forAll(
    createdEventGen
  ) { e =>
    val mutatingWitnesses = new java.util.ArrayList[String](e.getWitnessPartiesList)
    val mutatingSignatories = new java.util.ArrayList[String](e.getSignatoriesList)
    val mutatingObservers = new java.util.ArrayList[String](e.getObserversList)

    val event = new CreatedEvent(
      mutatingWitnesses,
      e.getEventId,
      Identifier.fromProto(e.getTemplateId),
      e.getContractId,
      DamlRecord.fromProto(e.getCreateArguments),
      java.util.Optional.empty(),
      java.util.Optional.empty(),
      mutatingSignatories,
      mutatingObservers,
    )

    mutatingWitnesses.add("INTRUDER!")
    mutatingSignatories.add("INTRUDER!")
    mutatingObservers.add("INTRUDER!")

    event.getWitnessParties should not contain "INTRUDER!"
    event.getSignatories should not contain "INTRUDER!"
    event.getObservers should not contain "INTRUDER!"
  }

  "CreatedEvents" should "disallow mutation of its mutable fields" in forAll(createdEventGen) { e =>
    val event = new CreatedEvent(
      e.getWitnessPartiesList,
      e.getEventId,
      Identifier.fromProto(e.getTemplateId),
      e.getContractId,
      DamlRecord.fromProto(e.getCreateArguments),
      java.util.Optional.empty(),
      java.util.Optional.empty(),
      e.getSignatoriesList,
      e.getObserversList,
    )

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.add("INTRUDER!"))

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.remove(0))
  }

}
