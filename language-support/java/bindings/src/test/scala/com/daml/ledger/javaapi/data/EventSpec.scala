// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class EventSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "Event.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    eventGen) { event =>
    val converted = Event.fromProtoEvent(event)
    Event.fromProtoEvent(converted.toProtoEvent) shouldEqual converted
  }

  "CreatedEvents" should "be protected from mutations of the parameters" in forAll(createdEventGen) {
    e =>
      val mutatingWitnesses = new java.util.ArrayList[String](e.getWitnessPartiesList)
      val mutatingSignatories = new java.util.ArrayList[String](e.getSignatoriesList)
      val mutatingObservers = new java.util.ArrayList[String](e.getObserversList)

      val event = new CreatedEvent(
        mutatingWitnesses,
        e.getEventId,
        Identifier.fromProto(e.getTemplateId),
        e.getContractId,
        Record.fromProto(e.getCreateArguments),
        java.util.Optional.empty(),
        java.util.Optional.empty(),
        mutatingSignatories,
        mutatingObservers
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
      Record.fromProto(e.getCreateArguments),
      java.util.Optional.empty(),
      java.util.Optional.empty(),
      e.getSignatoriesList,
      e.getObserversList
    )

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.add("INTRUDER!"))

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.remove(0))
  }

}
