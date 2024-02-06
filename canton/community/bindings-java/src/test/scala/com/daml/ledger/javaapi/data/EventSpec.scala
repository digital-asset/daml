// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.GeneratorsV2.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

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
    createdEventGen,
    identifierGen,
    identifierGen,
  ) { (e, extraIV, extraFIV) =>
    val base = CreatedEvent fromProto e
    def mutList[X](xs: java.util.Collection[_ <: X]) = new java.util.ArrayList[X](xs)
    def mutMap[K, V](m: java.util.Map[K, V]) = new java.util.HashMap(m)
    val mutatingWitnesses = mutList(e.getWitnessPartiesList)
    val mutatingSignatories = mutList(e.getSignatoriesList)
    val mutatingObservers = mutList(e.getObserversList)
    val extraIVJ = Identifier fromProto extraIV
    val extraFIVJ = Identifier fromProto extraFIV
    val mutatingIVs = mutMap(base.getInterfaceViews)
    val mutatingFIVs = mutMap(base.getFailedInterfaceViews)
    mutatingIVs.remove(extraIVJ)
    mutatingIVs.remove(extraFIVJ)

    val event = new CreatedEvent(
      mutatingWitnesses,
      base.getEventId,
      base.getTemplateId,
      base.getContractId,
      base.getArguments,
      base.getCreatedEventBlob,
      mutatingIVs,
      mutatingFIVs,
      base.getContractKey,
      mutatingSignatories,
      mutatingObservers,
      base.createdAt,
    )

    mutatingWitnesses.add("INTRUDER!")
    mutatingSignatories.add("INTRUDER!")
    mutatingObservers.add("INTRUDER!")
    mutatingIVs.put(extraIVJ, base.getArguments)
    mutatingFIVs.put(extraFIVJ, com.google.rpc.Status.getDefaultInstance)

    event.getWitnessParties should not contain "INTRUDER!"
    event.getSignatories should not contain "INTRUDER!"
    event.getObservers should not contain "INTRUDER!"
    event.getInterfaceViews shouldNot contain key extraIVJ
    event.getFailedInterfaceViews shouldNot contain key extraFIVJ
  }

  "CreatedEvents" should "disallow mutation of its mutable fields" in forAll(createdEventGen) { e =>
    val event = CreatedEvent fromProto e

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.add("INTRUDER!"))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.add("INTRUDER!"))

    an[UnsupportedOperationException] shouldBe thrownBy(event.getWitnessParties.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getSignatories.remove(0))
    an[UnsupportedOperationException] shouldBe thrownBy(event.getObservers.remove(0))
  }

}
