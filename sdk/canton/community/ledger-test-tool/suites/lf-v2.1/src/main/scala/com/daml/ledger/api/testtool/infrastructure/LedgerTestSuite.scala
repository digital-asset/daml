// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participants, PartyAllocation}
import com.daml.ledger.api.testtool.infrastructure.TestConstraints.NoLimitations
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.v2.commands.Command as CommandV1
import com.daml.ledger.api.v2.commands.Command.toJavaProto
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.api.v2.value.{GenMap, Identifier, List as ApiList, Optional, TextMap, Value}
import com.daml.ledger.javaapi.data.{Command, Identifier as JavaIdentifier, Party}
import com.daml.test.evidence.tag.EvidenceTag
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.{EitherValues, LoneElement, OptionValues, TryValues}

import java.util.List as JList
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

abstract class LedgerTestSuite
    extends OptionValues
    with LoneElement
    with TryValues
    with EitherValues {
  private val testCaseBuffer: ListBuffer[LedgerTestCase] = ListBuffer()

  final lazy val tests: Vector[LedgerTestCase] = testCaseBuffer.toVector

  protected final def test(
      shortIdentifier: String,
      description: String,
      partyAllocation: PartyAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      tags: List[EvidenceTag] = List.empty,
      limitation: TestConstraints = NoLimitations,
  )(testCase: ExecutionContext => PartialFunction[Participants, Future[Unit]]): Unit =
    testGivenAllParticipants(
      shortIdentifier,
      description,
      partyAllocation,
      timeoutScale,
      runConcurrently,
      repeated,
      enabled,
      disabledReason,
      tags,
      limitation,
    )((ec: ExecutionContext) => (_: Seq[ParticipantTestContext]) => testCase(ec))

  protected final def testGivenAllParticipants(
      shortIdentifier: String,
      description: String,
      partyAllocation: PartyAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      tags: List[EvidenceTag] = List.empty,
      limitation: TestConstraints = NoLimitations,
  )(
      testCase: ExecutionContext => Seq[ParticipantTestContext] => PartialFunction[
        Participants,
        Future[Unit],
      ]
  ): Unit = {
    val shortIdentifierRef = Ref.LedgerString.assertFromString(shortIdentifier)
    testCaseBuffer.append(
      new LedgerTestCase(
        this,
        shortIdentifierRef,
        description,
        timeoutScale,
        runConcurrently,
        repeated,
        tags,
        limitation,
        enabled,
        disabledReason,
        partyAllocation,
        testCase,
      )
    )
  }

  def name: String = getClass.getSimpleName

  def updateCommands(commands: JList[Command], f: CommandV1 => CommandV1): JList[Command] =
    commands.asScala
      .map(c => CommandV1.fromJavaProto(c.toProtoCommand))
      .map(f)
      .map(c => Command.fromProtoCommand(toJavaProto(c)))
      .asJava

  implicit class IdentifierConverter(id: JavaIdentifier) {
    def toV1: Identifier = Identifier.fromJavaProto(id.toProto)
  }

  implicit def partyToString(party: Party): String = party.getValue

  object ClearIdsImplicits {

    def clearIds(value: Value): Value = {
      val sum = value.sum match {
        case Sum.Record(record) =>
          Sum.Record(
            record.clearRecordId.copy(fields =
              record.fields.map(f => f.copy(value = f.value.map(clearIds)))
            )
          )
        case Sum.Variant(variant) =>
          Sum.Variant(variant.clearVariantId.copy(value = variant.value.map(clearIds)))
        case Sum.List(list) => Sum.List(ApiList(list.elements.map(clearIds)))
        case Sum.Optional(optional) =>
          Sum.Optional(Optional(optional.value.map(clearIds)))
        case Sum.GenMap(genmap) =>
          Sum.GenMap(GenMap(genmap.entries.map { case GenMap.Entry(k, v) =>
            GenMap.Entry(k.map(clearIds), v.map(clearIds))
          }))
        case Sum.TextMap(simplemap) =>
          Sum.TextMap(TextMap(simplemap.entries.map { case TextMap.Entry(k, v) =>
            TextMap.Entry(k, v.map(clearIds))
          }))
        case _ => value.sum
      }
      Value(sum)
    }

    implicit class ClearValueIdsImplicits(record: com.daml.ledger.api.v2.value.Record) {
      // TODO(#16361): remove when java bindings toValue produce an enriched Record w.r.t. type and field name
      def clearValueIds: com.daml.ledger.api.v2.value.Record =
        clearIds(Value(Sum.Record(record))).getRecord
    }
  }

  def assertAcsDelta(events: Seq[Event], acsDelta: Boolean, message: String): Unit =
    assert(
      events.forall(_.event match {
        case Event.Event.Created(created) => created.acsDelta == acsDelta
        case Event.Event.Exercised(exercised) => exercised.acsDelta == acsDelta
        case Event.Event.Empty => true
        case Event.Event.Archived(_) => true
      }),
      message + s", events: ${events.map(_.event).mkString(", ")}",
    )

}

sealed trait TestConstraints
sealed trait JsonSupported extends TestConstraints

object TestConstraints {
  case object NoLimitations extends JsonSupported
  final case class JsonOnly(reason: String, grpcTest: Option[String] = None) extends JsonSupported
  final case class GrpcOnly(reason: String, jsonTest: Option[String] = None) extends TestConstraints
}
