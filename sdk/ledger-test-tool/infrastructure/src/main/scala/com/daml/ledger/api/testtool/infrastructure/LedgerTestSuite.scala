// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participants, PartyAllocation}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.javaapi.data.{Command, Party}
import com.daml.ledger.javaapi.data.{Identifier => JavaIdentifier}
import com.daml.lf.data.Ref
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.ledger.api.v1.commands.Command.toJavaProto
import com.daml.ledger.api.v1.commands.{Command => CommandV1}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{GenMap, Identifier, Optional, Value}
import com.daml.ledger.api.v1.value.{List => ListV1}
import com.daml.ledger.api.v1.value.{Map => MapV1}

import java.math.BigDecimal
import java.time.Instant
import java.util.{List => JList}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

abstract class LedgerTestSuite {
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
  )(testCase: ExecutionContext => PartialFunction[Participants, Future[Unit]]): Unit = {
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
    )((ec: ExecutionContext) => (_: Seq[ParticipantTestContext]) => testCase(ec))
  }

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

  // TODO: when merged in canton (get it from ledger-common)
  object TimestampConversion {
    val MIN = Instant parse "0001-01-01T00:00:00Z"
    val MAX = Instant parse "9999-12-31T23:59:59.999999Z"
  }

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
        case Sum.List(list) => Sum.List(ListV1(list.elements.map(clearIds)))
        case Sum.Optional(optional) =>
          Sum.Optional(Optional(optional.value.map(clearIds)))
        case Sum.GenMap(genmap) =>
          Sum.GenMap(GenMap(genmap.entries.map({ case GenMap.Entry(k, v) =>
            GenMap.Entry(k.map(clearIds), v.map(clearIds))
          })))
        case Sum.Map(simplemap) =>
          Sum.Map(MapV1(simplemap.entries.map({ case MapV1.Entry(k, v) =>
            MapV1.Entry(k, v.map(clearIds))
          })))
        case _ => value.sum
      }
      Value(sum)
    }

    implicit class ClearValueIdsImplicits(record: com.daml.ledger.api.v1.value.Record) {
      // TODO: remove when java bindings toValue produce an enriched Record w.r.t. type and field name
      def clearValueIds: com.daml.ledger.api.v1.value.Record =
        clearIds(Value(Sum.Record(record))).getRecord
    }
  }

  object BigDecimalImplicits {
    // TODO: when merged in canton (merge it w/ the corresponding BigDecimalImplicits)

    implicit class IntToBigDecimal(value: Int) {
      def toBigDecimal: BigDecimal = BigDecimal.valueOf(value.toLong)
    }

    implicit class DoubleToBigDecimal(value: Double) {
      def toBigDecimal: BigDecimal = BigDecimal.valueOf(value)
    }

  }
}
