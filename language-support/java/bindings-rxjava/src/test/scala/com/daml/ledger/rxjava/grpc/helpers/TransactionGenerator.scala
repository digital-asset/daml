// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.time.Instant
import java.util
import java.util.Collections
import com.daml.ledger.javaapi.data
import com.daml.ledger.rxjava.grpc.helpers.TransactionsServiceImpl.LedgerItem
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.daml.ledger.api.v1.event.{
  ArchivedEvent,
  CreatedEvent,
  Event,
  ExercisedEvent,
  InterfaceView,
}
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind.Exercised
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.{Timestamp => ScalaTimestamp}
import com.google.rpc.{Status => JStatus}
import com.google.rpc.status.{Status => SStatus}
import org.scalacheck.{Arbitrary, Gen, Shrink}

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
object TransactionGenerator {

  implicit def noShrink[A]: Shrink[A] = Shrink.shrinkAny

  val nonEmptyId: Gen[String] = Gen
    .nonEmptyListOf(Arbitrary.arbChar.arbitrary)
    .map(s => {
      if (s.mkString.equals("")) { throw new IllegalStateException() }
      s.mkString
    })

  val timestampGen: Gen[(ScalaTimestamp, Instant)] = for {
    seconds <- Gen.posNum[Long]
    nanos <- Gen.posNum[Int]
  } yield (
    ScalaTimestamp(seconds, nanos),
    Instant.ofEpochSecond(seconds, nanos.toLong),
  )

  val identifierGen: Gen[(Identifier, data.Identifier)] = for {
    packageId <- nonEmptyId
    moduleName <- nonEmptyId
    entityName <- nonEmptyId
  } yield (
    Identifier(packageId, moduleName = moduleName, entityName = entityName),
    new data.Identifier(packageId, moduleName, entityName),
  )

  def recordFieldGen(withLabel: Boolean, height: Int): Gen[(RecordField, data.DamlRecord.Field)] =
    for {
      label <- if (withLabel) nonEmptyId else Gen.const("")
      (scalaValue, javaValue) <- valueGen(height)
    } yield {
      (
        RecordField(label, Some(scalaValue)),
        if (withLabel) new data.DamlRecord.Field(label, javaValue)
        else new data.DamlRecord.Field(javaValue),
      )
    }

  def recordGen(height: Int): Gen[(Record, data.DamlRecord)] =
    for {
      recordId <- Gen.option(identifierGen)
      (fieldsSize, newHeight) <- splitSizeAndHeight(height)
      withLabel <- Arbitrary.arbBool.arbitrary
      fields <- Gen.listOfN(fieldsSize, recordFieldGen(withLabel, newHeight))
      (scalaFields, javaFields) = fields.unzip
    } yield (
      Record(recordId.map(_._1), scalaFields),
      recordId match {
        case Some((_, javaRecordId)) => new data.DamlRecord(javaRecordId, javaFields.asJava)
        case None => new data.DamlRecord(javaFields.asJava)
      },
    )

  private def splitSizeAndHeight(height: Int) =
    for {
      maxSize <- Gen.size
      positiveMaxSize = if (maxSize > 0) maxSize else 1
      size <- Gen.chooseNum(1, positiveMaxSize)
      newHeight = height / size
    } yield (size, newHeight)

  def valueGen(height: Int): Gen[(Value, data.Value)] =
    if (height <= 0) unitValueGen.map { case (scalaUnit, javaUnit) =>
      (Value(scalaUnit), javaUnit)
    }
    else
      Gen
        .oneOf(
          unitValueGen,
          recordValueGen(height - 1),
          variantGen(height - 1),
          contractIdValueGen,
          listValueGen(height - 1),
          int64ValueGen,
          decimalValueGen,
          textValueGen,
          timestampValueGen,
          partyValueGen,
          boolValueGen,
          dateValueGen,
        )
        .map { case (scalaValue, javaValue) =>
          (Value(scalaValue), javaValue)
        }

  def recordValueGen(height: Int): Gen[(Sum.Record, data.DamlRecord)] = recordGen(height).map {
    case (scalaRecord, javaRecord) => (Sum.Record(scalaRecord), javaRecord)
  }

  def variantGen(height: Int): Gen[(Sum.Variant, data.Variant)] =
    for {
      variantId <- Gen.option(identifierGen)
      contructor <- nonEmptyId
      (scalaValue, javaValue) <- valueGen(height)
    } yield (
      Sum.Variant(Variant(variantId.map(_._1), contructor, Some(scalaValue))),
      variantId match {
        case Some((_, javaVariantId)) => new data.Variant(javaVariantId, contructor, javaValue)
        case None => new data.Variant(contructor, javaValue)
      },
    )

  val contractIdValueGen: Gen[(Sum.ContractId, data.ContractId)] = nonEmptyId.map { contractId =>
    (Sum.ContractId(contractId), new data.ContractId(contractId))
  }

  def listValueGen(height: Int): Gen[(Sum.List, data.DamlList)] =
    for {
      (elementsSize, newHeight) <- splitSizeAndHeight(height)
      elements <- Gen.listOfN(elementsSize, valueGen(newHeight))
      (scalaElements, javaElements) = elements.unzip
    } yield (Sum.List(value.List(scalaElements)), data.DamlList.of(javaElements.asJava))

  val int64ValueGen: Gen[(Sum.Int64, data.Int64)] = Arbitrary.arbLong.arbitrary.map { int64 =>
    (Sum.Int64(int64), new data.Int64(int64))
  }

  val decimalValueGen: Gen[(Sum.Numeric, data.Numeric)] = for {
    sign <- Gen.pick(1, List("", "+", "-"))
    leading <- Gen.choose(1, 9)
    decimals <- Gen.listOfN(37, Gen.choose(0, 9))
    text = s"${sign.head}$leading${decimals.take(27).mkString}.${decimals.drop(27).mkString}"
  } yield (Sum.Numeric(text), new data.Numeric(new java.math.BigDecimal(text)))

  val textValueGen: Gen[(Sum.Text, data.Text)] = Arbitrary.arbString.arbitrary.map { text =>
    (Sum.Text(text), new data.Text(text))
  }

  val timestampValueGen: Gen[(Sum.Timestamp, data.Timestamp)] = Arbitrary.arbLong.arbitrary.map {
    timestamp =>
      (Sum.Timestamp(timestamp), new data.Timestamp(timestamp))
  }

  val partyValueGen: Gen[(Sum.Party, data.Party)] = nonEmptyId.map { party =>
    (Sum.Party(party), new data.Party(party))
  }

  val boolValueGen: Gen[(Sum.Bool, data.Bool)] = Arbitrary.arbBool.arbitrary.map { bool =>
    (Sum.Bool(bool), new data.Bool(bool))
  }

  val dateValueGen: Gen[(Sum.Date, data.Date)] = Arbitrary.arbInt.arbitrary.map { date =>
    (Sum.Date(date), new data.Date(date))
  }

  val unitValueGen: Gen[(Sum.Unit, data.Unit)] =
    Gen.const((Sum.Unit(Empty()), data.Unit.getInstance()))

  private val statusGen: Gen[(SStatus, JStatus)] = for {
    code <- Gen.chooseNum(0, Int.MaxValue)
    message <- Gen.alphaNumStr
  } yield (SStatus(code, message), JStatus.newBuilder().setCode(code).setMessage(message).build)

  private val interfaceViewGen
      : Gen[(InterfaceView, (data.Identifier, Either[JStatus, data.DamlRecord]))] = for {
    (scalaInterfaceId, javaInterfaceId) <- identifierGen
    statusOrRecord <- Gen.either(statusGen, Gen.sized(recordGen))
  } yield (
    InterfaceView(
      Some(scalaInterfaceId),
      statusOrRecord.left.toOption.map(_._1),
      statusOrRecord.toOption.map(_._1),
    ),
    (javaInterfaceId, statusOrRecord.left.map(_._2).map(_._2)),
  )

  private val createdEventGen: Gen[(Created, data.CreatedEvent)] = for {
    eventId <- nonEmptyId
    contractId <- nonEmptyId
    agreementText <- Gen.option(Gen.asciiStr)
    contractKey <- Gen.option(valueGen(0))
    (scalaTemplateId, javaTemplateId) <- identifierGen
    (scalaRecord, javaRecord) <- Gen.sized(recordGen)
    signatories <- Gen.listOf(nonEmptyId)
    observers <- Gen.listOf(nonEmptyId)
    interfaceViews <- Gen.listOf(interfaceViewGen)
  } yield (
    Created(
      CreatedEvent(
        eventId,
        contractId,
        Some(scalaTemplateId),
        contractKey.map(_._1),
        Some(scalaRecord),
        None,
        interfaceViews.map(_._1),
        signatories ++ observers,
        signatories,
        observers,
        agreementText,
      )
    ),
    new data.CreatedEvent(
      (signatories ++ observers).asJava,
      eventId,
      javaTemplateId,
      contractId,
      javaRecord,
      interfaceViews.view.collect { case (_, (id, Right(rec))) => (id, rec) }.toMap.asJava,
      interfaceViews.view.collect { case (_, (id, Left(stat))) => (id, stat) }.toMap.asJava,
      agreementText.toJava,
      contractKey.map(_._2).toJava,
      signatories.toSet.asJava,
      observers.toSet.asJava,
    ),
  )

  val archivedEventGen: Gen[(Archived, data.ArchivedEvent)] = for {
    eventId <- nonEmptyId
    contractId <- nonEmptyId
    (scalaTemplateId, javaTemplateId) <- identifierGen
    parties <- Gen.listOf(nonEmptyId)
  } yield (
    Archived(ArchivedEvent(eventId, contractId, Some(scalaTemplateId), parties)),
    new data.ArchivedEvent(parties.asJava, eventId, javaTemplateId, contractId),
  )

  val exercisedEventGen: Gen[(Exercised, data.ExercisedEvent)] = for {
    eventId <- nonEmptyId
    contractId <- nonEmptyId
    (scalaTemplateId, javaTemplateId) <- identifierGen
    mbInterfaceId <- Gen.option(identifierGen)
    scalaInterfaceId = mbInterfaceId.map(_._1)
    javaInterfaceId = mbInterfaceId.map(_._2)
    choice <- nonEmptyId
    (scalaChoiceArgument, javaChoiceArgument) <- Gen.sized(valueGen)
    actingParties <- Gen.listOf(nonEmptyId)
    consuming <- Arbitrary.arbBool.arbitrary
    (scalaChildren, javaChildren) <- eventsGen
    witnessParties <- Gen.listOf(nonEmptyId)
    (scalaExerciseResult, javaExerciseResult) <- Gen.sized(valueGen)
  } yield (
    Exercised(
      ExercisedEvent(
        eventId,
        contractId,
        Some(scalaTemplateId),
        scalaInterfaceId,
        choice,
        Some(scalaChoiceArgument),
        actingParties,
        consuming,
        witnessParties,
        Nil,
        Some(scalaExerciseResult),
      )
    ),
    new data.ExercisedEvent(
      witnessParties.asJava,
      eventId,
      javaTemplateId,
      javaInterfaceId.toJava,
      contractId,
      choice,
      javaChoiceArgument,
      actingParties.asJava,
      consuming,
      Collections.emptyList(),
      javaExerciseResult,
    ),
  )

  val eventGen: Gen[(Event, data.Event)] =
    Gen.oneOf[(Event.Event, data.Event)](createdEventGen, archivedEventGen).map {
      case (scalaEvent, javaEvent) =>
        (Event(scalaEvent), javaEvent)
    }

  def eventsGen: Gen[(List[Event], util.List[data.Event])] = eventGen.map {
    case (scalaEvent, javaEvent) => (List(scalaEvent), List(javaEvent).asJava)
  }

  val transactionGen: Gen[(LedgerItem, data.Transaction)] = for {
    transactionId <- nonEmptyId
    commandId <- nonEmptyId
    workflowId <- nonEmptyId
    (scalaTimestamp, javaTimestamp) <- timestampGen
    (scalaEvents, javaEvents) <- eventsGen
    offset <- Gen.numStr
  } yield (
    LedgerItem(transactionId, commandId, workflowId, scalaTimestamp, scalaEvents, offset),
    new data.Transaction(transactionId, commandId, workflowId, javaTimestamp, javaEvents, offset),
  )

  val transactionTreeGen: Gen[(LedgerItem, data.TransactionTree)] = for {
    transactionId <- nonEmptyId
    commandId <- nonEmptyId
    workflowId <- nonEmptyId
    (scalaTimestamp, javaTimestamp) <- timestampGen
    (scalaEvents, javaEvents) <- eventsGen
    offset <- Gen.numStr
  } yield (
    LedgerItem(transactionId, commandId, workflowId, scalaTimestamp, scalaEvents, offset),
    new data.TransactionTree(
      transactionId,
      commandId,
      workflowId,
      javaTimestamp,
      Collections.emptyMap(),
      Collections.emptyList(),
      offset,
    ),
  )

  val ledgerContentGen: Gen[(List[LedgerItem], List[data.Transaction])] =
    Gen.listOf(transactionGen).map(_.unzip)

  val ledgerContentTreeGen: Gen[(List[LedgerItem], List[data.TransactionTree])] =
    Gen.listOf(transactionTreeGen).map(_.unzip)

  val ledgerContentWithEventIdGen: Gen[(List[LedgerItem], String, data.TransactionTree)] = for {
    (arbitraryLedgerContent, _) <- ledgerContentTreeGen
    (queriedLedgerContent, queriedTransaction) <- transactionTreeGen.suchThat(_._1.events.nonEmpty)
    ledgerContent = arbitraryLedgerContent :+ queriedLedgerContent
    eventIds = queriedLedgerContent.events.map(TransactionsServiceImpl.eventId)
    eventIdList <- Gen.pick(1, eventIds)
    eventId = eventIdList.head
  } yield (ledgerContent, eventId, queriedTransaction)

  val ledgerContentWithTransactionIdGen: Gen[(List[LedgerItem], String, data.TransactionTree)] =
    for {
      (arbitraryLedgerContent, _) <- ledgerContentTreeGen
      (queriedLedgerContent, queriedTransaction) <- transactionTreeGen
      ledgerContent = arbitraryLedgerContent :+ queriedLedgerContent
      transactionId = queriedLedgerContent.transactionId
    } yield (ledgerContent, transactionId, queriedTransaction)

  val nonEmptyLedgerContent: Gen[(List[LedgerItem], List[data.Transaction])] =
    Gen.nonEmptyListOf(transactionGen).map(_.unzip)

}
