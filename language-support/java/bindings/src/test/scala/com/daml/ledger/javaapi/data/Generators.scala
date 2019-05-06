// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.time.{Instant, LocalDate}

import com.digitalasset.ledger.api.v1.{
  ActiveContractsServiceOuterClass,
  EventOuterClass,
  TransactionFilterOuterClass,
  ValueOuterClass
}
import com.google.protobuf.Empty
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConverters._

object Generators {

  def valueGen: Gen[ValueOuterClass.Value] =
    Gen.sized(
      height =>
        if (height <= 0) unitValueGen
        else
          Gen.oneOf(
            recordValueGen,
            variantValueGen,
            contractIdValueGen,
            listValueGen,
            int64ValueGen,
            decimalValueGen,
            textValueGen,
            timestampValueGen,
            partyValueGen,
            boolValueGen,
            unitValueGen,
            dateValueGen
        ))

  def recordGen: Gen[ValueOuterClass.Record] =
    for {
      recordId <- Gen.option(identifierGen)
      fields <- Gen.sized(height =>
        for {
          size <- Gen.size.flatMap(maxSize => Gen.chooseNum(1, maxSize))
          newHeight = height / size
          withLabel <- Arbitrary.arbBool.arbitrary
          recordFields <- Gen.listOfN(size, Gen.resize(newHeight, recordFieldGen(withLabel)))
        } yield recordFields)
    } yield {
      val builder = ValueOuterClass.Record.newBuilder()
      recordId.foreach(builder.setRecordId)
      builder.addAllFields(fields.asJava)
      builder.build()
    }

  def recordValueGen: Gen[ValueOuterClass.Value] = recordGen.map(valueFromRecord)

  def valueFromRecord(
      record: ValueOuterClass.Record): com.digitalasset.ledger.api.v1.ValueOuterClass.Value = {
    ValueOuterClass.Value.newBuilder().setRecord(record).build()
  }

  def identifierGen: Gen[ValueOuterClass.Identifier] =
    for {
      moduleName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      entityName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      packageId <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
    } yield
      ValueOuterClass.Identifier
        .newBuilder()
        .setModuleName(moduleName)
        .setEntityName(entityName)
        .setPackageId(packageId)
        .build()

  def recordLabelGen: Gen[String] =
    for {
      head <- Arbitrary.arbChar.arbitrary
      tail <- Arbitrary.arbString.arbitrary
    } yield head +: tail

  def recordFieldGen(withLabel: Boolean): Gen[ValueOuterClass.RecordField] = {
    if (withLabel) {
      for {
        label <- recordLabelGen
        value <- valueGen
      } yield ValueOuterClass.RecordField.newBuilder().setLabel(label).setValue(value).build()
    } else {
      valueGen.flatMap(ValueOuterClass.RecordField.newBuilder().setValue(_).build())
    }
  }

  def unitValueGen: Gen[ValueOuterClass.Value] =
    Gen.const(ValueOuterClass.Value.newBuilder().setUnit(Empty.newBuilder().build()).build())

  def variantGen: Gen[ValueOuterClass.Variant] =
    for {
      variantId <- identifierGen
      constructor <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield
      ValueOuterClass.Variant
        .newBuilder()
        .setVariantId(variantId)
        .setConstructor(constructor)
        .setValue(value)
        .build()

  def variantValueGen: Gen[ValueOuterClass.Value] =
    variantGen.map(ValueOuterClass.Value.newBuilder().setVariant(_).build())

  def optionalGen: Gen[ValueOuterClass.Optional] =
    Gen
      .option(valueGen)
      .map(_.fold(ValueOuterClass.Optional.getDefaultInstance) { v =>
        ValueOuterClass.Optional.newBuilder().setValue(v).build()
      })

  def optionalValueGen: Gen[ValueOuterClass.Value] =
    optionalGen.map(ValueOuterClass.Value.newBuilder().setOptional(_).build())

  def contractIdValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(ValueOuterClass.Value.newBuilder().setContractId(_).build())

  def listGen: Gen[ValueOuterClass.List] =
    Gen
      .sized(height =>
        for {
          size <- Gen.size.flatMap(maxSize =>
            if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          list <- Gen
            .listOfN(size, Gen.resize(newHeight, valueGen))
            .map(_.asJava)
        } yield list)
      .map(ValueOuterClass.List.newBuilder().addAllElements(_).build())

  def listValueGen: Gen[ValueOuterClass.Value] =
    listGen.map(ValueOuterClass.Value.newBuilder().setList(_).build())

  def int64ValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbLong.arbitrary.map(ValueOuterClass.Value.newBuilder().setInt64(_).build())

  def textValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(ValueOuterClass.Value.newBuilder().setText(_).build())

  def timestampValueGen: Gen[ValueOuterClass.Value] =
    instantGen.map(instant =>
      ValueOuterClass.Value.newBuilder().setTimestamp(instant.toEpochMilli * 1000).build())

  def instantGen: Gen[Instant] =
    Gen
      .chooseNum(
        Instant.parse("0001-01-01T00:00:00Z").toEpochMilli,
        Instant.parse("9999-12-31T23:59:59.999999Z").toEpochMilli)
      .map(Instant.ofEpochMilli)

  def partyValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(ValueOuterClass.Value.newBuilder().setParty(_).build())

  def boolValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbBool.arbitrary.map(ValueOuterClass.Value.newBuilder().setBool(_).build())

  def dateValueGen: Gen[ValueOuterClass.Value] =
    localDateGen.map(d => ValueOuterClass.Value.newBuilder().setDate(d.toEpochDay.toInt).build())

  def localDateGen: Gen[LocalDate] =
    Gen
      .chooseNum(LocalDate.parse("0001-01-01").toEpochDay, LocalDate.parse("9999-12-31").toEpochDay)
      .map(LocalDate.ofEpochDay)

  def decimalValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbBigDecimal.arbitrary.map(d =>
      ValueOuterClass.Value.newBuilder().setDecimal(d.bigDecimal.toPlainString).build())

  def eventGen: Gen[EventOuterClass.Event] =
    for {
      eventBuilder <- eventBuilderGen
    } yield {
      val b = EventOuterClass.Event
        .newBuilder()
      eventBuilder(b)
      b.build()
    }

  def eventBuilderGen: Gen[EventOuterClass.Event.Builder => EventOuterClass.Event] =
    Gen.oneOf(
      createdEventGen.map(e => (b: EventOuterClass.Event.Builder) => b.setCreated(e).build()),
      archivedEventGen.map(e => (b: EventOuterClass.Event.Builder) => b.setArchived(e).build()),
      exercisedEventGen.map(e => (b: EventOuterClass.Event.Builder) => b.setExercised(e).build())
    )

  val createdEventGen: Gen[EventOuterClass.CreatedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      createArgument <- recordGen
      eventId <- Arbitrary.arbString.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
    } yield
      EventOuterClass.CreatedEvent
        .newBuilder()
        .setContractId(contractId)
        .setTemplateId(templateId)
        .setCreateArguments(createArgument)
        .setEventId(eventId)
        .addAllWitnessParties(witnessParties.asJava)
        .build()

  val archivedEventGen: Gen[EventOuterClass.ArchivedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      eventId <- Arbitrary.arbString.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)

    } yield
      EventOuterClass.ArchivedEvent
        .newBuilder()
        .setContractId(contractId)
        .setTemplateId(templateId)
        .setEventId(eventId)
        .addAllWitnessParties(witnessParties.asJava)
        .build()

  val exercisedEventGen: Gen[EventOuterClass.ExercisedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      actingParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      eventId <- Arbitrary.arbString.arbitrary
      choice <- Arbitrary.arbString.arbitrary
      choiceArgument <- valueGen
      isConsuming <- Arbitrary.arbBool.arbitrary
      contractCreatingEventId <- Arbitrary.arbString.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      exerciseResult <- valueGen
    } yield
      EventOuterClass.ExercisedEvent
        .newBuilder()
        .setContractId(contractId)
        .setTemplateId(templateId)
        .addAllActingParties(actingParties.asJava)
        .setChoice(choice)
        .setChoiceArgument(choiceArgument)
        .setConsuming(isConsuming)
        .setContractCreatingEventId(contractCreatingEventId)
        .setEventId(eventId)
        .addAllWitnessParties(witnessParties.asJava)
        .setExerciseResult(exerciseResult)
        .build()

  def transactionFilterGen: Gen[TransactionFilterOuterClass.TransactionFilter] =
    for {
      filtersByParty <- Gen.mapOf(partyWithFiltersGen)
    } yield
      TransactionFilterOuterClass.TransactionFilter
        .newBuilder()
        .putAllFiltersByParty(filtersByParty.asJava)
        .build()

  def partyWithFiltersGen: Gen[(String, TransactionFilterOuterClass.Filters)] =
    for {
      party <- Arbitrary.arbString.arbitrary
      filters <- filtersGen
    } yield (party, filters)

  def filtersGen: Gen[TransactionFilterOuterClass.Filters] =
    for {
      inclusive <- inclusiveGen
    } yield
      TransactionFilterOuterClass.Filters
        .newBuilder()
        .setInclusive(inclusive)
        .build()

  def inclusiveGen: Gen[TransactionFilterOuterClass.InclusiveFilters] =
    for {
      templateIds <- Gen.listOf(identifierGen)
    } yield
      TransactionFilterOuterClass.InclusiveFilters
        .newBuilder()
        .addAllTemplateIds(templateIds.asJava)
        .build()

  def getActiveContractRequestGen: Gen[ActiveContractsServiceOuterClass.GetActiveContractsRequest] =
    for {
      ledgerId <- Arbitrary.arbString.arbitrary
      transactionFilter <- transactionFilterGen
      verbose <- Arbitrary.arbBool.arbitrary
    } yield
      ActiveContractsServiceOuterClass.GetActiveContractsRequest
        .newBuilder()
        .setLedgerId(ledgerId)
        .setFilter(transactionFilter)
        .setVerbose(verbose)
        .build()
}
