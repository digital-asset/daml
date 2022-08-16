// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.time.{Instant, LocalDate}

import com.daml.ledger.api.v1._
import com.google.protobuf.Empty
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary

import scala.jdk.CollectionConverters._

object Generators {

  def valueGen: Gen[ValueOuterClass.Value] =
    Gen.sized(height =>
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
          dateValueGen,
        )
    )

  def recordGen: Gen[ValueOuterClass.Record] =
    for {
      recordId <- Gen.option(identifierGen)
      fields <- Gen.sized(height =>
        for {
          size <- Gen.size.flatMap(maxSize => Gen.chooseNum(1, maxSize))
          newHeight = height / size
          withLabel <- Arbitrary.arbBool.arbitrary
          recordFields <- Gen.listOfN(size, Gen.resize(newHeight, recordFieldGen(withLabel)))
        } yield recordFields
      )
    } yield {
      val builder = ValueOuterClass.Record.newBuilder()
      recordId.foreach(builder.setRecordId)
      builder.addAllFields(fields.asJava)
      builder.build()
    }

  def recordValueGen: Gen[ValueOuterClass.Value] = recordGen.map(valueFromRecord)

  def valueFromRecord(
      record: ValueOuterClass.Record
  ): com.daml.ledger.api.v1.ValueOuterClass.Value = {
    ValueOuterClass.Value.newBuilder().setRecord(record).build()
  }

  def identifierGen: Gen[ValueOuterClass.Identifier] =
    for {
      moduleName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      entityName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      packageId <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
    } yield ValueOuterClass.Identifier
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
    } yield ValueOuterClass.Variant
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
          size <- Gen.size
            .flatMap(maxSize => if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          list <- Gen
            .listOfN(size, Gen.resize(newHeight, valueGen))
            .map(_.asJava)
        } yield list
      )
      .map(ValueOuterClass.List.newBuilder().addAllElements(_).build())

  def listValueGen: Gen[ValueOuterClass.Value] =
    listGen.map(ValueOuterClass.Value.newBuilder().setList(_).build())

  def textMapGen: Gen[ValueOuterClass.Map] =
    Gen
      .sized(height =>
        for {
          size <- Gen.size
            .flatMap(maxSize => if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          keys <- Gen.listOfN(size, Arbitrary.arbString.arbitrary)
          values <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
        } yield (keys zip values).map { case (k, v) =>
          ValueOuterClass.Map.Entry.newBuilder().setKey(k).setValue(v).build()
        }
      )
      .map(x => ValueOuterClass.Map.newBuilder().addAllEntries(x.asJava).build())

  def textMapValueGen: Gen[ValueOuterClass.Value] =
    textMapGen.map(ValueOuterClass.Value.newBuilder().setMap(_).build())

  def genMapGen: Gen[ValueOuterClass.GenMap] =
    Gen
      .sized(height =>
        for {
          size <- Gen.size
            .flatMap(maxSize => if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          keys <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
          values <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
        } yield (keys zip values).map { case (k, v) =>
          ValueOuterClass.GenMap.Entry.newBuilder().setKey(k).setValue(v).build()
        }
      )
      .map(x => ValueOuterClass.GenMap.newBuilder().addAllEntries(x.asJava).build())

  def genMapValueGen: Gen[ValueOuterClass.Value] =
    genMapGen.map(ValueOuterClass.Value.newBuilder().setGenMap(_).build())

  def int64ValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbLong.arbitrary.map(ValueOuterClass.Value.newBuilder().setInt64(_).build())

  def textValueGen: Gen[ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(ValueOuterClass.Value.newBuilder().setText(_).build())

  def timestampValueGen: Gen[ValueOuterClass.Value] =
    instantGen.map(instant =>
      ValueOuterClass.Value.newBuilder().setTimestamp(instant.toEpochMilli * 1000).build()
    )

  def instantGen: Gen[Instant] =
    Gen
      .chooseNum(
        Instant.parse("0001-01-01T00:00:00Z").toEpochMilli,
        Instant.parse("9999-12-31T23:59:59.999999Z").toEpochMilli,
      )
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
      ValueOuterClass.Value.newBuilder().setNumeric(d.bigDecimal.toPlainString).build()
    )

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
    )

  private[this] val failingStatusGen = Gen const com.google.rpc.Status.getDefaultInstance

  private[this] val interfaceViewGen: Gen[EventOuterClass.InterfaceView] =
    Gen.zip(identifierGen, Gen.either(recordGen, failingStatusGen)).map { case (id, vs) =>
      val b = EventOuterClass.InterfaceView.newBuilder().setInterfaceId(id)
      vs.fold(b.setViewValue, b.setViewStatus).build()
    }

  val createdEventGen: Gen[EventOuterClass.CreatedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      createArgument <- recordGen
      interfaceViews <- Gen.listOf(interfaceViewGen)
      eventId <- Arbitrary.arbString.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      signatories <- Gen.listOf(Gen.asciiPrintableStr)
      observers <- Gen.listOf(Gen.asciiPrintableStr)
    } yield EventOuterClass.CreatedEvent
      .newBuilder()
      .setContractId(contractId)
      .setTemplateId(templateId)
      .setCreateArguments(createArgument)
      .addAllInterfaceViews(interfaceViews.asJava)
      .setEventId(eventId)
      .addAllWitnessParties(witnessParties.asJava)
      .addAllSignatories(signatories.asJava)
      .addAllObservers(observers.asJava)
      .build()

  val archivedEventGen: Gen[EventOuterClass.ArchivedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      eventId <- Arbitrary.arbString.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)

    } yield EventOuterClass.ArchivedEvent
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
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      exerciseResult <- valueGen
    } yield EventOuterClass.ExercisedEvent
      .newBuilder()
      .setContractId(contractId)
      .setTemplateId(templateId)
      .addAllActingParties(actingParties.asJava)
      .setChoice(choice)
      .setChoiceArgument(choiceArgument)
      .setConsuming(isConsuming)
      .setEventId(eventId)
      .addAllWitnessParties(witnessParties.asJava)
      .setExerciseResult(exerciseResult)
      .build()

  def transactionFilterGen: Gen[TransactionFilterOuterClass.TransactionFilter] =
    for {
      filtersByParty <- Gen.mapOf(partyWithFiltersGen)
    } yield TransactionFilterOuterClass.TransactionFilter
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
    } yield TransactionFilterOuterClass.Filters
      .newBuilder()
      .setInclusive(inclusive)
      .build()

  def inclusiveGen: Gen[TransactionFilterOuterClass.InclusiveFilters] =
    for {
      templateIds <- Gen.listOf(identifierGen)
      interfaceFilters <- Gen.listOf(interfaceFilterGen)
    } yield TransactionFilterOuterClass.InclusiveFilters
      .newBuilder()
      .addAllTemplateIds(templateIds.asJava)
      .addAllInterfaceFilters(interfaceFilters.asJava)
      .build()

  private[this] def interfaceFilterGen: Gen[TransactionFilterOuterClass.InterfaceFilter] =
    Gen.zip(identifierGen, arbitrary[Boolean]).map { case (interfaceId, includeInterfaceView) =>
      TransactionFilterOuterClass.InterfaceFilter
        .newBuilder()
        .setInterfaceId(interfaceId)
        .setIncludeInterfaceView(includeInterfaceView)
        .build()
    }

  def getActiveContractRequestGen: Gen[ActiveContractsServiceOuterClass.GetActiveContractsRequest] =
    for {
      ledgerId <- Arbitrary.arbString.arbitrary
      transactionFilter <- transactionFilterGen
      verbose <- Arbitrary.arbBool.arbitrary
    } yield ActiveContractsServiceOuterClass.GetActiveContractsRequest
      .newBuilder()
      .setLedgerId(ledgerId)
      .setFilter(transactionFilter)
      .setVerbose(verbose)
      .build()

  val createCommandGen: Gen[CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      record <- recordGen
    } yield CommandsOuterClass.Command
      .newBuilder()
      .setCreate(
        CommandsOuterClass.CreateCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setCreateArguments(record)
      )
      .build()

  val exerciseCommandGen: Gen[CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      choiceName <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield CommandsOuterClass.Command
      .newBuilder()
      .setExercise(
        CommandsOuterClass.ExerciseCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setChoice(choiceName)
          .setChoiceArgument(value)
      )
      .build()

  val createAndExerciseCommandGen: Gen[CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      record <- recordGen
      choiceName <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield CommandsOuterClass.Command
      .newBuilder()
      .setCreateAndExercise(
        CommandsOuterClass.CreateAndExerciseCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setCreateArguments(record)
          .setChoice(choiceName)
          .setChoiceArgument(value)
      )
      .build()

  val commandGen: Gen[CommandsOuterClass.Command] =
    Gen.oneOf(createCommandGen, exerciseCommandGen, createAndExerciseCommandGen)
}
