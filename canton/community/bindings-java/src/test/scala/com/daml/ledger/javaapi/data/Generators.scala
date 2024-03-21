// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.*
import com.daml.ledger.api.v2.TransactionFilterOuterClass
import com.google.protobuf.{ByteString, Empty}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

import java.time.{Duration, Instant, LocalDate}
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

object Generators {

  def valueGen: Gen[v2.ValueOuterClass.Value] =
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

  def recordGen: Gen[v2.ValueOuterClass.Record] =
    for {
      recordId <- Gen.option(identifierGen)
      fields <- Gen.sized(height =>
        for {
          size <- Gen.size.flatMap(maxSize => Gen.chooseNum(1, math.max(maxSize, 1)))
          newHeight = height / size
          withLabel <- Arbitrary.arbBool.arbitrary
          recordFields <- Gen.listOfN(size, Gen.resize(newHeight, recordFieldGen(withLabel)))
        } yield recordFields
      )
    } yield {
      val builder = v2.ValueOuterClass.Record.newBuilder()
      recordId.foreach(builder.setRecordId)
      builder.addAllFields(fields.asJava)
      builder.build()
    }

  def recordValueGen: Gen[v2.ValueOuterClass.Value] = recordGen.map(valueFromRecord)

  def valueFromRecord(
      record: v2.ValueOuterClass.Record
  ): com.daml.ledger.api.v2.ValueOuterClass.Value = {
    v2.ValueOuterClass.Value.newBuilder().setRecord(record).build()
  }

  def identifierGen: Gen[v2.ValueOuterClass.Identifier] =
    for {
      moduleName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      entityName <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
      packageId <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
    } yield v2.ValueOuterClass.Identifier
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

  def recordFieldGen(withLabel: Boolean): Gen[v2.ValueOuterClass.RecordField] = {
    if (withLabel) {
      for {
        label <- recordLabelGen
        value <- valueGen
      } yield v2.ValueOuterClass.RecordField.newBuilder().setLabel(label).setValue(value).build()
    } else {
      valueGen.flatMap(v2.ValueOuterClass.RecordField.newBuilder().setValue(_).build())
    }
  }

  def unitValueGen: Gen[v2.ValueOuterClass.Value] =
    Gen.const(v2.ValueOuterClass.Value.newBuilder().setUnit(Empty.newBuilder().build()).build())

  def variantGen: Gen[v2.ValueOuterClass.Variant] =
    for {
      variantId <- identifierGen
      constructor <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield v2.ValueOuterClass.Variant
      .newBuilder()
      .setVariantId(variantId)
      .setConstructor(constructor)
      .setValue(value)
      .build()

  def variantValueGen: Gen[v2.ValueOuterClass.Value] =
    variantGen.map(v2.ValueOuterClass.Value.newBuilder().setVariant(_).build())

  def optionalGen: Gen[v2.ValueOuterClass.Optional] =
    Gen
      .option(valueGen)
      .map(_.fold(v2.ValueOuterClass.Optional.getDefaultInstance) { v =>
        v2.ValueOuterClass.Optional.newBuilder().setValue(v).build()
      })

  def optionalValueGen: Gen[v2.ValueOuterClass.Value] =
    optionalGen.map(v2.ValueOuterClass.Value.newBuilder().setOptional(_).build())

  def contractIdValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(
      v2.ValueOuterClass.Value.newBuilder().setContractId(_).build()
    )

  def byteStringGen: Gen[ByteString] =
    Arbitrary.arbString.arbitrary.map(str => com.google.protobuf.ByteString.copyFromUtf8(str))

  def listGen: Gen[v2.ValueOuterClass.List] =
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
      .map(v2.ValueOuterClass.List.newBuilder().addAllElements(_).build())

  def listValueGen: Gen[v2.ValueOuterClass.Value] =
    listGen.map(v2.ValueOuterClass.Value.newBuilder().setList(_).build())

  def textMapGen: Gen[v2.ValueOuterClass.TextMap] =
    Gen
      .sized(height =>
        for {
          size <- Gen.size
            .flatMap(maxSize => if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          keys <- Gen.listOfN(size, Arbitrary.arbString.arbitrary)
          values <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
        } yield (keys zip values).map { case (k, v) =>
          v2.ValueOuterClass.TextMap.Entry.newBuilder().setKey(k).setValue(v).build()
        }
      )
      .map(x => v2.ValueOuterClass.TextMap.newBuilder().addAllEntries(x.asJava).build())

  def textMapValueGen: Gen[v2.ValueOuterClass.Value] =
    textMapGen.map(v2.ValueOuterClass.Value.newBuilder().setTextMap(_).build())

  def genMapGen: Gen[v2.ValueOuterClass.GenMap] =
    Gen
      .sized(height =>
        for {
          size <- Gen.size
            .flatMap(maxSize => if (maxSize >= 1) Gen.chooseNum(1, maxSize) else Gen.const(1))
          newHeight = height / size
          keys <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
          values <- Gen.listOfN(size, Gen.resize(newHeight, valueGen))
        } yield (keys zip values).map { case (k, v) =>
          v2.ValueOuterClass.GenMap.Entry.newBuilder().setKey(k).setValue(v).build()
        }
      )
      .map(x => v2.ValueOuterClass.GenMap.newBuilder().addAllEntries(x.asJava).build())

  def genMapValueGen: Gen[v2.ValueOuterClass.Value] =
    genMapGen.map(v2.ValueOuterClass.Value.newBuilder().setGenMap(_).build())

  def int64ValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbLong.arbitrary.map(v2.ValueOuterClass.Value.newBuilder().setInt64(_).build())

  def textValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(v2.ValueOuterClass.Value.newBuilder().setText(_).build())

  def timestampValueGen: Gen[v2.ValueOuterClass.Value] =
    instantGen.map(instant =>
      v2.ValueOuterClass.Value.newBuilder().setTimestamp(instant.toEpochMilli * 1000).build()
    )

  def instantGen: Gen[Instant] =
    Gen
      .chooseNum(
        Instant.parse("0001-01-01T00:00:00Z").toEpochMilli,
        Instant.parse("9999-12-31T23:59:59.999999Z").toEpochMilli,
      )
      .map(Instant.ofEpochMilli)

  def partyValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbString.arbitrary.map(v2.ValueOuterClass.Value.newBuilder().setParty(_).build())

  def boolValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbBool.arbitrary.map(v2.ValueOuterClass.Value.newBuilder().setBool(_).build())

  def dateValueGen: Gen[v2.ValueOuterClass.Value] =
    localDateGen.map(d => v2.ValueOuterClass.Value.newBuilder().setDate(d.toEpochDay.toInt).build())

  def localDateGen: Gen[LocalDate] =
    Gen
      .chooseNum(LocalDate.parse("0001-01-01").toEpochDay, LocalDate.parse("9999-12-31").toEpochDay)
      .map(LocalDate.ofEpochDay)

  def decimalValueGen: Gen[v2.ValueOuterClass.Value] =
    Arbitrary.arbBigDecimal.arbitrary.map(d =>
      v2.ValueOuterClass.Value.newBuilder().setNumeric(d.bigDecimal.toPlainString).build()
    )

  def eventGen: Gen[v2.EventOuterClass.Event] = {
    import v2.EventOuterClass.Event
    for {
      event <- Gen.oneOf(
        createdEventGen.map(e => (b: Event.Builder) => b.setCreated(e)),
        archivedEventGen.map(e => (b: Event.Builder) => b.setArchived(e)),
      )
    } yield v2.EventOuterClass.Event
      .newBuilder()
      .pipe(event)
      .build()
  }

  def treeEventGen: Gen[v2.TransactionOuterClass.TreeEvent] = {
    import v2.TransactionOuterClass.TreeEvent
    for {
      event <- Gen.oneOf(
        createdEventGen.map(e => (b: TreeEvent.Builder) => b.setCreated(e)),
        exercisedEventGen.map(e => (b: TreeEvent.Builder) => b.setExercised(e)),
      )
    } yield v2.TransactionOuterClass.TreeEvent
      .newBuilder()
      .pipe(event)
      .build()
  }

  private[this] val failingStatusGen = Gen const com.google.rpc.Status.getDefaultInstance

  private[this] val interfaceViewGen: Gen[v2.EventOuterClass.InterfaceView] =
    Gen.zip(identifierGen, Gen.either(recordGen, failingStatusGen)).map { case (id, vs) =>
      val b = v2.EventOuterClass.InterfaceView.newBuilder().setInterfaceId(id)
      vs.fold(b.setViewValue, b.setViewStatus).build()
    }

  val eventIdGen: Gen[String] = Arbitrary.arbString.arbitrary.suchThat(_.nonEmpty)
  val packageNameGen: Gen[String] = Arbitrary.arbString.arbitrary.suchThat(_.nonEmpty)

  val createdEventGen: Gen[v2.EventOuterClass.CreatedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      packageName <- packageNameGen
      createArgument <- recordGen
      createEventBlob <- byteStringGen
      interfaceViews <- Gen.listOf(interfaceViewGen)
      eventId <- eventIdGen
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      signatories <- Gen.listOf(Gen.asciiPrintableStr)
      observers <- Gen.listOf(Gen.asciiPrintableStr)
    } yield v2.EventOuterClass.CreatedEvent
      .newBuilder()
      .setContractId(contractId)
      .setTemplateId(templateId)
      .setPackageName(packageName)
      .setCreateArguments(createArgument)
      .setCreatedEventBlob(createEventBlob)
      .addAllInterfaceViews(interfaceViews.asJava)
      .setEventId(eventId)
      .addAllWitnessParties(witnessParties.asJava)
      .addAllSignatories(signatories.asJava)
      .addAllObservers(observers.asJava)
      .build()

  val archivedEventGen: Gen[v2.EventOuterClass.ArchivedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      eventId <- eventIdGen
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)

    } yield v2.EventOuterClass.ArchivedEvent
      .newBuilder()
      .setContractId(contractId)
      .setTemplateId(templateId)
      .setEventId(eventId)
      .addAllWitnessParties(witnessParties.asJava)
      .build()

  val exercisedEventGen: Gen[v2.EventOuterClass.ExercisedEvent] =
    for {
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      actingParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      eventId <- eventIdGen
      choice <- Arbitrary.arbString.arbitrary
      choiceArgument <- valueGen
      isConsuming <- Arbitrary.arbBool.arbitrary
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      exerciseResult <- valueGen
    } yield v2.EventOuterClass.ExercisedEvent
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

  def transactionFilterGen: Gen[v2.TransactionFilterOuterClass.TransactionFilter] =
    for {
      filtersByParty <- Gen.mapOf(partyWithFiltersGen)
    } yield v2.TransactionFilterOuterClass.TransactionFilter
      .newBuilder()
      .putAllFiltersByParty(filtersByParty.asJava)
      .build()

  def partyWithFiltersGen: Gen[(String, v2.TransactionFilterOuterClass.Filters)] =
    for {
      party <- Arbitrary.arbString.arbitrary
      filters <- filtersGen
    } yield (party, filters)

  def filtersGen: Gen[v2.TransactionFilterOuterClass.Filters] =
    for {
      inclusive <- inclusiveGen
    } yield v2.TransactionFilterOuterClass.Filters
      .newBuilder()
      .setInclusive(inclusive)
      .build()

  def inclusiveGen: Gen[v2.TransactionFilterOuterClass.InclusiveFilters] =
    for {
      templateIds <- Gen.listOf(identifierGen)
      interfaceFilters <- Gen.listOf(interfaceFilterGen)
    } yield v2.TransactionFilterOuterClass.InclusiveFilters
      .newBuilder()
      .addAllTemplateFilters(
        templateIds
          .map(templateId =>
            TransactionFilterOuterClass.TemplateFilter.newBuilder
              .setTemplateId(templateId)
              .build
          )
          .asJava
      )
      .addAllInterfaceFilters(interfaceFilters.asJava)
      .build()

  private[this] def interfaceFilterGen: Gen[v2.TransactionFilterOuterClass.InterfaceFilter] =
    Gen.zip(identifierGen, arbitrary[Boolean]).map { case (interfaceId, includeInterfaceView) =>
      v2.TransactionFilterOuterClass.InterfaceFilter
        .newBuilder()
        .setInterfaceId(interfaceId)
        .setIncludeInterfaceView(includeInterfaceView)
        .build()
    }

  def getActiveContractRequestGen: Gen[v2.StateServiceOuterClass.GetActiveContractsRequest] =
    for {
      transactionFilter <- transactionFilterGen
      verbose <- Arbitrary.arbBool.arbitrary
      activeAtOffset <- Arbitrary.arbString.arbitrary
    } yield v2.StateServiceOuterClass.GetActiveContractsRequest
      .newBuilder()
      .setFilter(transactionFilter)
      .setVerbose(verbose)
      .setActiveAtOffset(activeAtOffset)
      .build()

  def activeContractGen: Gen[v2.StateServiceOuterClass.ActiveContract] = {
    for {
      createdEvent <- createdEventGen
      domainId <- Arbitrary.arbString.arbitrary
      reassignmentCounter <- Arbitrary.arbLong.arbitrary
    } yield v2.StateServiceOuterClass.ActiveContract
      .newBuilder()
      .setCreatedEvent(createdEvent)
      .setDomainId(domainId)
      .setReassignmentCounter(reassignmentCounter)
      .build()
  }

  def unassignedEventGen: Gen[v2.ReassignmentOuterClass.UnassignedEvent] = {
    for {
      unassignId <- Arbitrary.arbString.arbitrary
      contractId <- contractIdValueGen.map(_.getContractId)
      templateId <- identifierGen
      source <- Arbitrary.arbString.arbitrary
      target <- Arbitrary.arbString.arbitrary
      submitter <- Arbitrary.arbString.arbitrary
      reassignmentCounter <- Arbitrary.arbLong.arbitrary
      assignmentExclusivity <- instantGen
      witnessParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
    } yield v2.ReassignmentOuterClass.UnassignedEvent
      .newBuilder()
      .setUnassignId(unassignId)
      .setContractId(contractId)
      .setTemplateId(templateId)
      .setSource(source)
      .setTarget(target)
      .setSubmitter(submitter)
      .setReassignmentCounter(reassignmentCounter)
      .setAssignmentExclusivity(Utils.instantToProto(assignmentExclusivity))
      .addAllWitnessParties(witnessParties.asJava)
      .build()
  }

  def assignedEventGen: Gen[v2.ReassignmentOuterClass.AssignedEvent] = {
    for {
      source <- Arbitrary.arbString.arbitrary
      target <- Arbitrary.arbString.arbitrary
      unassignId <- Arbitrary.arbString.arbitrary
      submitter <- Arbitrary.arbString.arbitrary
      reassignmentCounter <- Arbitrary.arbLong.arbitrary
      createdEvent <- createdEventGen
    } yield v2.ReassignmentOuterClass.AssignedEvent
      .newBuilder()
      .setSource(source)
      .setTarget(target)
      .setUnassignId(unassignId)
      .setSubmitter(submitter)
      .setReassignmentCounter(reassignmentCounter)
      .setCreatedEvent(createdEvent)
      .build()
  }

  def incompleteUnassignedGen: Gen[v2.StateServiceOuterClass.IncompleteUnassigned] = {
    for {
      createdEvent <- createdEventGen
      unassignedEvent <- unassignedEventGen
    } yield v2.StateServiceOuterClass.IncompleteUnassigned
      .newBuilder()
      .setCreatedEvent(createdEvent)
      .setUnassignedEvent(unassignedEvent)
      .build()
  }

  def incompleteAssignedGen: Gen[v2.StateServiceOuterClass.IncompleteAssigned] = {
    for {
      assignedEvent <- assignedEventGen
    } yield v2.StateServiceOuterClass.IncompleteAssigned
      .newBuilder()
      .setAssignedEvent(assignedEvent)
      .build()
  }

  def contractEntryBuilderGen: Gen[
    v2.StateServiceOuterClass.GetActiveContractsResponse.Builder => v2.StateServiceOuterClass.GetActiveContractsResponse.Builder
  ] =
    Gen.oneOf(
      activeContractGen.map(e =>
        (b: v2.StateServiceOuterClass.GetActiveContractsResponse.Builder) => b.setActiveContract(e)
      ),
      incompleteUnassignedGen.map(e =>
        (b: v2.StateServiceOuterClass.GetActiveContractsResponse.Builder) =>
          b.setIncompleteUnassigned(e)
      ),
      incompleteAssignedGen.map(e =>
        (b: v2.StateServiceOuterClass.GetActiveContractsResponse.Builder) =>
          b.setIncompleteAssigned(e)
      ),
      Gen.const((b: v2.StateServiceOuterClass.GetActiveContractsResponse.Builder) => b),
    )

  def getActiveContractResponseGen: Gen[v2.StateServiceOuterClass.GetActiveContractsResponse] = {
    for {
      offset <- Arbitrary.arbString.arbitrary
      workflowId <- Arbitrary.arbString.arbitrary
      entryGen <- contractEntryBuilderGen
    } yield v2.StateServiceOuterClass.GetActiveContractsResponse
      .newBuilder()
      .setOffset(offset)
      .setWorkflowId(workflowId)
      .pipe(entryGen)
      .build()
  }

  def getConnectedDomainsRequestGen: Gen[v2.StateServiceOuterClass.GetConnectedDomainsRequest] = {
    for {
      party <- Arbitrary.arbString.arbitrary
    } yield v2.StateServiceOuterClass.GetConnectedDomainsRequest
      .newBuilder()
      .setParty(party)
      .build()
  }

  def connectedDomainGen
      : Gen[v2.StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain] = {
    for {
      domainAlias <- Arbitrary.arbString.arbitrary
      domainId <- Arbitrary.arbString.arbitrary
      permission <- Gen.oneOf(
        v2.StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
        v2.StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
        v2.StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION,
      )
    } yield v2.StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain
      .newBuilder()
      .setDomainAlias(domainAlias)
      .setDomainId(domainId)
      .setPermission(permission)
      .build()
  }

  def getConnectedDomainsResponseGen: Gen[v2.StateServiceOuterClass.GetConnectedDomainsResponse] = {
    for {
      domains <- Gen.listOf(connectedDomainGen)
    } yield v2.StateServiceOuterClass.GetConnectedDomainsResponse
      .newBuilder()
      .addAllConnectedDomains(domains.asJava)
      .build()
  }

  def participantOffsetGen: Gen[v2.ParticipantOffsetOuterClass.ParticipantOffset] = {
    import v2.ParticipantOffsetOuterClass.{ParticipantOffset as OffsetProto}
    for {
      modifier <- Gen.oneOf(
        Arbitrary.arbString.arbitrary.map(absolute =>
          (b: OffsetProto.Builder) => b.setAbsolute(absolute)
        ),
        Gen.const((b: OffsetProto.Builder) =>
          b.setBoundary(OffsetProto.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN)
        ),
        Gen.const((b: OffsetProto.Builder) =>
          b.setBoundary(OffsetProto.ParticipantBoundary.PARTICIPANT_BOUNDARY_END)
        ),
      )
    } yield OffsetProto
      .newBuilder()
      .pipe(modifier)
      .build()
  }

  def getLedgerEndResponseGen: Gen[v2.StateServiceOuterClass.GetLedgerEndResponse] = {
    for {
      offset <- participantOffsetGen
    } yield v2.StateServiceOuterClass.GetLedgerEndResponse
      .newBuilder()
      .setOffset(offset)
      .build()
  }

  def getLatestPrunedOffsetsResponseGen
      : Gen[v2.StateServiceOuterClass.GetLatestPrunedOffsetsResponse] = {
    for {
      participantPruned <- participantOffsetGen
      allDivulgedPruned <- participantOffsetGen
    } yield v2.StateServiceOuterClass.GetLatestPrunedOffsetsResponse
      .newBuilder()
      .setParticipantPrunedUpToInclusive(participantPruned)
      .setAllDivulgedContractsPrunedUpToInclusive(allDivulgedPruned)
      .build()
  }

  def createdGen: Gen[v2.EventQueryServiceOuterClass.Created] = {
    for {
      createdEvent <- createdEventGen
      domainId <- Arbitrary.arbString.arbitrary
    } yield v2.EventQueryServiceOuterClass.Created
      .newBuilder()
      .setCreatedEvent(createdEvent)
      .setDomainId(domainId)
      .build()
  }

  def archivedGen: Gen[v2.EventQueryServiceOuterClass.Archived] = {
    for {
      archivedEvent <- archivedEventGen
      domainId <- Arbitrary.arbString.arbitrary
    } yield v2.EventQueryServiceOuterClass.Archived
      .newBuilder()
      .setArchivedEvent(archivedEvent)
      .setDomainId(domainId)
      .build()
  }

  def getEventsByContractIdResponseGen
      : Gen[v2.EventQueryServiceOuterClass.GetEventsByContractIdResponse] = {
    import v2.EventQueryServiceOuterClass.{GetEventsByContractIdResponse as Response}
    for {
      optCreated <- Gen.option(createdGen)
      optArchived <- Gen.option(archivedGen)
    } yield Response
      .newBuilder()
      .pipe(builder => optCreated.fold(builder)(c => builder.setCreated(c)))
      .pipe(builder => optArchived.fold(builder)(a => builder.setArchived(a)))
      .build()
  }

  def completionStreamRequestGen
      : Gen[v2.CommandCompletionServiceOuterClass.CompletionStreamRequest] = {
    import v2.CommandCompletionServiceOuterClass.{CompletionStreamRequest as Request}
    for {
      applicationId <- Arbitrary.arbString.arbitrary
      parties <- Gen.listOf(Arbitrary.arbString.arbitrary)
      beginExclusive <- participantOffsetGen
    } yield Request
      .newBuilder()
      .setApplicationId(applicationId)
      .addAllParties(parties.asJava)
      .setBeginExclusive(beginExclusive)
      .build()
  }

  def completionGen: Gen[v2.CompletionOuterClass.Completion] = {
    import v2.CompletionOuterClass.Completion
    for {
      commandId <- Arbitrary.arbString.arbitrary
      status <- Gen.const(com.google.rpc.Status.getDefaultInstance)
      updateId <- Arbitrary.arbString.arbitrary
      applicationId <- Arbitrary.arbString.arbitrary
      actAs <- Gen.listOf(Arbitrary.arbString.arbitrary)
      submissionId <- Arbitrary.arbString.arbitrary
      deduplication <- Gen.oneOf(
        Arbitrary.arbString.arbitrary.map(offset =>
          (b: Completion.Builder) => b.setDeduplicationOffset(offset)
        ),
        Arbitrary.arbLong.arbitrary.map(seconds =>
          (b: Completion.Builder) =>
            b.setDeduplicationDuration(Utils.durationToProto(Duration.ofSeconds(seconds)))
        ),
      )
      traceContext <- Gen.const(Utils.newProtoTraceContext("parent", "state"))
    } yield Completion
      .newBuilder()
      .setCommandId(commandId)
      .setStatus(status)
      .setUpdateId(updateId)
      .setApplicationId(applicationId)
      .addAllActAs(actAs.asJava)
      .setSubmissionId(submissionId)
      .pipe(deduplication)
      .setTraceContext(traceContext)
      .build()
  }

  def checkpointGen: Gen[v2.CheckpointOuterClass.Checkpoint] = {
    import v2.CheckpointOuterClass.Checkpoint
    for {
      recordTime <- instantGen
      offset <- participantOffsetGen
    } yield Checkpoint
      .newBuilder()
      .setRecordTime(Utils.instantToProto(recordTime))
      .setOffset(offset)
      .build()
  }

  def completionStreamResponseGen
      : Gen[v2.CommandCompletionServiceOuterClass.CompletionStreamResponse] = {
    import v2.CommandCompletionServiceOuterClass.{CompletionStreamResponse as Response}
    for {
      checkpoint <- checkpointGen
      completion <- completionGen
      domainId <- Arbitrary.arbString.arbitrary
    } yield Response
      .newBuilder()
      .setCheckpoint(checkpoint)
      .setCompletion(completion)
      .setDomainId(domainId)
      .build()
  }

  def transactionGen: Gen[v2.TransactionOuterClass.Transaction] = {
    import v2.TransactionOuterClass.Transaction
    for {
      updateId <- Arbitrary.arbString.arbitrary
      commandId <- Arbitrary.arbString.arbitrary
      workflowId <- Arbitrary.arbString.arbitrary
      effectiveAt <- instantGen
      events <- Gen.listOf(eventGen)
      offset <- Arbitrary.arbString.arbitrary
      domainId <- Arbitrary.arbString.arbitrary
      traceContext <- Gen.const(Utils.newProtoTraceContext("parent", "state"))
      recordTime <- instantGen
    } yield Transaction
      .newBuilder()
      .setUpdateId(updateId)
      .setCommandId(commandId)
      .setWorkflowId(workflowId)
      .setEffectiveAt(Utils.instantToProto(effectiveAt))
      .addAllEvents(events.asJava)
      .setOffset(offset)
      .setDomainId(domainId)
      .setTraceContext(traceContext)
      .setRecordTime(Utils.instantToProto(recordTime))
      .build()
  }

  def transactionTreeGen: Gen[v2.TransactionOuterClass.TransactionTree] = {
    import v2.TransactionOuterClass.TransactionTree
    def idTreeEventPairGen =
      eventIdGen.flatMap(id => treeEventGen.map(e => id -> e))
    for {
      updateId <- Arbitrary.arbString.arbitrary
      commandId <- Arbitrary.arbString.arbitrary
      workflowId <- Arbitrary.arbString.arbitrary
      effectiveAt <- instantGen
      eventsById <- Gen.mapOfN(10, idTreeEventPairGen)
      rootEventIds = eventsById.headOption.map(_._1).toList
      offset <- Arbitrary.arbString.arbitrary
      domainId <- Arbitrary.arbString.arbitrary
      traceContext <- Gen.const(Utils.newProtoTraceContext("parent", "state"))
      recordTime <- instantGen
    } yield TransactionTree
      .newBuilder()
      .setUpdateId(updateId)
      .setCommandId(commandId)
      .setWorkflowId(workflowId)
      .setEffectiveAt(Utils.instantToProto(effectiveAt))
      .putAllEventsById(eventsById.asJava)
      .addAllRootEventIds(rootEventIds.asJava)
      .setOffset(offset)
      .setDomainId(domainId)
      .setTraceContext(traceContext)
      .setRecordTime(Utils.instantToProto(recordTime))
      .build()
  }

  def reassignmentGen: Gen[v2.ReassignmentOuterClass.Reassignment] = {
    import v2.ReassignmentOuterClass.Reassignment
    for {
      updateId <- Arbitrary.arbString.arbitrary
      commandId <- Arbitrary.arbString.arbitrary
      workflowId <- Arbitrary.arbString.arbitrary
      offset <- Arbitrary.arbString.arbitrary
      event <- Gen.oneOf(
        unassignedEventGen.map(unassigned =>
          (b: Reassignment.Builder) => b.setUnassignedEvent(unassigned)
        ),
        assignedEventGen.map(assigned => (b: Reassignment.Builder) => b.setAssignedEvent(assigned)),
      )
      traceContext <- Gen.const(Utils.newProtoTraceContext("parent", "state"))
      recordTime <- instantGen
    } yield Reassignment
      .newBuilder()
      .setUpdateId(updateId)
      .setCommandId(commandId)
      .setWorkflowId(workflowId)
      .setOffset(offset)
      .pipe(event)
      .setTraceContext(traceContext)
      .setRecordTime(Utils.instantToProto(recordTime))
      .build()
  }

  def getTransactionByEventIdRequestGen
      : Gen[v2.UpdateServiceOuterClass.GetTransactionByEventIdRequest] = {
    import v2.UpdateServiceOuterClass.{GetTransactionByEventIdRequest as Request}
    for {
      eventId <- eventIdGen
      requestingParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
    } yield Request
      .newBuilder()
      .setEventId(eventId)
      .addAllRequestingParties(requestingParties.asJava)
      .build()
  }

  def getTransactionByIdRequestGen: Gen[v2.UpdateServiceOuterClass.GetTransactionByIdRequest] = {
    import v2.UpdateServiceOuterClass.{GetTransactionByIdRequest as Request}
    for {
      updateId <- Arbitrary.arbString.arbitrary
      requestingParties <- Gen.listOf(Arbitrary.arbString.arbitrary)
    } yield Request
      .newBuilder()
      .setUpdateId(updateId)
      .addAllRequestingParties(requestingParties.asJava)
      .build()
  }

  def getTransactionResponseGen: Gen[v2.UpdateServiceOuterClass.GetTransactionResponse] =
    transactionGen.map(
      v2.UpdateServiceOuterClass.GetTransactionResponse
        .newBuilder()
        .setTransaction(_)
        .build()
    )

  def getTransactionTreeResponseGen: Gen[v2.UpdateServiceOuterClass.GetTransactionTreeResponse] =
    transactionTreeGen.map(
      v2.UpdateServiceOuterClass.GetTransactionTreeResponse
        .newBuilder()
        .setTransaction(_)
        .build()
    )

  def getUpdatesRequestGen: Gen[v2.UpdateServiceOuterClass.GetUpdatesRequest] = {
    import v2.UpdateServiceOuterClass.{GetUpdatesRequest as Request}
    for {
      beginExclusive <- participantOffsetGen
      endInclusive <- participantOffsetGen
      filter <- transactionFilterGen
      verbose <- Arbitrary.arbBool.arbitrary
    } yield Request
      .newBuilder()
      .setBeginExclusive(beginExclusive)
      .setEndInclusive(endInclusive)
      .setFilter(filter)
      .setVerbose(verbose)
      .build()
  }

  def getUpdatesResponseGen: Gen[v2.UpdateServiceOuterClass.GetUpdatesResponse] = {
    import v2.UpdateServiceOuterClass.{GetUpdatesResponse as Response}
    for {
      update <- Gen.oneOf(
        transactionGen.map(transaction => (b: Response.Builder) => b.setTransaction(transaction)),
        reassignmentGen.map(reassingment =>
          (b: Response.Builder) => b.setReassignment(reassingment)
        ),
      )
    } yield Response
      .newBuilder()
      .pipe(update)
      .build()
  }

  def getUpdateTreesResponseGen: Gen[v2.UpdateServiceOuterClass.GetUpdateTreesResponse] = {
    import v2.UpdateServiceOuterClass.{GetUpdateTreesResponse as Response}
    for {
      update <- Gen.oneOf(
        transactionTreeGen.map(transactionTree =>
          (b: Response.Builder) => b.setTransactionTree(transactionTree)
        ),
        reassignmentGen.map(reassingment =>
          (b: Response.Builder) => b.setReassignment(reassingment)
        ),
      )
    } yield Response
      .newBuilder()
      .pipe(update)
      .build()
  }

  val createCommandGen: Gen[v2.CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      record <- recordGen
    } yield v2.CommandsOuterClass.Command
      .newBuilder()
      .setCreate(
        v2.CommandsOuterClass.CreateCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setCreateArguments(record)
      )
      .build()

  val exerciseCommandGen: Gen[v2.CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      choiceName <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield v2.CommandsOuterClass.Command
      .newBuilder()
      .setExercise(
        v2.CommandsOuterClass.ExerciseCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setChoice(choiceName)
          .setChoiceArgument(value)
      )
      .build()

  val createAndExerciseCommandGen: Gen[v2.CommandsOuterClass.Command] =
    for {
      templateId <- identifierGen
      record <- recordGen
      choiceName <- Arbitrary.arbString.arbitrary
      value <- valueGen
    } yield v2.CommandsOuterClass.Command
      .newBuilder()
      .setCreateAndExercise(
        v2.CommandsOuterClass.CreateAndExerciseCommand
          .newBuilder()
          .setTemplateId(templateId)
          .setCreateArguments(record)
          .setChoice(choiceName)
          .setChoiceArgument(value)
      )
      .build()

  val commandGen: Gen[v2.CommandsOuterClass.Command] =
    Gen.oneOf(createCommandGen, exerciseCommandGen, createAndExerciseCommandGen)

  val bytesGen: Gen[ByteString] =
    Gen
      .nonEmptyListOf(Arbitrary.arbByte.arbitrary)
      .map(x => ByteString.copyFrom(x.toArray))

  val disclosedContractGen: Gen[v2.CommandsOuterClass.DisclosedContract] = {
    import v2.CommandsOuterClass.DisclosedContract
    for {
      templateId <- identifierGen
      contractId <- Arbitrary.arbString.arbitrary
      createdEventBlob <- bytesGen
    } yield DisclosedContract
      .newBuilder()
      .setTemplateId(templateId)
      .setContractId(contractId)
      .setCreatedEventBlob(createdEventBlob)
      .build()
  }

  val commandsGen: Gen[v2.CommandsOuterClass.Commands] = {
    import v2.CommandsOuterClass.Commands
    for {
      workflowId <- Arbitrary.arbString.arbitrary
      applicationId <- Arbitrary.arbString.arbitrary
      commandId <- Arbitrary.arbString.arbitrary
      commands <- Gen.listOf(commandGen)
      deduplication <- Gen.oneOf(
        Arbitrary.arbLong.arbitrary.map(duration =>
          (b: Commands.Builder) =>
            b.setDeduplicationDuration(Utils.durationToProto(Duration.ofSeconds(duration)))
        ),
        Arbitrary.arbString.arbitrary.map(offset =>
          (b: Commands.Builder) => b.setDeduplicationOffset(offset)
        ),
      )
      minLedgerTimeAbs <- Arbitrary.arbInstant.arbitrary.map(Utils.instantToProto)
      minLedgerTimeRel <- Arbitrary.arbLong.arbitrary.map(t =>
        Utils.durationToProto(Duration.ofSeconds(t))
      )
      actAs <- Gen.nonEmptyListOf(Arbitrary.arbString.arbitrary)
      readAs <- Gen.listOf(Arbitrary.arbString.arbitrary)
      submissionId <- Arbitrary.arbString.arbitrary
      disclosedContract <- disclosedContractGen
      domainId <- Arbitrary.arbString.arbitrary
    } yield Commands
      .newBuilder()
      .setWorkflowId(workflowId)
      .setApplicationId(applicationId)
      .setCommandId(commandId)
      .addAllCommands(commands.asJava)
      .pipe(deduplication)
      .setMinLedgerTimeAbs(minLedgerTimeAbs)
      .setMinLedgerTimeRel(minLedgerTimeRel)
      .addAllActAs(actAs.asJava)
      .addAllReadAs(readAs.asJava)
      .setSubmissionId(submissionId)
      .addDisclosedContracts(disclosedContract)
      .setDomainId(domainId)
      .build()
  }

  val unassignCommandGen: Gen[v2.ReassignmentCommandOuterClass.UnassignCommand] = {
    import v2.ReassignmentCommandOuterClass.UnassignCommand
    for {
      contractId <- Arbitrary.arbString.arbitrary
      source <- Arbitrary.arbString.arbitrary
      target <- Arbitrary.arbString.arbitrary
    } yield UnassignCommand
      .newBuilder()
      .setContractId(contractId)
      .setSource(source)
      .setTarget(target)
      .build()
  }

  val assignCommandGen: Gen[v2.ReassignmentCommandOuterClass.AssignCommand] = {
    import v2.ReassignmentCommandOuterClass.AssignCommand
    for {
      unassignId <- Arbitrary.arbString.arbitrary
      source <- Arbitrary.arbString.arbitrary
      target <- Arbitrary.arbString.arbitrary
    } yield AssignCommand
      .newBuilder()
      .setUnassignId(unassignId)
      .setSource(source)
      .setTarget(target)
      .build()
  }

  val reassignmentCommandGen: Gen[v2.ReassignmentCommandOuterClass.ReassignmentCommand] = {
    import v2.ReassignmentCommandOuterClass.ReassignmentCommand
    for {
      workflowId <- Arbitrary.arbString.arbitrary
      applicationId <- Arbitrary.arbString.arbitrary
      commandId <- Arbitrary.arbString.arbitrary
      submitter <- Arbitrary.arbString.arbitrary
      command <- Gen.oneOf(
        unassignCommandGen.map(unassign =>
          (b: ReassignmentCommand.Builder) => b.setUnassignCommand(unassign)
        ),
        assignCommandGen.map(assign =>
          (b: ReassignmentCommand.Builder) => b.setAssignCommand(assign)
        ),
      )
      submissionId <- Arbitrary.arbString.arbitrary
    } yield ReassignmentCommand
      .newBuilder()
      .setWorkflowId(workflowId)
      .setApplicationId(applicationId)
      .setCommandId(commandId)
      .setSubmitter(submitter)
      .pipe(command)
      .setSubmissionId(submissionId)
      .build()
  }

  def submitAndWaitForUpdateIdResponseGen
      : Gen[v2.CommandServiceOuterClass.SubmitAndWaitForUpdateIdResponse] = {
    import v2.CommandServiceOuterClass.{SubmitAndWaitForUpdateIdResponse as Response}
    for {
      updateId <- Arbitrary.arbString.arbitrary
      completionOffset <- Arbitrary.arbString.arbitrary
    } yield Response
      .newBuilder()
      .setUpdateId(updateId)
      .setCompletionOffset(completionOffset)
      .build()
  }
  def submitAndWaitForTransactionResponseGen
      : Gen[v2.CommandServiceOuterClass.SubmitAndWaitForTransactionResponse] = {
    import v2.CommandServiceOuterClass.{SubmitAndWaitForTransactionResponse as Response}
    for {
      transaction <- transactionGen
      completionOffset <- Arbitrary.arbString.arbitrary
    } yield Response
      .newBuilder()
      .setTransaction(transaction)
      .setCompletionOffset(completionOffset)
      .build()
  }
  def submitAndWaitForTransactionTreeResponseGen
      : Gen[v2.CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse] = {
    import v2.CommandServiceOuterClass.{SubmitAndWaitForTransactionTreeResponse as Response}
    for {
      transaction <- transactionTreeGen
      completionOffset <- Arbitrary.arbString.arbitrary
    } yield Response
      .newBuilder()
      .setTransaction(transaction)
      .setCompletionOffset(completionOffset)
      .build()
  }
}
