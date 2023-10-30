// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.api.v1.trace_context.TraceContext as ProtoTraceContext
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.NamedLogging.loggerWithoutTracing
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  hashFromHexString,
  offset,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.Raw
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Identifier, Party}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}

import java.sql.Connection
import scala.collection.immutable.ArraySeq

object EventStorageBackendTemplate {
  import com.digitalasset.canton.platform.store.backend.Conversions.ArrayColumnToIntArray.*
  import com.digitalasset.canton.platform.store.backend.Conversions.ArrayColumnToStringArray.*

  private val baseColumnsForFlatTransactionsCreate =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "event_id",
      "contract_id",
      "template_id",
      "create_argument",
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
      "create_key_hash",
      "create_key_value_compression",
      "create_key_maintainers",
      "submitters",
      "driver_metadata",
      "domain_id",
      "trace_context",
    )

  private val baseColumnsForFlatTransactionsExercise =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "event_id",
      "contract_id",
      "template_id",
      "NULL as create_argument",
      "NULL as create_argument_compression",
      "NULL as create_signatories",
      "NULL as create_observers",
      "NULL as create_agreement_text",
      "create_key_value",
      "NULL as create_key_hash",
      "create_key_value_compression",
      "NULL as create_key_maintainers",
      "submitters",
      "NULL as driver_metadata",
      "domain_id",
      "trace_context",
    )

  val selectColumnsForFlatTransactionsCreate: String =
    baseColumnsForFlatTransactionsCreate.mkString(", ")

  val selectColumnsForFlatTransactionsExercise: String =
    baseColumnsForFlatTransactionsExercise.mkString(", ")

  private val selectColumnsForACSEvents =
    baseColumnsForFlatTransactionsCreate.map(c => s"create_evs.$c").mkString(", ")

  private type SharedRow =
    Offset ~ String ~ Int ~ Long ~ String ~ String ~ Timestamp ~ Int ~ Option[String] ~
      Option[String] ~ Array[Int] ~ Option[Array[Int]] ~ Option[Int] ~ Option[Array[Byte]]

  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("event_id") ~
      str("contract_id") ~
      timestampFromMicros("ledger_effective_time") ~
      int("template_id") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[Int]("event_witnesses") ~
      array[Int]("submitters").? ~
      int("domain_id").? ~
      byteArray("trace_context").?

  private type CreatedEventRow =
    SharedRow ~ Array[Byte] ~ Option[Int] ~ Array[Int] ~ Array[Int] ~ Option[String] ~
      Option[Array[Byte]] ~ Option[Hash] ~ Option[Int] ~ Option[Array[Int]] ~ Option[Array[Byte]]

  private val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_key_value").? ~
      hashFromHexString("create_key_hash").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      byteArray("driver_metadata").?

  private type ExercisedEventRow =
    SharedRow ~ Boolean ~ String ~ Array[Byte] ~ Option[Int] ~ Option[Array[Byte]] ~ Option[Int] ~
      Array[Int] ~ Array[String]

  private val exercisedEventRow: RowParser[ExercisedEventRow] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    sharedRow ~
      bool("exercise_consuming") ~
      str("exercise_choice") ~
      byteArray("exercise_argument") ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[Int]("exercise_actors") ~
      array[String]("exercise_child_event_ids")
  }

  private type ArchiveEventRow = SharedRow

  private val archivedEventRow: RowParser[ArchiveEventRow] = sharedRow

  private def extractTraceContext(
      tcBytes: Option[Array[Byte]],
      logger: TracedLogger,
  ): TraceContext =
    SerializableTraceContext
      .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(tcBytes.map(ProtoTraceContext.parseFrom))
      .traceContext

  private[common] def createdFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ internedDomainId ~ traceContext ~ createArgument ~ createArgumentCompression ~
          createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyHash ~ createKeyValueCompression ~ createKeyMaintainers ~ driverMetadata =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(
              createSignatories.map(stringInterning.party.unsafe.externalize)
            ),
            createObservers = ArraySeq.unsafeWrapArray(
              createObservers.map(stringInterning.party.unsafe.externalize)
            ),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyHash = createKeyHash,
            createKeyValueCompression = createKeyValueCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.map(stringInterning.party.unsafe.externalize))
              .getOrElse(Array.empty),
            ledgerEffectiveTime = ledgerEffectiveTime,
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
            driverMetadata = driverMetadata,
          ),
          domainId = internedDomainId.map(stringInterning.domainId.unsafe.externalize),
          traceContext = traceContext,
        )
    }

  private[common] def archivedFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent.Archived]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ internedDomainId ~ traceContext =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Archived(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
          ),
          domainId = internedDomainId.map(stringInterning.domainId.unsafe.externalize),
          traceContext = traceContext,
        )
    }

  def rawFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent]] =
    createdFlatEventParser(allQueryingParties, stringInterning) | archivedFlatEventParser(
      allQueryingParties,
      stringInterning,
    )

  private def createdTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ internedDomainId ~ traceContext ~
          createArgument ~ createArgumentCompression ~ createSignatories ~ createObservers ~ createAgreementText ~
          createKeyValue ~ createKeyHash ~ createKeyValueCompression ~ createKeyMaintainers ~ driverMetadata =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(
              createSignatories.map(stringInterning.party.unsafe.externalize)
            ),
            createObservers = ArraySeq.unsafeWrapArray(
              createObservers.map(stringInterning.party.unsafe.externalize)
            ),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyHash = createKeyHash,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyValueCompression = createKeyValueCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.map(stringInterning.party.unsafe.externalize))
              .getOrElse(Array.empty),
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
            driverMetadata = driverMetadata,
          ),
          domainId = internedDomainId.map(stringInterning.domainId.unsafe.externalize),
          traceContext = traceContext,
        )
    }

  private def exercisedTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent.Exercised]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ internedDomainId ~ traceContext ~ exerciseConsuming ~ qualifiedChoiceName ~ exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~ exerciseChildEventIds =>
        val Ref.QualifiedChoiceName(interfaceId, choiceName) =
          Ref.QualifiedChoiceName.assertFromString(qualifiedChoiceName)
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId.nonEmpty && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Exercised(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            interfaceId = interfaceId,
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = choiceName,
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors = ArraySeq.unsafeWrapArray(
              exerciseActors.map(stringInterning.party.unsafe.externalize)
            ),
            exerciseChildEventIds = ArraySeq.unsafeWrapArray(exerciseChildEventIds),
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
          ),
          domainId = internedDomainId.map(stringInterning.domainId.unsafe.externalize),
          traceContext = traceContext,
        )
    }

  def rawTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent]] =
    createdTreeEventParser(allQueryingParties, stringInterning) | exercisedTreeEventParser(
      allQueryingParties,
      stringInterning,
    )

  val selectColumnsForTransactionTreeCreate: String = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "create_argument",
    "create_argument_compression",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "create_key_hash",
    "create_key_value_compression",
    "create_key_maintainers",
    "NULL as exercise_choice",
    "NULL as exercise_argument",
    "NULL as exercise_argument_compression",
    "NULL as exercise_result",
    "NULL as exercise_result_compression",
    "NULL as exercise_actors",
    "NULL as exercise_child_event_ids",
    "submitters",
    "driver_metadata",
    "domain_id",
    "trace_context",
  ).mkString(", ")

  val selectColumnsForTransactionTreeExercise: String = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "NULL as create_argument",
    "NULL as create_argument_compression",
    "NULL as create_signatories",
    "NULL as create_observers",
    "NULL as create_agreement_text",
    "create_key_value",
    "NULL as create_key_hash",
    "create_key_value_compression",
    "NULL as create_key_maintainers",
    "exercise_choice",
    "exercise_argument",
    "exercise_argument_compression",
    "exercise_result",
    "exercise_result_compression",
    "exercise_actors",
    "exercise_child_event_ids",
    "submitters",
    "NULL as driver_metadata",
    "domain_id",
    "trace_context",
  ).mkString(", ")

  val EventSequentialIdFirstLast: RowParser[(Long, Long)] =
    long("event_sequential_id_first") ~ long("event_sequential_id_last") map {
      case event_sequential_id_first ~ event_sequential_id_last =>
        (event_sequential_id_first, event_sequential_id_last)
    }

  val assignEventRow =
    str("command_id").? ~
      str("workflow_id").? ~
      str("event_offset") ~
      int("source_domain_id") ~
      int("target_domain_id") ~
      str("unassign_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      str("update_id") ~
      str("contract_id") ~
      int("template_id") ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("driver_metadata") ~
      byteArray("trace_context").?

  private def assignEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.RawAssignEvent] =
    assignEventRow map {
      case commandId ~
          workflowId ~
          offset ~
          sourceDomainId ~
          targetDomainId ~
          unassignId ~
          submitter ~
          reassignmentCounter ~
          updateId ~
          contractId ~
          templateId ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createAgreementText ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          traceContext =>
        EventStorageBackend.RawAssignEvent(
          commandId = commandId,
          workflowId = workflowId,
          offset = offset,
          sourceDomainId = stringInterning.domainId.unsafe.externalize(sourceDomainId),
          targetDomainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          unassignId = unassignId,
          submitter = submitter.map(stringInterning.party.unsafe.externalize),
          reassignmentCounter = reassignmentCounter,
          rawCreatedEvent = EventStorageBackend.RawCreatedEvent(
            updateId = updateId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            witnessParties = flatEventWitnesses.view
              .filter(allQueryingParties)
              .map(stringInterning.party.unsafe.externalize)
              .toSet,
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
            agreementText = createAgreementText,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.view.map(stringInterning.party.unsafe.externalize).toSet)
              .getOrElse(Set.empty),
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            driverMetadata = driverMetadata,
          ),
          traceContext = traceContext,
        )
    }

  val unassignEventRow =
    str("command_id").? ~
      str("workflow_id").? ~
      str("event_offset") ~
      int("source_domain_id") ~
      int("target_domain_id") ~
      str("unassign_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      str("update_id") ~
      str("contract_id") ~
      int("template_id") ~
      array[Int]("flat_event_witnesses") ~
      timestampFromMicros("assignment_exclusivity").? ~
      byteArray("trace_context").?

  private def unassignEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.RawUnassignEvent] =
    unassignEventRow map {
      case commandId ~
          workflowId ~
          offset ~
          sourceDomainId ~
          targetDomainId ~
          unassignId ~
          submitter ~
          reassignmentCounter ~
          updateId ~
          contractId ~
          templateId ~
          flatEventWitnesses ~
          assignmentExclusivity ~
          traceContext =>
        EventStorageBackend.RawUnassignEvent(
          commandId = commandId,
          workflowId = workflowId,
          offset = offset,
          sourceDomainId = stringInterning.domainId.unsafe.externalize(sourceDomainId),
          targetDomainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          unassignId = unassignId,
          submitter = submitter.map(stringInterning.party.unsafe.externalize),
          reassignmentCounter = reassignmentCounter,
          updateId = updateId,
          contractId = contractId,
          templateId = stringInterning.templateId.externalize(templateId),
          witnessParties = flatEventWitnesses.view
            .filter(allQueryingParties)
            .map(stringInterning.party.unsafe.externalize)
            .toSet,
          assignmentExclusivity = assignmentExclusivity,
          traceContext = traceContext,
        )
    }

  val assignActiveContractRow =
    str("workflow_id").? ~
      int("target_domain_id") ~
      long("reassignment_counter") ~
      str("update_id") ~
      str("contract_id") ~
      int("template_id") ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("driver_metadata") ~
      long("event_sequential_id")

  private def assignActiveContractParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.RawActiveContract] =
    assignActiveContractRow map {
      case workflowId ~
          targetDomainId ~
          reassignmentCounter ~
          updateId ~
          contractId ~
          templateId ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createAgreementText ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          eventSequentialId =>
        EventStorageBackend.RawActiveContract(
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          reassignmentCounter = reassignmentCounter,
          rawCreatedEvent = EventStorageBackend.RawCreatedEvent(
            updateId = updateId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            witnessParties = flatEventWitnesses.view
              .filter(allQueryingParties)
              .map(stringInterning.party.unsafe.externalize)
              .toSet,
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
            agreementText = createAgreementText,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.map(stringInterning.party.unsafe.externalize).toSet)
              .getOrElse(Set.empty),
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            driverMetadata = driverMetadata,
          ),
          eventSequentialId = eventSequentialId,
        )
    }

  val createActiveContractRow =
    str("workflow_id").? ~
      int("domain_id") ~
      str("transaction_id") ~
      str("contract_id") ~
      int("template_id") ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("driver_metadata").? ~
      long("event_sequential_id")

  private def createActiveContractParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.RawActiveContract] =
    createActiveContractRow map {
      case workflowId ~
          targetDomainId ~
          transactionId ~
          contractId ~
          templateId ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createAgreementText ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          eventSequentialId =>
        EventStorageBackend.RawActiveContract(
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          reassignmentCounter = 0L, // zero for create
          rawCreatedEvent = EventStorageBackend.RawCreatedEvent(
            updateId = transactionId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            witnessParties = flatEventWitnesses.view
              .filter(allQueryingParties)
              .map(stringInterning.party.unsafe.externalize)
              .toSet,
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
            agreementText = createAgreementText,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.map(stringInterning.party.unsafe.externalize).toSet)
              .getOrElse(Set.empty),
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            driverMetadata = driverMetadata.getOrElse(Array.empty),
          ),
          eventSequentialId = eventSequentialId,
        )
    }
}

abstract class EventStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    // This method is needed in pruneEvents, but belongs to [[ParameterStorageBackend]].
    participantAllDivulgedContractsPrunedUpToInclusive: Connection => Option[Offset],
    val loggerFactory: NamedLoggerFactory,
) extends EventStorageBackend
    with NamedLogging {
  import EventStorageBackendTemplate.*

  override def transactionPointwiseQueries: TransactionPointwiseQueries =
    new TransactionPointwiseQueries(
      queryStrategy = queryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
    )

  override def transactionStreamingQueries: TransactionStreamingQueries =
    new TransactionStreamingQueries(
      queryStrategy = queryStrategy,
      stringInterning = stringInterning,
    )

  override def eventReaderQueries: EventReaderQueries =
    new EventReaderQueries(
      queryStrategy = queryStrategy,
      stringInterning = stringInterning,
    )

  override def activeContractCreateEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
      SELECT
        #$selectColumnsForACSEvents,
        flat_event_witnesses as event_witnesses,
        '' AS command_id
      FROM
        participant_events_create create_evs
      WHERE
        create_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        AND NOT EXISTS (  -- check not archived as of snapshot
          SELECT 1
          FROM participant_events_consuming_exercise consuming_evs
          WHERE
            create_evs.contract_id = consuming_evs.contract_id
            AND consuming_evs.event_sequential_id <= $endInclusive
        )
      ORDER BY
        create_evs.event_sequential_id -- deliver in index order
      """
      .asVectorOf(rawFlatEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  // Improvement idea: Implement pruning queries in terms of event sequential id in order to be able to drop offset based indices.
  /** Deletes a subset of the indexed data (up to the pruning offset) in the following order and in the manner specified:
    * 1.a if pruning-all-divulged-contracts is enabled: all divulgence events (retroactive divulgence),
    * 1.b otherwise: divulgence events for which there are archive events (retroactive divulgence),
    * 2. entries from filter for create stakeholders for there is an archive for the corresponding create event,
    * 3. entries from filter for create non-stakeholder informees for there is an archive for the corresponding create event,
    * 4. all entries from filter for consuming stakeholders,
    * 5. all entries from filter for consuming non-stakeholders informees,
    * 6. all entries from filter for non-consuming informees,
    * 7. create events table for which there is an archive event,
    * 8. if pruning-all-divulged-contracts is enabled: create contracts which did not have a locally hosted party before their creation offset (immediate divulgence),
    * 9. all consuming events,
    * 10. all non-consuming events,
    * 11. transaction meta entries for which there exists at least one create event.
    */
  override def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit connection: Connection, traceContext: TraceContext): Unit = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

    if (pruneAllDivulgedContracts) {
      pruneWithLogging(queryDescription = "All retroactive divulgence events pruning") {
        // Note: do not use `QueryStrategy.offsetIsSmallerOrEqual` because divulgence events have a nullable offset
        SQL"""
          -- Retroactive divulgence events
          delete from participant_events_divulgence delete_events
          where delete_events.event_offset <= $pruneUpToInclusive
            or delete_events.event_offset is null
          """
      }
    } else {
      pruneWithLogging(queryDescription = "Archived retroactive divulgence events pruning") {
        // Note: do not use `QueryStrategy.offsetIsSmallerOrEqual` because divulgence events have a nullable offset
        SQL"""
          -- Retroactive divulgence events (only for contracts archived before the specified offset)
          delete from participant_events_divulgence delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive
            and exists (
              select 1 from participant_events_consuming_exercise archive_events
              where
                archive_events.event_offset <= $pruneUpToInclusive and
                archive_events.contract_id = delete_events.contract_id
            )"""
      }
    }

    pruneIdFilterTables(pruneUpToInclusive)

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (only for contracts archived before the specified offset)
          delete from participant_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            )"""
    }

    if (pruneAllDivulgedContracts) {
      val pruneAfterClause = {
        // We need to distinguish between the two cases since lexicographical comparison
        // in Oracle doesn't work with '' (empty strings are treated as NULLs) as one of the operands
        participantAllDivulgedContractsPrunedUpToInclusive(connection) match {
          case Some(pruneAfter) => cSQL"and event_offset > $pruneAfter"
          case None => cSQL""
        }
      }

      pruneWithLogging(queryDescription = "Immediate divulgence events pruning") {
        SQL"""
            -- Immediate divulgence pruning
            delete from participant_events_create c
            where event_offset <= $pruneUpToInclusive
            -- Only prune create events which did not have a locally hosted party before their creation offset
            and not exists (
              select 1
              from party_entries p
              where p.typ = 'accept'
              and p.ledger_offset <= c.event_offset
              and #${queryStrategy.isTrue("p.is_local")}
              and #${queryStrategy.arrayContains("c.flat_event_witnesses", "p.party_id")}
            )
            $pruneAfterClause
         """
      }
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from participant_events_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from participant_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "transaction meta pruning") {
      pruneTransactionMeta(pruneUpToInclusive = pruneUpToInclusive)
    }
  }

  private def pruneIdFilterTables(pruneUpToInclusive: Offset)(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    pruneWithLogging("Pruning id filter create stakeholder table") {
      pruneIdFilterCreateStakeholder(pruneUpToInclusive)
    }
    pruneWithLogging("Pruning id filter create non-stakeholder informee table") {
      pruneIdFilterCreateNonStakeholderInformee(pruneUpToInclusive)
    }
    pruneWithLogging("Pruning id filter consuming stakeholder table") {
      pruneIdFilterConsumingStakeholder(pruneUpToInclusive)
    }
    pruneWithLogging("Pruning id filter consuming non-stakeholders informee table") {
      pruneIdFilterConsumingNonStakeholderInformee(pruneUpToInclusive)
    }
    pruneWithLogging("Pruning id filter non-consuming informee table") {
      pruneIdFilterNonConsumingInformee(pruneUpToInclusive)
    }
  }

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")
  }

  override def maxEventSequentialId(
      untilInclusiveOffset: Offset
  )(connection: Connection): Long = {
    val ledgerEnd = ledgerEndCache()
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
     SELECT
        event_sequential_id_first
     FROM
        participant_transaction_meta
     WHERE
        ${queryStrategy.offsetIsGreater("event_offset", untilInclusiveOffset)}
        AND event_offset <= ${ledgerEnd._1}
     ORDER BY
        event_offset
     ${QueryStrategy.limitClause(Some(1))}
   """.as(get[Long](1).singleOpt)(connection)
      .getOrElse(
        // after the offset there is no meta, so no tx,
        // therefore the next (minimum) event sequential id will be
        // the first event sequential id after the ledger end
        ledgerEnd._2 + 1
      ) - 1
  }

  private def pruneIdFilterCreateStakeholder(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "pe_create_id_filter_stakeholder",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  private def pruneIdFilterCreateNonStakeholderInformee(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "pe_create_id_filter_non_stakeholder_informee",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  private def pruneIdFilterConsumingStakeholder(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_id_filter_stakeholder",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  private def pruneIdFilterConsumingNonStakeholderInformee(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_id_filter_non_stakeholder_informee",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )
  }

  private def pruneIdFilterNonConsumingInformee(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_non_consuming_id_filter_informee",
      eventsTableName = "participant_events_non_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  private def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
         DELETE FROM
            participant_transaction_meta m
         WHERE
          m.event_offset <= $pruneUpToInclusive
       """
  }

  // Improvement idea:
  // In order to prune an id filter table we query two additional tables: create and consuming events tables.
  // This can be simplified to query only the create events table if we ensure the ordering
  // that create events tables are pruned before id filter tables.
  /** Prunes create events id filter table only for contracts archived before the specified offset
    */
  private def pruneIdFilterCreate(tableName: String, pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            #$tableName id_filter
          WHERE EXISTS (
            SELECT * from participant_events_create c
            WHERE
            c.event_offset <= $pruneUpToInclusive
            AND
            EXISTS (
              SELECT 1 FROM participant_events_consuming_exercise archive
              WHERE
                archive.event_offset <= $pruneUpToInclusive
                AND
                archive.contract_id = c.contract_id
              )
            AND
            c.event_sequential_id = id_filter.event_sequential_id
          )"""
  }

  // Improvement idea:
  // In order to prune an id filter table we query an events table to discover
  // the event offset corresponding.
  // This query can simplified not to query the events table at all
  // if we were to prune by the sequential id rather than by the offset.
  private def pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName: String,
      eventsTableName: String,
      pruneUpToInclusive: Offset,
  ): SimpleSql[Row] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            #$idFilterTableName id_filter
          WHERE EXISTS (
            SELECT * FROM #$eventsTableName events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = id_filter.event_sequential_id
          )"""

  }

  override def assignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
  )(connection: Connection): Vector[EventStorageBackend.RawAssignEvent] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
        SELECT *
        FROM participant_events_assign assign_evs
        WHERE assign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY assign_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(assignEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def unassignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
  )(connection: Connection): Vector[EventStorageBackend.RawUnassignEvent] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
          SELECT *
          FROM participant_events_unassign unassign_evs
          WHERE unassign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          ORDER BY unassign_evs.event_sequential_id -- deliver in index order
          """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(unassignEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def activeContractAssignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.RawActiveContract] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
        SELECT *
        FROM participant_events_assign assign_evs
        WHERE
          assign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          AND NOT EXISTS (  -- check not archived as of snapshot in the same domain
                SELECT 1
                FROM participant_events_consuming_exercise consuming_evs
                WHERE
                  assign_evs.contract_id = consuming_evs.contract_id
                  AND assign_evs.target_domain_id = consuming_evs.domain_id
                  AND consuming_evs.event_sequential_id <= $endInclusive
              )
          AND NOT EXISTS (  -- check not unassigned after as of snapshot in the same domain
                SELECT 1
                FROM participant_events_unassign unassign_evs
                WHERE
                  assign_evs.contract_id = unassign_evs.contract_id
                  AND assign_evs.target_domain_id = unassign_evs.source_domain_id
                  AND unassign_evs.event_sequential_id > assign_evs.event_sequential_id
                  AND unassign_evs.event_sequential_id <= $endInclusive
                ${QueryStrategy.limitClause(Some(1))}
              )
        ORDER BY assign_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(assignActiveContractParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def activeContractCreateEventBatchV2(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.RawActiveContract] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
        SELECT *
        FROM participant_events_create create_evs
        WHERE
          create_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          AND NOT EXISTS (  -- check not archived as of snapshot in the same domain
                SELECT 1
                FROM participant_events_consuming_exercise consuming_evs
                WHERE
                  create_evs.contract_id = consuming_evs.contract_id
                  AND create_evs.domain_id = consuming_evs.domain_id
                  AND consuming_evs.event_sequential_id <= $endInclusive
              )
          AND NOT EXISTS (  -- check not unassigned as of snapshot in the same domain
                SELECT 1
                FROM participant_events_unassign unassign_evs
                WHERE
                  create_evs.contract_id = unassign_evs.contract_id
                  AND create_evs.domain_id = unassign_evs.source_domain_id
                  AND unassign_evs.event_sequential_id <= $endInclusive
                ${QueryStrategy.limitClause(Some(1))}
              )
        ORDER BY create_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(createActiveContractParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def fetchAssignEventIdsForStakeholder(
      stakeholder: Party,
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "pe_assign_id_filter_stakeholder",
      witness = stakeholder,
      templateIdO = templateId,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  override def fetchUnassignEventIdsForStakeholder(
      stakeholder: Party,
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "pe_unassign_id_filter_stakeholder",
      witness = stakeholder,
      templateIdO = templateId,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  override def lookupAssignSequentialIdByOffset(
      offsets: Iterable[String]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM participant_events_assign
        WHERE
          event_offset ${queryStrategy.anyOfStrings(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupUnassignSequentialIdByOffset(
      offsets: Iterable[String]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM participant_events_unassign
        WHERE
          event_offset ${queryStrategy.anyOfStrings(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupAssignSequentialIdByContractId(
      contractIds: Iterable[String]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT MIN(assign_evs.event_sequential_id) as event_sequential_id
        FROM participant_events_assign assign_evs
        WHERE contract_id ${queryStrategy.anyOfStrings(contractIds)}
        GROUP BY contract_id
        ORDER BY event_sequential_id
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupCreateSequentialIdByContractId(
      contractIds: Iterable[String]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM participant_events_create
        WHERE
          contract_id ${queryStrategy.anyOfStrings(contractIds)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

}
