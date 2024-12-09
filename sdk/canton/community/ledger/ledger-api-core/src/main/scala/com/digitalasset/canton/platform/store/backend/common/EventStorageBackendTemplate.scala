// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  contractId,
  hashFromHexString,
  offset,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  DomainOffset,
  Entry,
  RawActiveContract,
  RawArchivedEvent,
  RawAssignEvent,
  RawCreatedEvent,
  RawExercisedEvent,
  RawFlatEvent,
  RawTreeEvent,
  RawUnassignEvent,
  intToAuthorizationLevel,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Identifier, Party}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.value.Value.ContractId

import java.sql.Connection
import scala.util.Using

object EventStorageBackendTemplate {

  private val MaxBatchSizeOfIncompleteReassignmentOffsetTempTablePopulation: Int = 500

  private val baseColumnsForFlatTransactionsCreate =
    Seq(
      "event_offset",
      "update_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "contract_id",
      "template_id",
      "package_name",
      "package_version",
      "create_argument",
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_key_value",
      "create_key_hash",
      "create_key_value_compression",
      "create_key_maintainers",
      "submitters",
      "driver_metadata",
      "domain_id",
      "trace_context",
      "record_time",
    )

  private val baseColumnsForFlatTransactionsExercise =
    Seq(
      "event_offset",
      "update_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "contract_id",
      "template_id",
      "package_name",
      "NULL as package_version",
      "NULL as create_argument",
      "NULL as create_argument_compression",
      "NULL as create_signatories",
      "NULL as create_observers",
      "create_key_value",
      "NULL as create_key_hash",
      "create_key_value_compression",
      "NULL as create_key_maintainers",
      "submitters",
      "NULL as driver_metadata",
      "domain_id",
      "trace_context",
      "record_time",
    )

  val selectColumnsForFlatTransactionsCreate: String =
    baseColumnsForFlatTransactionsCreate.mkString(", ")

  val selectColumnsForFlatTransactionsExercise: String =
    baseColumnsForFlatTransactionsExercise.mkString(", ")

  private type SharedRow =
    Long ~ String ~ Int ~ Long ~ String ~ Timestamp ~ Int ~ Int ~ Option[String] ~
      Option[String] ~ Array[Int] ~ Option[Array[Int]] ~ Int ~ Option[Array[Byte]] ~ Timestamp

  private val sharedRow: RowParser[SharedRow] =
    long("event_offset") ~
      str("update_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("contract_id") ~
      timestampFromMicros("ledger_effective_time") ~
      int("template_id") ~
      int("package_name") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[Int]("event_witnesses") ~
      array[Int]("submitters").? ~
      int("domain_id") ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time")

  private type CreatedEventRow =
    SharedRow ~ Array[Byte] ~ Option[Int] ~ Array[Int] ~ Array[Int] ~
      Option[Array[Byte]] ~ Option[Hash] ~ Option[Int] ~ Option[Array[Int]] ~
      Array[Byte] ~ Option[Int]

  private val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      byteArray("create_key_value").? ~
      hashFromHexString("create_key_hash").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      byteArray("driver_metadata") ~
      int("package_version").?

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

  private[common] def createdEventParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawCreatedEvent]] =
    createdEventRow map {
      case offset ~
          updateId ~
          nodeIndex ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageName ~
          commandId ~
          workflowId ~
          eventWitnesses ~
          submitters ~
          internedDomainId ~
          traceContext ~
          recordTime ~
          createArgument ~
          createArgumentCompression ~
          createSignatories ~
          createObservers ~
          createKeyValue ~
          createKeyHash ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          driverMetadata ~
          packageVersion =>
        Entry(
          offset = offset,
          updateId = updateId,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = filteredCommandId(commandId, submitters, allQueryingPartiesO),
          workflowId = workflowId,
          event = RawCreatedEvent(
            updateId = updateId,
            offset = offset,
            nodeIndex = nodeIndex,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            packageVersion = packageVersion.map(stringInterning.packageVersion.externalize),
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              eventWitnesses,
              stringInterning,
            ),
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
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
          domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
          traceContext = traceContext,
          recordTime = recordTime,
        )
    }

  private[common] def archivedEventParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawArchivedEvent]] =
    archivedEventRow map {
      case eventOffset ~
          updateId ~
          nodeIndex ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageName ~
          commandId ~
          workflowId ~
          flatEventWitnesses ~
          submitters ~
          internedDomainId ~
          traceContext ~
          recordTime =>
        Entry(
          offset = eventOffset,
          updateId = updateId,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = filteredCommandId(commandId, submitters, allQueryingPartiesO),
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawArchivedEvent(
            updateId = updateId,
            offset = eventOffset,
            nodeIndex = nodeIndex,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
              stringInterning,
            ),
          ),
        )
    }

  def rawFlatEventParser(
      allQueryingParties: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawFlatEvent]] =
    (createdEventParser(allQueryingParties, stringInterning): RowParser[Entry[RawFlatEvent]]) |
      archivedEventParser(allQueryingParties, stringInterning)

  private def exercisedEventParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawExercisedEvent]] =
    exercisedEventRow map {
      case eventOffset ~
          updateId ~
          nodeIndex ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageName ~
          commandId ~
          workflowId ~
          treeEventWitnesses ~
          submitters ~
          internedDomainId ~
          traceContext ~
          recordTime ~
          exerciseConsuming ~
          choice ~
          exerciseArgument ~
          exerciseArgumentCompression ~
          exerciseResult ~
          exerciseResultCompression ~
          exerciseActors ~
          exerciseChildEventIds =>
        Entry(
          offset = eventOffset,
          updateId = updateId,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = filteredCommandId(commandId, submitters, allQueryingPartiesO),
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(internedDomainId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawExercisedEvent(
            updateId = updateId,
            offset = eventOffset,
            nodeIndex = nodeIndex,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = choice,
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors =
              exerciseActors.view.map(stringInterning.party.unsafe.externalize).toSeq,
            exerciseChildEventIds = exerciseChildEventIds.toSeq,
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              treeEventWitnesses,
              stringInterning,
            ),
          ),
        )
    }

  def rawTreeEventParser(
      allQueryingParties: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawTreeEvent]] =
    (createdEventParser(allQueryingParties, stringInterning): RowParser[Entry[RawTreeEvent]]) |
      exercisedEventParser(allQueryingParties, stringInterning)

  val selectColumnsForTransactionTreeCreate: String = Seq(
    "event_offset",
    "update_id",
    "node_index",
    "event_sequential_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "package_name",
    "package_version",
    "workflow_id",
    "create_argument",
    "create_argument_compression",
    "create_signatories",
    "create_observers",
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
    "record_time",
  ).mkString(", ")

  val selectColumnsForTransactionTreeExercise: String = Seq(
    "event_offset",
    "update_id",
    "node_index",
    "event_sequential_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "package_name",
    "NULL as package_version",
    "workflow_id",
    "NULL as create_argument",
    "NULL as create_argument_compression",
    "NULL as create_signatories",
    "NULL as create_observers",
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
    "record_time",
  ).mkString(", ")

  val EventSequentialIdFirstLast: RowParser[(Long, Long)] =
    long("event_sequential_id_first") ~ long("event_sequential_id_last") map {
      case event_sequential_id_first ~ event_sequential_id_last =>
        (event_sequential_id_first, event_sequential_id_last)
    }

  val partyToParticipantEventRow =
    long("event_sequential_id") ~
      offset("event_offset") ~
      str("update_id") ~
      int("party_id") ~
      str("participant_id") ~
      int("participant_permission") ~
      int("domain_id") ~
      timestampFromMicros("record_time") ~
      byteArray("trace_context").?

  private def partyToParticipantEventParser(
      stringInterning: StringInterning
  ): RowParser[EventStorageBackend.RawParticipantAuthorization] =
    partyToParticipantEventRow map {
      case _ ~
          eventOffset ~
          updateId ~
          partyId ~
          participantId ~
          participant_permission ~
          domainId ~
          recordTime ~
          traceContext =>
        EventStorageBackend.RawParticipantAuthorization(
          offset = eventOffset,
          updateId = updateId,
          partyId = stringInterning.party.unsafe.externalize(partyId),
          participantId = participantId,
          participant_permission = intToAuthorizationLevel(participant_permission),
          recordTime = recordTime,
          domainId = stringInterning.domainId.unsafe.externalize(domainId),
          traceContext = traceContext,
        )
    }

  val assignEventRow =
    str("command_id").? ~
      str("workflow_id").? ~
      long("event_offset") ~
      int("source_domain_id") ~
      int("target_domain_id") ~
      str("unassign_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      str("update_id") ~
      str("contract_id") ~
      int("template_id") ~
      int("package_name") ~
      int("package_version").? ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("driver_metadata") ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time") ~
      long("event_sequential_id")

  private def assignEventParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawAssignEvent]] =
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
          packageName ~
          packageVersion ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          traceContext ~
          recordTime ~
          eventSequentialId =>
        Entry(
          offset = offset,
          updateId = updateId,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = Timestamp.MinValue, // Not applicable
          commandId =
            filteredCommandId(commandId, submitter.map(Array[Int](_)), allQueryingPartiesO),
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawAssignEvent(
            sourceDomainId = stringInterning.domainId.unsafe.externalize(sourceDomainId),
            targetDomainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
            unassignId = unassignId,
            submitter = submitter.map(stringInterning.party.unsafe.externalize),
            reassignmentCounter = reassignmentCounter,
            rawCreatedEvent = RawCreatedEvent(
              updateId = updateId,
              offset = offset,
              nodeIndex = 0,
              contractId = contractId,
              templateId = stringInterning.templateId.externalize(templateId),
              packageName = stringInterning.packageName.externalize(packageName),
              packageVersion = packageVersion.map(stringInterning.packageVersion.externalize),
              witnessParties = filterAndExternalizeWitnesses(
                allQueryingPartiesO,
                flatEventWitnesses,
                stringInterning,
              ),
              signatories =
                createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
              observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
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
          ),
        )
    }

  val unassignEventRow =
    str("command_id").? ~
      str("workflow_id").? ~
      long("event_offset") ~
      int("source_domain_id") ~
      int("target_domain_id") ~
      str("unassign_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      str("update_id") ~
      str("contract_id") ~
      int("template_id") ~
      int("package_name") ~
      array[Int]("flat_event_witnesses") ~
      timestampFromMicros("assignment_exclusivity").? ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time") ~
      long("event_sequential_id")

  private def unassignEventParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawUnassignEvent]] =
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
          packageName ~
          flatEventWitnesses ~
          assignmentExclusivity ~
          traceContext ~
          recordTime ~
          eventSequentialId =>
        Entry(
          offset = offset,
          updateId = updateId,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = Timestamp.MinValue, // Not applicable
          commandId =
            filteredCommandId(commandId, submitter.map(Array[Int](_)), allQueryingPartiesO),
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawUnassignEvent(
            sourceDomainId = stringInterning.domainId.unsafe.externalize(sourceDomainId),
            targetDomainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
            unassignId = unassignId,
            submitter = submitter.map(stringInterning.party.unsafe.externalize),
            reassignmentCounter = reassignmentCounter,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
              stringInterning,
            ),
            assignmentExclusivity = assignmentExclusivity,
          ),
        )
    }

  val assignActiveContractRow =
    str("workflow_id").? ~
      int("target_domain_id") ~
      long("reassignment_counter") ~
      str("update_id") ~
      long("event_offset") ~
      str("contract_id") ~
      int("template_id") ~
      int("package_name") ~
      int("package_version").? ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
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
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[RawActiveContract] =
    assignActiveContractRow map {
      case workflowId ~
          targetDomainId ~
          reassignmentCounter ~
          updateId ~
          offset ~
          contractId ~
          templateId ~
          packageName ~
          packageVersion ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          eventSequentialId =>
        RawActiveContract(
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          reassignmentCounter = reassignmentCounter,
          rawCreatedEvent = RawCreatedEvent(
            updateId = updateId,
            offset = offset,
            nodeIndex = 0,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            packageVersion = packageVersion.map(stringInterning.packageVersion.externalize),
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
              stringInterning,
            ),
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
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
      str("update_id") ~
      long("event_offset") ~
      str("contract_id") ~
      int("template_id") ~
      int("package_name") ~
      int("package_version").? ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      array[Int]("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("driver_metadata") ~
      long("event_sequential_id") ~
      int("node_index")

  private def createActiveContractParser(
      allQueryingPartiesO: Option[Set[Int]],
      stringInterning: StringInterning,
  ): RowParser[RawActiveContract] =
    createActiveContractRow map {
      case workflowId ~
          targetDomainId ~
          updateId ~
          offset ~
          contractId ~
          templateId ~
          packageName ~
          packageVersion ~
          flatEventWitnesses ~
          createSignatories ~
          createObservers ~
          createArgument ~
          createArgumentCompression ~
          createKeyValue ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          ledgerEffectiveTime ~
          createKeyHash ~
          driverMetadata ~
          eventSequentialId ~
          nodeIndex =>
        RawActiveContract(
          workflowId = workflowId,
          domainId = stringInterning.domainId.unsafe.externalize(targetDomainId),
          reassignmentCounter = 0L, // zero for create
          rawCreatedEvent = RawCreatedEvent(
            updateId = updateId,
            offset = offset,
            nodeIndex = nodeIndex,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            packageName = stringInterning.packageName.externalize(packageName),
            packageVersion = packageVersion.map(stringInterning.packageVersion.externalize),
            witnessParties = filterAndExternalizeWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
              stringInterning,
            ),
            signatories =
              createSignatories.view.map(stringInterning.party.unsafe.externalize).toSet,
            observers = createObservers.view.map(stringInterning.party.unsafe.externalize).toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyMaintainers = createKeyMaintainers
              .map(_.map(stringInterning.party.unsafe.externalize).toSet)
              .getOrElse(Set.empty),
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            driverMetadata = driverMetadata,
          ),
          eventSequentialId = eventSequentialId,
        )
    }

  private def filterAndExternalizeWitnesses(
      allQueryingPartiesO: Option[Set[Int]],
      flatEventWitnesses: Array[Int],
      stringInterning: StringInterning,
  ): Set[String] =
    allQueryingPartiesO
      .fold(flatEventWitnesses)(allQueryingParties =>
        flatEventWitnesses
          .filter(allQueryingParties)
      )
      .map(stringInterning.party.unsafe.externalize)
      .toSet

  private def filteredCommandId(
      commandId: Option[String],
      submitters: Option[Array[Int]],
      allQueryingPartiesO: Option[Set[Int]],
  ): Option[String] = {
    def submittersInQueryingParties: Boolean = allQueryingPartiesO match {
      case Some(allQueryingParties) =>
        submitters.getOrElse(Array.empty).exists(allQueryingParties)
      case None => submitters.nonEmpty
    }
    commandId.filter(_ != "" && submittersInQueryingParties)
  }

  private def domainOffsetParser(
      offsetColumnName: String,
      stringInterning: StringInterning,
  ): RowParser[DomainOffset] =
    offset(offsetColumnName) ~
      int("domain_id") ~
      timestampFromMicros("record_time") ~
      timestampFromMicros("publication_time") map {
        case offset ~ internedDomainId ~ recordTime ~ publicationTime =>
          DomainOffset(
            offset = offset,
            domainId = stringInterning.domainId.externalize(internedDomainId),
            recordTime = recordTime,
            publicationTime = publicationTime,
          )
      }

  private def completionDomainOffsetParser(
      stringInterning: StringInterning
  ): RowParser[DomainOffset] =
    domainOffsetParser("completion_offset", stringInterning)

  private def metaDomainOffsetParser(stringInterning: StringInterning): RowParser[DomainOffset] =
    domainOffsetParser("event_offset", stringInterning)
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
  import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

  override def transactionPointwiseQueries: TransactionPointwiseQueries =
    new TransactionPointwiseQueries(
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
    )

  override def transactionStreamingQueries: TransactionStreamingQueries =
    new TransactionStreamingQueries(
      queryStrategy = queryStrategy,
      stringInterning = stringInterning,
    )

  override def eventReaderQueries: EventReaderQueries =
    new EventReaderQueries(stringInterning)

  // Improvement idea: Implement pruning queries in terms of event sequential id in order to be able to drop offset based indices.
  /** Deletes a subset of the indexed data (up to the pruning offset) in the following order and in the manner specified:
    * 1. entries from filter for create stakeholders for there is an archive for the corresponding create event,
    * 2. entries from filter for create non-stakeholder informees for there is an archive for the corresponding create event,
    * 3. all entries from filter for consuming stakeholders,
    * 4. all entries from filter for consuming non-stakeholders informees,
    * 5. all entries from filter for non-consuming informees,
    * 6. create events table for which there is an archive event,
    * 7. if pruning-all-divulged-contracts is enabled: create contracts which did not have a locally hosted party before their creation offset (immediate divulgence),
    * 8. all consuming events,
    * 9. all non-consuming events,
    * 10. transaction meta entries for which there exists at least one create event.
    */
  override def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit connection: Connection, traceContext: TraceContext): Unit = {
    val _ =
      SQL"""
          -- Create temporary table for storing incomplete reassignment offsets
          CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS temp_incomplete_reassignment_offsets (
            incomplete_offset bigint PRIMARY KEY NOT NULL
          ) ON COMMIT DELETE ROWS
          """.execute()
    val incompleteOffsetBatches = incompletReassignmentOffsets.distinct
      .grouped(MaxBatchSizeOfIncompleteReassignmentOffsetTempTablePopulation)
    Using.resource(
      connection.prepareStatement(
        "INSERT INTO temp_incomplete_reassignment_offsets(incomplete_offset) VALUES (?)"
      )
    ) { preparedStatement =>
      incompleteOffsetBatches.zipWithIndex.foreach { case (batch, index) =>
        batch.foreach { offset =>
          preparedStatement.setLong(1, offset.unwrap)
          preparedStatement.addBatch()
        }
        val _ = preparedStatement.executeBatch()
        logger.debug(
          s"Uploaded incomplete offsets batch #${index + 1} / ${incompleteOffsetBatches.size}"
        )
      }
    }
    val _ =
      SQL"${queryStrategy.analyzeTable("temp_incomplete_reassignment_offsets")}".execute()
    logger.info(
      s"Populated temp_incomplete_reassignment_offsets table with ${incompletReassignmentOffsets.size} entries"
    )

    pruneIdFilterTables(pruneUpToInclusive)

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (only for contracts archived before the specified offset)
          delete from lapi_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            ${createIsArchivedOrUnassigned("delete_events", pruneUpToInclusive)}"""
    }

    pruneWithLogging(queryDescription = "Assign events pruning") {
      SQL"""
          -- Assigned events
          delete from lapi_events_assign delete_events
          where
            -- do not prune incomplete
            ${reassignmentIsNotIncomplete("delete_events")}
            -- only prune if it is archived in same domain, or unassigned later in the same domain
            and ${assignIsArchivedOrUnassigned("delete_events", pruneUpToInclusive)}
            and delete_events.event_offset <= $pruneUpToInclusive"""
    }

    if (pruneAllDivulgedContracts) {
      val pruneAfterClause =
        participantAllDivulgedContractsPrunedUpToInclusive(connection) match {
          case Some(pruneAfter) => cSQL"and event_offset > $pruneAfter"
          case None => cSQL""
        }

      pruneWithLogging(queryDescription = "Immediate divulgence events pruning") {
        SQL"""
            -- Immediate divulgence pruning
            delete from lapi_events_create c
            where event_offset <= $pruneUpToInclusive
            -- Only prune create events which did not have a locally hosted party before their creation offset
            and not exists (
              select 1
              from lapi_party_entries p
              where p.typ = 'accept'
              and p.ledger_offset <= c.event_offset
              and p.is_local
              and #${queryStrategy.arrayContains("c.flat_event_witnesses", "p.party_id")}
            )
            $pruneAfterClause
         """
      }
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from lapi_events_consuming_exercise delete_events
          where
            -- do not prune if it is preceeded in the same domain by an incomplete assign
            -- this is needed so that incomplete assign is not resulting in an active contract
            ${deactivationIsNotDirectlyPreceededByIncompleteAssign("delete_events", "domain_id")}
            and delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from lapi_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Transaction Meta pruning") {
      SQL"""
           DELETE FROM
              lapi_transaction_meta m
           WHERE
            m.event_offset <= $pruneUpToInclusive
         """
    }

    pruneWithLogging(queryDescription = "Unassign events pruning") {
      SQL"""
          -- Unassigned events
          delete from lapi_events_unassign delete_events
          where
            -- do not prune incomplete
            ${reassignmentIsNotIncomplete("delete_events")}
            -- do not prune if it is preceeded in the same domain by an incomplete assign
            -- this is needed so that incomplete assign is not resulting in an active contract
            and ${deactivationIsNotDirectlyPreceededByIncompleteAssign(
          "delete_events",
          "source_domain_id",
        )}
            and delete_events.event_offset <= $pruneUpToInclusive"""
    }
  }

  private def pruneIdFilterTables(pruneUpToInclusive: Offset)(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    // Improvement idea:
    // In order to prune an id filter table we query two additional tables: create and consuming events tables.
    // This can be simplified to query only the create events table if we ensure the ordering
    // that create events tables are pruned before id filter tables.
    def pruneIdFilterCreate(tableName: String): SimpleSql[Row] =
      SQL"""
            DELETE FROM
              #$tableName id_filter
            WHERE EXISTS (
              SELECT * from lapi_events_create c
              WHERE
              c.event_offset <= $pruneUpToInclusive
              AND ${createIsArchivedOrUnassigned("c", pruneUpToInclusive)}
              AND c.event_sequential_id = id_filter.event_sequential_id
            )"""

    // Improvement idea:
    // In order to prune an id filter table we query an events table to discover
    // the event offset corresponding.
    // This query can simplified not to query the events table at all
    // if we were to prune by the sequential id rather than by the offset.
    def pruneIdFilterConsuming(
        idFilterTableName: String
    ): SimpleSql[Row] =
      SQL"""
            DELETE FROM
              #$idFilterTableName id_filter
            WHERE EXISTS (
              SELECT * FROM lapi_events_consuming_exercise events
            WHERE
              events.event_offset <= $pruneUpToInclusive
              AND
              ${deactivationIsNotDirectlyPreceededByIncompleteAssign("events", "domain_id")}
              AND
              events.event_sequential_id = id_filter.event_sequential_id
            )"""

    def pruneIdFilterNonConsuming(
        idFilterTableName: String
    ): SimpleSql[Row] =
      SQL"""
            DELETE FROM
              #$idFilterTableName id_filter
            WHERE EXISTS (
              SELECT * FROM lapi_events_non_consuming_exercise events
            WHERE
              events.event_offset <= $pruneUpToInclusive
              AND
              events.event_sequential_id = id_filter.event_sequential_id
            )"""

    pruneWithLogging("Pruning id filter create stakeholder table") {
      pruneIdFilterCreate("lapi_pe_create_id_filter_stakeholder")
    }
    pruneWithLogging("Pruning id filter create non-stakeholder informee table") {
      pruneIdFilterCreate("lapi_pe_create_id_filter_non_stakeholder_informee")
    }
    pruneWithLogging("Pruning id filter consuming stakeholder table") {
      pruneIdFilterConsuming(
        idFilterTableName = "lapi_pe_consuming_id_filter_stakeholder"
      )
    }
    pruneWithLogging("Pruning id filter consuming non-stakeholders informee table") {
      pruneIdFilterConsuming(
        idFilterTableName = "lapi_pe_consuming_id_filter_non_stakeholder_informee"
      )
    }
    pruneWithLogging("Pruning id filter non-consuming informee table") {
      pruneIdFilterNonConsuming(
        idFilterTableName = "lapi_pe_non_consuming_id_filter_informee"
      )
    }
    pruneWithLogging("Pruning id filter assign stakeholder table") {
      SQL"""
          DELETE FROM lapi_pe_assign_id_filter_stakeholder id_filter
          WHERE EXISTS (
            SELECT * from lapi_events_assign assign
            WHERE
              assign.event_offset <= $pruneUpToInclusive
              AND ${assignIsArchivedOrUnassigned("assign", pruneUpToInclusive)}
              AND ${reassignmentIsNotIncomplete("assign")}
              AND assign.event_sequential_id = id_filter.event_sequential_id
          )"""
    }
    pruneWithLogging("Pruning id filter unassign stakeholder table") {
      SQL"""
          DELETE FROM lapi_pe_unassign_id_filter_stakeholder id_filter
          WHERE EXISTS (
            SELECT * from lapi_events_unassign unassign
            WHERE
            unassign.event_offset <= $pruneUpToInclusive
            AND ${reassignmentIsNotIncomplete("unassign")}
            AND ${deactivationIsNotDirectlyPreceededByIncompleteAssign(
          "unassign",
          "source_domain_id",
        )}
            AND unassign.event_sequential_id = id_filter.event_sequential_id
          )"""
    }
  }

  private def createIsArchivedOrUnassigned(
      createEventTableName: String,
      pruneUpToInclusive: Offset,
  ): CompositeSql =
    cSQL"""
          ${eventIsArchivedOrUnassigned(createEventTableName, pruneUpToInclusive, "domain_id")}
          and ${activationIsNotDirectlyFollowedByIncompleteUnassign(
        createEventTableName,
        "domain_id",
        pruneUpToInclusive,
      )}
          """

  private def assignIsArchivedOrUnassigned(
      assignEventTableName: String,
      pruneUpToInclusive: Offset,
  ): CompositeSql =
    cSQL"""
      ${eventIsArchivedOrUnassigned(assignEventTableName, pruneUpToInclusive, "target_domain_id")}
      and ${activationIsNotDirectlyFollowedByIncompleteUnassign(
        assignEventTableName,
        "target_domain_id",
        pruneUpToInclusive,
      )}
      """

  private def eventIsArchivedOrUnassigned(
      eventTableName: String,
      pruneUpToInclusive: Offset,
      eventDomainName: String,
  ): CompositeSql =
    cSQL"""
          (
            exists (
              SELECT 1 FROM lapi_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive
                -- please note: this is the only indexed constraint, this is enough since there can be at most one archival
                AND archive_events.contract_id = #$eventTableName.contract_id
                AND archive_events.domain_id = #$eventTableName.#$eventDomainName
            )
            or
            exists (
              SELECT 1 FROM lapi_events_unassign unassign_events
              WHERE
                unassign_events.event_offset <= $pruneUpToInclusive
                AND unassign_events.contract_id = #$eventTableName.contract_id
                AND unassign_events.source_domain_id = #$eventTableName.#$eventDomainName
                -- with this constraint the index (contract_id, domain_id, event_sequential_id) can be used
                -- and what we only need is one unassign later in the same domain
                AND unassign_events.event_sequential_id > #$eventTableName.event_sequential_id
              ${QueryStrategy.limitClause(Some(1))}
            )
          )"""
  private def reassignmentIsNotIncomplete(eventTableName: String): CompositeSql =
    cSQL"""
          not exists (
            select 1
            from temp_incomplete_reassignment_offsets
            where temp_incomplete_reassignment_offsets.incomplete_offset = #$eventTableName.event_offset
          )"""

  // the not exists (select where in (select limit 1)) contruction is the one which is compatible with H2
  // other similar constructions with CTE/subqueries are working just fine with PG but not with H2 due
  // to some impediment/bug not being able to recognize references to the deactivationTableName (it is also
  // an experimental feature in H2)
  // in case the PG version produces inefficient plans, the implementation need to be made polimorphic accordingly
  // authors hope is that the in (select limit 1) clause will be materialized only once due to no relation to the
  // incomplete temp table
  // Please note! The limit clause is essential, otherwise contracts which move frequently accross domains can
  // cause quadratic increase in query cost.
  private def deactivationIsNotDirectlyPreceededByIncompleteAssign(
      deactivationTableName: String,
      deactivationDomainColumnName: String,
  ): CompositeSql =
    cSQL"""
          not exists (
            SELECT 1
            FROM temp_incomplete_reassignment_offsets
            WHERE
              temp_incomplete_reassignment_offsets.incomplete_offset in (
                SELECT assign_events.event_offset
                FROM lapi_events_assign assign_events
                WHERE
                  -- this one is backed by a (contract_id, event_sequential_id) index only
                  assign_events.contract_id = #$deactivationTableName.contract_id
                  AND assign_events.target_domain_id = #$deactivationTableName.#$deactivationDomainColumnName
                  AND assign_events.event_sequential_id < #$deactivationTableName.event_sequential_id
                ORDER BY event_sequential_id DESC
                ${QueryStrategy.limitClause(Some(1))}
              )
          )"""
  private def activationIsNotDirectlyFollowedByIncompleteUnassign(
      activationTableName: String,
      activationDomainColumnName: String,
      pruneUpToInclusive: Offset,
  ): CompositeSql =
    cSQL"""
          not exists (
            SELECT 1
            FROM temp_incomplete_reassignment_offsets
            WHERE
              temp_incomplete_reassignment_offsets.incomplete_offset in (
                SELECT unassign_events.event_offset
                FROM lapi_events_unassign unassign_events
                WHERE
                  -- this one is backed by a (contract_id, domain_id, event_sequential_id) index
                  unassign_events.contract_id = #$activationTableName.contract_id
                  AND unassign_events.source_domain_id = #$activationTableName.#$activationDomainColumnName
                  AND unassign_events.event_sequential_id > #$activationTableName.event_sequential_id
                  AND unassign_events.event_offset <= $pruneUpToInclusive
                ORDER BY event_sequential_id ASC
                ${QueryStrategy.limitClause(Some(1))}
              )
          )"""

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")
  }

  override def maxEventSequentialId(
      untilInclusiveOffset: Option[Offset]
  )(connection: Connection): Long = {
    val ledgerEnd = ledgerEndCache()
    SQL"""
     SELECT
        event_sequential_id_first
     FROM
        lapi_transaction_meta
     WHERE
        ${QueryStrategy.offsetIsGreater("event_offset", untilInclusiveOffset)}
        AND ${QueryStrategy.offsetIsLessOrEqual("event_offset", ledgerEnd.map(_.lastOffset))}
     ORDER BY
        event_offset
     ${QueryStrategy.limitClause(Some(1))}
   """.as(get[Long](1).singleOpt)(connection)
      .getOrElse(
        // after the offset there is no meta, so no tx,
        // therefore the next (minimum) event sequential id will be
        // the first event sequential id after the ledger end
        ledgerEnd.map(_.lastEventSeqId).getOrElse(0L) + 1
      ) - 1
  }

  override def assignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawAssignEvent]] = {
    val allInternedFilterParties =
      allFilterParties
        .map(
          _.iterator
            .map(stringInterning.party.tryInternalize)
            .flatMap(_.iterator)
            .toSet
        )
    SQL"""
        SELECT *
        FROM lapi_events_assign assign_evs
        WHERE assign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY assign_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(assignEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def unassignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawUnassignEvent]] = {
    val allInternedFilterParties = allFilterParties
      .map(
        _.iterator
          .map(stringInterning.party.tryInternalize)
          .flatMap(_.iterator)
          .toSet
      )
    SQL"""
          SELECT *
          FROM lapi_events_unassign unassign_evs
          WHERE unassign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          ORDER BY unassign_evs.event_sequential_id -- deliver in index order
          """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(unassignEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  override def activeContractAssignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract] = {
    val allInternedFilterParties =
      allFilterParties
        .map(
          _.iterator
            .map(stringInterning.party.tryInternalize)
            .flatMap(_.iterator)
            .toSet
        )
    SQL"""
        SELECT *
        FROM lapi_events_assign assign_evs
        WHERE
          assign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          AND NOT EXISTS (  -- check not archived as of snapshot in the same domain
                SELECT 1
                FROM lapi_events_consuming_exercise consuming_evs
                WHERE
                  assign_evs.contract_id = consuming_evs.contract_id
                  AND assign_evs.target_domain_id = consuming_evs.domain_id
                  AND consuming_evs.event_sequential_id <= $endInclusive
              )
          AND NOT EXISTS (  -- check not unassigned after as of snapshot in the same domain
                SELECT 1
                FROM lapi_events_unassign unassign_evs
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

  override def activeContractCreateEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract] = {
    val allInternedFilterParties = allFilterParties.map(
      _.iterator
        .map(stringInterning.party.tryInternalize)
        .flatMap(_.iterator)
        .toSet
    )
    SQL"""
        SELECT *
        FROM lapi_events_create create_evs
        WHERE
          create_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          AND NOT EXISTS (  -- check not archived as of snapshot in the same domain
                SELECT 1
                FROM lapi_events_consuming_exercise consuming_evs
                WHERE
                  create_evs.contract_id = consuming_evs.contract_id
                  AND create_evs.domain_id = consuming_evs.domain_id
                  AND consuming_evs.event_sequential_id <= $endInclusive
              )
          AND NOT EXISTS (  -- check not unassigned as of snapshot in the same domain
                SELECT 1
                FROM lapi_events_unassign unassign_evs
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
      stakeholderO: Option[Party],
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_assign_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateId,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  override def fetchUnassignEventIdsForStakeholder(
      stakeholderO: Option[Party],
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_unassign_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateId,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  override def lookupAssignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_assign
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupUnassignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_unassign
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupAssignSequentialIdByContractId(
      contractIds: Iterable[String]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT MIN(assign_evs.event_sequential_id) as event_sequential_id
        FROM lapi_events_assign assign_evs
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
        FROM lapi_events_create
        WHERE
          contract_id ${queryStrategy.anyOfStrings(contractIds)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  def firstDomainOffsetAfterOrAt(
      domainId: DomainId,
      afterOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[DomainOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, domain_id
          FROM lapi_command_completions
          WHERE
            domain_id = ${stringInterning.domainId.internalize(domainId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY domain_id ASC, record_time ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionDomainOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, domain_id
          FROM lapi_transaction_meta
          WHERE
            domain_id = ${stringInterning.domainId.internalize(domainId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY domain_id ASC, record_time ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaDomainOffsetParser(stringInterning))(connection),
    ).flatten
      .minByOption(_.recordTime)
      .filter(domainOffset =>
        Option(domainOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if the first is after LedgerEnd, then we have none

  def lastDomainOffsetBeforeOrAt(
      domainIdO: Option[DomainId],
      beforeOrAtOffsetInclusive: Offset,
  )(connection: Connection): Option[DomainOffset] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)
    val safeBeforeOrAtOffset =
      if (Option(beforeOrAtOffsetInclusive) > ledgerEndOffset) ledgerEndOffset
      else Some(beforeOrAtOffsetInclusive)
    val (domainIdFilter, domainIdOrdering) = domainIdO match {
      case Some(domainId) =>
        (
          cSQL"domain_id = ${stringInterning.domainId.internalize(domainId)} AND",
          cSQL"domain_id,",
        )

      case None =>
        (
          cSQL"",
          cSQL"",
        )
    }
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, domain_id
          FROM lapi_command_completions
          WHERE
            $domainIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("completion_offset", safeBeforeOrAtOffset)}
          ORDER BY $domainIdOrdering completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionDomainOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, domain_id
          FROM lapi_transaction_meta
          WHERE
            $domainIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("event_offset", safeBeforeOrAtOffset)}
          ORDER BY $domainIdOrdering event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaDomainOffsetParser(stringInterning))(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  def domainOffset(offset: Offset)(connection: Connection): Option[DomainOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, domain_id
          FROM lapi_command_completions
          WHERE
            completion_offset = $offset
          """.asSingleOpt(completionDomainOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, domain_id
          FROM lapi_transaction_meta
          WHERE
            event_offset = $offset
          """.asSingleOpt(metaDomainOffsetParser(stringInterning))(connection),
    ).flatten.headOption // if both present they should be the same
      .filter(domainOffset =>
        Option(domainOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // only offset allow before or at ledger end

  def firstDomainOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[DomainOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, domain_id
          FROM lapi_command_completions
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, completion_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionDomainOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, domain_id
          FROM lapi_transaction_meta
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, event_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaDomainOffsetParser(stringInterning))(connection),
    ).flatten
      .minByOption(_.offset)
      .filter(domainOffset =>
        Option(domainOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if first offset is beyond the ledger-end then we have no such

  def lastDomainOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[DomainOffset] = {
    val ledgerEndPublicationTime =
      ledgerEndCache().map(_.lastPublicationTime).getOrElse(CantonTimestamp.MinValue).underlying
    val safePublicationTime =
      if (beforeOrAtPublicationTimeInclusive > ledgerEndPublicationTime)
        ledgerEndPublicationTime
      else
        beforeOrAtPublicationTimeInclusive
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, domain_id
          FROM lapi_command_completions
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionDomainOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, domain_id
          FROM lapi_transaction_meta
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaDomainOffsetParser(stringInterning))(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  def archivals(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[ContractId] = {
    val fromExclusiveSeqId =
      fromExclusive
        .map(from => maxEventSequentialId(Some(from))(connection))
        .getOrElse(-1L)
    val toInclusiveSeqId = maxEventSequentialId(Some(toInclusive))(connection)
    SQL"""
        SELECT contract_id
        FROM lapi_events_consuming_exercise
        WHERE
          event_sequential_id > $fromExclusiveSeqId AND
          event_sequential_id <= $toInclusiveSeqId
        """
      .asVectorOf(contractId("contract_id"))(connection)
      .toSet
  }
  override def fetchTopologyPartyEventIds(
      party: Option[Party],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_events_party_to_participant",
      witnessO = party,
      templateIdO = None,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  override def topologyPartyEventBatch(
      eventSequentialIds: Iterable[Long]
  )(connection: Connection): Vector[EventStorageBackend.RawParticipantAuthorization] =
    SQL"""
          SELECT *
          FROM lapi_events_party_to_participant e
          WHERE e.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
          ORDER BY e.event_sequential_id -- deliver in index order
          """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(partyToParticipantEventParser(stringInterning))(connection)
}
