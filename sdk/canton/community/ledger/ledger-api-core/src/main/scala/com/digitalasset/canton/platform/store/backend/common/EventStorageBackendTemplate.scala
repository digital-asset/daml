// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  authorizationEventParser,
  contractId,
  hashFromHexString,
  offset,
  parties,
  timestampFromMicros,
  updateId,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.*
import com.digitalasset.canton.platform.store.backend.RowDef.*
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.{
  EventStorageBackend,
  PersistentEventType,
  RowDef,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.PaginationInput
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Party}
import com.digitalasset.canton.protocol.{ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  FullIdentifier,
  Identifier,
  NameTypeConRef,
  NameTypeConRefConverter,
}
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.sql.Connection
import scala.util.Using

object EventStorageBackendTemplate {

  private val MaxBatchSizeOfIncompleteReassignmentOffsetTempTablePopulation: Int = 500

  object RowDefs {
    // update related
    val workflowId: RowDef[Option[String]] =
      column("workflow_id", str(_).?)
    def genSynchronizerId(columnName: String)(stringInterning: StringInterning): RowDef[String] =
      column(columnName, int(_).map(stringInterning.synchronizerId.unsafe.externalize))
    def synchronizerId(stringInterning: StringInterning): RowDef[String] =
      genSynchronizerId("synchronizer_id")(stringInterning)
    def sourceSynchronizerId(stringInterning: StringInterning): RowDef[String] =
      genSynchronizerId("source_synchronizer_id")(stringInterning)
    def targetSynchronizerId(stringInterning: StringInterning): RowDef[String] =
      genSynchronizerId("target_synchronizer_id")(stringInterning)
    val eventOffset: RowDef[Long] =
      column("event_offset", long)
    val updateIdDef: RowDef[String] =
      column("update_id", updateId(_).map(_.toHexString))
    def commandId(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[Option[String]] =
      combine(
        column("command_id", str(_).?),
        column("submitters", parties(stringInterning)(_).?),
      )(filteredCommandId(_, _, allQueryingPartiesO))
    val traceContext: RowDef[Array[Byte]] =
      column("trace_context", byteArray(_))
    val recordTime: RowDef[Timestamp] =
      column("record_time", timestampFromMicros)
    val externalTransactionHash: RowDef[Option[Array[Byte]]] =
      column("external_transaction_hash", byteArray(_).?)

    // event related
    val nodeId: RowDef[Int] =
      column("node_id", int)
    val eventSequentialId: RowDef[Long] =
      column("event_sequential_id", long)
    def filteredAdditionalWitnesses(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    )(witnessIsAcsDelta: Boolean): RowDef[Set[String]] =
      if (witnessIsAcsDelta)
        static(Set.empty)
      else
        column("additional_witnesses", parties(stringInterning)(_))
          .map(filterWitnesses(allQueryingPartiesO, _))
    val eventType: RowDef[PersistentEventType] =
      column("event_type", int(_).map(PersistentEventType.fromInt))

    // contract related
    def representativePackageId(stringInterning: StringInterning): RowDef[String] =
      column("representative_package_id", int(_).map(stringInterning.packageId.unsafe.externalize))
    val contractIdDef: RowDef[ContractId] =
      column("contract_id", contractId)
    val internalContractId: RowDef[Long] =
      column("internal_contract_id", long)
    val reassignmentCounter: RowDef[Long] =
      column("reassignment_counter", long(_).?.map(_.getOrElse(0L)))
    val ledgerEffectiveTime: RowDef[Timestamp] =
      column("ledger_effective_time", timestampFromMicros)
    def templateId(stringInterning: StringInterning): RowDef[FullIdentifier] =
      combine(
        column("template_id", int(_).map(stringInterning.templateId.externalize)),
        column("package_id", int(_).map(stringInterning.packageId.externalize)),
      )(_ toFullIdentifier _)
    def filteredStakeholderParties(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[Set[String]] =
      // stakeholders are not present in various_witnessed, but exercises and transient/divulged contracts retrieved from there
      column("stakeholders", parties(stringInterning)(_).?)
        .map(_.getOrElse(Seq.empty))
        .map(filterWitnesses(allQueryingPartiesO, _))

    // reassignment related
    val reassignmentId: RowDef[String] =
      column(
        "reassignment_id",
        byteArray(_).map(ReassignmentId.assertFromBytes(_).toProtoPrimitive),
      )
    def submitter(stringInterning: StringInterning): RowDef[Option[String]] =
      column("submitters", parties(stringInterning)(_).?.map(_.getOrElse(Seq.empty).headOption))
    val assignmentExclusivity: RowDef[Option[Timestamp]] =
      column("assignment_exclusivity", timestampFromMicros(_).?)

    // exercise related
    val consuming: RowDef[Boolean] =
      column("consuming", bool(_))
    def exerciseChoice(stringInterning: StringInterning): RowDef[ChoiceName] =
      column("exercise_choice", int(_).map(stringInterning.choiceName.externalize))
    def exerciseChoiceInterface(stringInterning: StringInterning): RowDef[Option[Identifier]] =
      column(
        "exercise_choice_interface",
        int(_).?.map(_.map(stringInterning.interfaceId.externalize)),
      )
    val exerciseArgument: RowDef[Array[Byte]] =
      column("exercise_argument", byteArray(_))
    val exerciseArgumentCompression: RowDef[Option[Int]] =
      column("exercise_argument_compression", int(_).?)
    val exerciseResult: RowDef[Option[Array[Byte]]] =
      column("exercise_result", byteArray(_).?)
    val exerciseResultCompression: RowDef[Option[Int]] =
      column("exercise_result_compression", int(_).?)
    def exerciseActors(stringInterning: StringInterning): RowDef[Set[String]] =
      column("exercise_actors", parties(stringInterning)(_).map(_.map(_.toString).toSet))
    val exerciseLastDescendantNodeId: RowDef[Int] =
      column("exercise_last_descendant_node_id", int)

    // properties
    def commonEventPropertiesParser(
        stringInterning: StringInterning
    ): RowDef[CommonEventProperties] =
      combine(
        eventSequentialId,
        eventOffset,
        nodeId,
        workflowId,
        synchronizerId(stringInterning),
      )(CommonEventProperties.apply)

    def commonUpdatePropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[CommonUpdateProperties] =
      combine(
        updateIdDef,
        commandId(stringInterning, allQueryingPartiesO),
        traceContext,
        recordTime,
      )(CommonUpdateProperties.apply)

    def transactionPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[TransactionProperties] =
      combine(
        commonEventPropertiesParser(stringInterning),
        commonUpdatePropertiesParser(stringInterning, allQueryingPartiesO),
        externalTransactionHash,
      )(TransactionProperties.apply)

    def reassignmentPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[ReassignmentProperties] =
      combine(
        commonEventPropertiesParser(stringInterning),
        commonUpdatePropertiesParser(stringInterning, allQueryingPartiesO),
        reassignmentId,
        submitter(stringInterning),
        reassignmentCounter,
      )(ReassignmentProperties.apply)

    def thinCreatedEventPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        witnessIsAcsDelta: Boolean,
        eventIsAcsDelta: Boolean,
    ): RowDef[ThinCreatedEventProperties] =
      combine(
        representativePackageId(stringInterning),
        filteredAdditionalWitnesses(stringInterning, allQueryingPartiesO)(witnessIsAcsDelta),
        internalContractId,
        static(allQueryingPartiesO.map(_.map(_.toString))),
        if (eventIsAcsDelta) reassignmentCounter else static(0L),
        static(eventIsAcsDelta),
      )(ThinCreatedEventProperties.apply)

    // raws
    def rawThinActiveContractParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawThinActiveContract] =
      combine(
        commonEventPropertiesParser(stringInterning),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = true,
          eventIsAcsDelta = true,
        ),
      )(RawThinActiveContract.apply)

    def rawThinCreatedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        witnessIsAcsDelta: Boolean,
        eventIsAcsDelta: Boolean,
    ): RowDef[RawThinCreatedEvent] =
      combine(
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = witnessIsAcsDelta,
          eventIsAcsDelta = eventIsAcsDelta,
        ),
      )(RawThinCreatedEvent.apply)

    def rawThinAssignEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawThinAssignEvent] =
      combine(
        reassignmentPropertiesParser(stringInterning, allQueryingPartiesO),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = true,
          eventIsAcsDelta = true,
        ),
        sourceSynchronizerId(stringInterning),
      )(RawThinAssignEvent.apply)

    def rawArchivedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        acsDelta: Boolean,
    ): RowDef[RawArchivedEvent] =
      combine(
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        if (acsDelta) filteredStakeholderParties(stringInterning, allQueryingPartiesO)
        else static(Set.empty[String]),
        ledgerEffectiveTime,
      )(RawArchivedEvent.apply)

    def rawExercisedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        eventIsAcsDelta: Boolean,
    ): RowDef[RawExercisedEvent] =
      combine(
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        if (eventIsAcsDelta) static(true) else consuming,
        exerciseChoice(stringInterning),
        exerciseChoiceInterface(stringInterning),
        exerciseArgument,
        exerciseArgumentCompression,
        exerciseResult,
        exerciseResultCompression,
        exerciseActors(stringInterning),
        exerciseLastDescendantNodeId,
        filteredAdditionalWitnesses(stringInterning, allQueryingPartiesO)(witnessIsAcsDelta =
          false
        ),
        if (eventIsAcsDelta) filteredStakeholderParties(stringInterning, allQueryingPartiesO)
        else static(Set.empty[String]),
        ledgerEffectiveTime,
        static(eventIsAcsDelta),
      )(RawExercisedEvent.apply)

    def rawUnassignEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawUnassignEvent] =
      combine(
        reassignmentPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        filteredStakeholderParties(stringInterning, allQueryingPartiesO),
        assignmentExclusivity,
        targetSynchronizerId(stringInterning),
      )(RawUnassignEvent.apply)
  }

  private val baseColumnsForFlatTransactionsCreate =
    Seq(
      "event_offset",
      "update_id",
      "node_id",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "contract_id",
      "template_id",
      "package_id",
      "representative_package_id",
      "create_argument",
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_key_value",
      "create_key_hash",
      "create_key_value_compression",
      "create_key_maintainers",
      "submitters",
      "authentication_data",
      "synchronizer_id",
      "trace_context",
      "record_time",
      "external_transaction_hash",
      "flat_event_witnesses",
      "internal_contract_id",
    )

  private val baseColumnsForFlatTransactionsExercise =
    Seq(
      "event_offset",
      "update_id",
      "node_id",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "contract_id",
      "template_id",
      "package_id",
      "NULL as representative_package_id",
      "NULL as create_argument",
      "NULL as create_argument_compression",
      "NULL as create_signatories",
      "NULL as create_observers",
      "NULL as create_key_value",
      "NULL as create_key_hash",
      "NULL as create_key_value_compression",
      "NULL as create_key_maintainers",
      "submitters",
      "NULL as authentication_data",
      "synchronizer_id",
      "trace_context",
      "record_time",
      "external_transaction_hash",
      "flat_event_witnesses",
      "NULL as internal_contract_id",
    )

  val selectColumnsForFlatTransactionsCreate: String =
    baseColumnsForFlatTransactionsCreate.mkString(", ")

  val selectColumnsForFlatTransactionsExercise: String =
    baseColumnsForFlatTransactionsExercise.mkString(", ")

  private type SharedRow =
    Long ~ UpdateId ~ Int ~ Long ~ ContractId ~ Timestamp ~ Int ~ Int ~ Option[String] ~
      Option[String] ~ Seq[Party] ~ Option[Seq[Party]] ~ Int ~ Option[Array[Byte]] ~ Timestamp ~
      Option[Array[Byte]]

  private def sharedRow(stringInterning: StringInterning): RowParser[SharedRow] =
    long("event_offset") ~
      updateId("update_id") ~
      int("node_id") ~
      long("event_sequential_id") ~
      contractId("contract_id") ~
      timestampFromMicros("ledger_effective_time") ~
      int("template_id") ~
      int("package_id") ~
      str("command_id").? ~
      str("workflow_id").? ~
      parties(stringInterning)("event_witnesses") ~
      parties(stringInterning)("submitters").? ~
      int("synchronizer_id") ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time") ~
      byteArray("external_transaction_hash").?

  private type CreatedEventRow =
    SharedRow ~ Array[Byte] ~ Option[Int] ~ Seq[Party] ~ Seq[Party] ~
      Option[Array[Byte]] ~ Option[Hash] ~ Option[Int] ~ Option[Seq[Party]] ~
      Array[Byte] ~ Seq[Party] ~ Int ~ Long

  private def createdEventRow(stringInterning: StringInterning): RowParser[CreatedEventRow] =
    sharedRow(stringInterning) ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      parties(stringInterning)("create_signatories") ~
      parties(stringInterning)("create_observers") ~
      byteArray("create_key_value").? ~
      hashFromHexString("create_key_hash").? ~
      int("create_key_value_compression").? ~
      parties(stringInterning)("create_key_maintainers").? ~
      byteArray("authentication_data") ~
      parties(stringInterning)("flat_event_witnesses") ~
      int("representative_package_id") ~
      long("internal_contract_id")

  private type ExercisedEventRow =
    SharedRow ~ Boolean ~ Int ~ Option[Int] ~ Array[Byte] ~ Option[Int] ~
      Option[Array[Byte]] ~ Option[Int] ~ Seq[Party] ~ Int ~ Option[Seq[Party]]

  private def exercisedEventRow(stringInterning: StringInterning): RowParser[ExercisedEventRow] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    sharedRow(stringInterning) ~
      bool("exercise_consuming") ~
      int("exercise_choice") ~
      int("exercise_choice_interface").? ~
      byteArray("exercise_argument") ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      parties(stringInterning)("exercise_actors") ~
      int("exercise_last_descendant_node_id") ~
      parties(stringInterning)("flat_event_witnesses").?
  }

  private type ArchiveEventRow = SharedRow

  private def archivedEventRow(stringInterning: StringInterning): RowParser[ArchiveEventRow] =
    sharedRow(stringInterning)

  private[common] def createdEventParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawCreatedEventLegacy]] =
    createdEventRow(stringInterning) map {
      case offset ~
          updateId ~
          nodeId ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageId ~
          commandId ~
          workflowId ~
          eventWitnesses ~
          submitters ~
          internedSynchronizerId ~
          traceContext ~
          recordTime ~
          externalTransactionHash ~
          createArgument ~
          createArgumentCompression ~
          createSignatories ~
          createObservers ~
          createKeyValue ~
          createKeyHash ~
          createKeyValueCompression ~
          createKeyMaintainers ~
          authenticationData ~
          flatEventWitnesses ~
          representativePackageId ~
          internalContractId =>
        Entry(
          offset = offset,
          nodeId = nodeId,
          updateId = updateId.toHexString,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = Some(ledgerEffectiveTime),
          commandId = filteredCommandId(commandId, submitters, allQueryingPartiesO),
          workflowId = workflowId,
          event = RawCreatedEventLegacy(
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            representativePackageId =
              stringInterning.packageId.externalize(representativePackageId),
            witnessParties = filterWitnesses(
              allQueryingPartiesO,
              eventWitnesses,
            ),
            flatEventWitnesses = filterWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
            ),
            signatories = createSignatories.toSet,
            observers = createObservers.toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            createKeyMaintainers = createKeyMaintainers.map(_.toSet[String]).getOrElse(Set.empty),
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            authenticationData = authenticationData,
            internalContractId = internalContractId,
          ),
          synchronizerId =
            stringInterning.synchronizerId.unsafe.externalize(internedSynchronizerId),
          traceContext = traceContext,
          recordTime = recordTime,
          externalTransactionHash = externalTransactionHash,
        )
    }

  private[common] def archivedEventParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawArchivedEventLegacy]] =
    archivedEventRow(stringInterning) map {
      case eventOffset ~
          updateId ~
          nodeId ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageId ~
          commandId ~
          workflowId ~
          flatEventWitnesses ~
          submitters ~
          internedSynchronizerId ~
          traceContext ~
          recordTime ~
          externalTransactionHash =>
        Entry(
          offset = eventOffset,
          nodeId = nodeId,
          updateId = updateId.toHexString,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = Some(ledgerEffectiveTime),
          commandId = filteredCommandId(
            commandId,
            submitters,
            allQueryingPartiesO,
          ),
          workflowId = workflowId,
          synchronizerId =
            stringInterning.synchronizerId.unsafe.externalize(internedSynchronizerId),
          traceContext = traceContext,
          recordTime = recordTime,
          externalTransactionHash = externalTransactionHash,
          event = RawArchivedEventLegacy(
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            witnessParties = filterWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
            ),
          ),
        )
    }

  def rawAcsDeltaEventParser(
      allQueryingParties: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawAcsDeltaEventLegacy]] =
    (createdEventParser(allQueryingParties, stringInterning): RowParser[
      Entry[RawAcsDeltaEventLegacy]
    ]) |
      archivedEventParser(allQueryingParties, stringInterning)

  private def exercisedEventParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawExercisedEventLegacy]] =
    exercisedEventRow(stringInterning) map {
      case eventOffset ~
          updateId ~
          nodeId ~
          eventSequentialId ~
          contractId ~
          ledgerEffectiveTime ~
          templateId ~
          packageId ~
          commandId ~
          workflowId ~
          treeEventWitnesses ~
          submitters ~
          internedSynchronizerId ~
          traceContext ~
          recordTime ~
          externalTransactionHash ~
          exerciseConsuming ~
          choiceName ~
          choiceInterface ~
          exerciseArgument ~
          exerciseArgumentCompression ~
          exerciseResult ~
          exerciseResultCompression ~
          exerciseActors ~
          exerciseLastDescendantNodeId ~
          flatEventWitnesses =>
        Entry(
          offset = eventOffset,
          nodeId = nodeId,
          updateId = updateId.toHexString,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = Some(ledgerEffectiveTime),
          commandId = filteredCommandId(
            commandId,
            submitters,
            allQueryingPartiesO,
          ),
          workflowId = workflowId,
          synchronizerId =
            stringInterning.synchronizerId.unsafe.externalize(internedSynchronizerId),
          traceContext = traceContext,
          recordTime = recordTime,
          externalTransactionHash = externalTransactionHash,
          event = RawExercisedEventLegacy(
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = stringInterning.choiceName.externalize(choiceName),
            exerciseChoiceInterface = choiceInterface.map { interfaceId =>
              stringInterning.interfaceId.externalize(interfaceId)
            },
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors = exerciseActors,
            exerciseLastDescendantNodeId = exerciseLastDescendantNodeId,
            witnessParties = filterWitnesses(
              allQueryingPartiesO,
              treeEventWitnesses,
            ),
            flatEventWitnesses = filterWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses.getOrElse(Seq.empty),
            ),
          ),
        )
    }

  def rawLedgerEffectsEventParser(
      allQueryingParties: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawLedgerEffectsEventLegacy]] =
    (createdEventParser(allQueryingParties, stringInterning): RowParser[
      Entry[RawLedgerEffectsEventLegacy]
    ]) |
      exercisedEventParser(allQueryingParties, stringInterning)

  val selectColumnsForTransactionTreeCreate: String = Seq(
    "event_offset",
    "update_id",
    "node_id",
    "event_sequential_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "package_id",
    "representative_package_id",
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
    "NULL as exercise_choice_interface",
    "NULL as exercise_argument",
    "NULL as exercise_argument_compression",
    "NULL as exercise_result",
    "NULL as exercise_result_compression",
    "NULL as exercise_actors",
    "NULL as exercise_last_descendant_node_id",
    "submitters",
    "authentication_data",
    "synchronizer_id",
    "trace_context",
    "record_time",
    "external_transaction_hash",
    "flat_event_witnesses",
    "internal_contract_id",
  ).mkString(", ")

  def selectColumnsForTransactionTreeExercise(includeFlatEventWitnesses: Boolean): String =
    Seq(
      "event_offset",
      "update_id",
      "node_id",
      "event_sequential_id",
      "contract_id",
      "ledger_effective_time",
      "template_id",
      "package_id",
      "NULL as representative_package_id",
      "workflow_id",
      "NULL as create_argument",
      "NULL as create_argument_compression",
      "NULL as create_signatories",
      "NULL as create_observers",
      "NULL as create_key_value",
      "NULL as create_key_hash",
      "NULL as create_key_value_compression",
      "NULL as create_key_maintainers",
      "exercise_choice",
      "exercise_choice_interface",
      "exercise_argument",
      "exercise_argument_compression",
      "exercise_result",
      "exercise_result_compression",
      "exercise_actors",
      "exercise_last_descendant_node_id",
      "submitters",
      "NULL as authentication_data",
      "synchronizer_id",
      "trace_context",
      "record_time",
      "external_transaction_hash",
      (if (includeFlatEventWitnesses) "" else "NULL as ") + "flat_event_witnesses",
      "NULL as internal_contract_id",
    ).mkString(", ")

  val EventSequentialIdFirstLast: RowParser[(Long, Long)] =
    long("event_sequential_id_first") ~ long("event_sequential_id_last") map {
      case event_sequential_id_first ~ event_sequential_id_last =>
        (event_sequential_id_first, event_sequential_id_last)
    }

  val partyToParticipantEventRow =
    long("event_sequential_id") ~
      offset("event_offset") ~
      updateId("update_id") ~
      int("party_id") ~
      int("participant_id") ~
      authorizationEventParser("participant_permission", "participant_authorization_event") ~
      int("synchronizer_id") ~
      timestampFromMicros("record_time") ~
      byteArray("trace_context").?

  def partyToParticipantEventParser(
      stringInterning: StringInterning
  ): RowParser[EventStorageBackend.RawParticipantAuthorization] =
    partyToParticipantEventRow map {
      case _ ~
          eventOffset ~
          updateId ~
          partyId ~
          participantId ~
          authorizationEvent ~
          synchronizerId ~
          recordTime ~
          traceContext =>
        EventStorageBackend.RawParticipantAuthorization(
          offset = eventOffset,
          updateId = updateId.toHexString,
          partyId = stringInterning.party.unsafe.externalize(partyId),
          participantId = stringInterning.participantId.unsafe.externalize(participantId),
          authorizationEvent = authorizationEvent,
          recordTime = recordTime,
          synchronizerId = stringInterning.synchronizerId.unsafe.externalize(synchronizerId),
          traceContext = traceContext,
        )
    }

  def assignEventRow(stringInterning: StringInterning) =
    str("command_id").? ~
      str("workflow_id").? ~
      long("event_offset") ~
      int("source_synchronizer_id") ~
      int("target_synchronizer_id") ~
      byteArray("reassignment_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      updateId("update_id") ~
      contractId("contract_id") ~
      int("template_id") ~
      int("package_id") ~
      parties(stringInterning)("flat_event_witnesses") ~
      parties(stringInterning)("create_signatories") ~
      parties(stringInterning)("create_observers") ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      parties(stringInterning)("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("authentication_data") ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time") ~
      long("event_sequential_id") ~
      int("node_id") ~
      long("internal_contract_id")

  private def assignEventParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawAssignEventLegacy]] =
    assignEventRow(stringInterning) map {
      case commandId ~
          workflowId ~
          offset ~
          sourceSynchronizerId ~
          targetSynchronizerId ~
          reassignmentId ~
          submitter ~
          reassignmentCounter ~
          updateId ~
          contractId ~
          templateId ~
          packageId ~
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
          authenticationData ~
          traceContext ~
          recordTime ~
          eventSequentialId ~
          nodeId ~
          internalContractId =>
        val witnessParties = filterWitnesses(
          allQueryingPartiesO,
          flatEventWitnesses,
        )
        Entry(
          offset = offset,
          nodeId = nodeId,
          updateId = updateId.toHexString,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = None, // Not applicable
          commandId = filteredCommandId(
            commandId,
            submitter.map(s => Seq(stringInterning.party.externalize(s))),
            allQueryingPartiesO,
          ),
          workflowId = workflowId,
          synchronizerId = stringInterning.synchronizerId.unsafe.externalize(targetSynchronizerId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawAssignEventLegacy(
            sourceSynchronizerId =
              stringInterning.synchronizerId.unsafe.externalize(sourceSynchronizerId),
            targetSynchronizerId =
              stringInterning.synchronizerId.unsafe.externalize(targetSynchronizerId),
            reassignmentId = ReassignmentId.assertFromBytes(reassignmentId).toProtoPrimitive,
            submitter = submitter.map(stringInterning.party.unsafe.externalize),
            reassignmentCounter = reassignmentCounter,
            rawCreatedEvent = RawCreatedEventLegacy(
              contractId = contractId,
              templateId = stringInterning.templateId
                .externalize(templateId)
                .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
              // TODO(#27872): Use the assignment representative package ID when available
              representativePackageId = stringInterning.packageId.externalize(packageId),
              witnessParties = witnessParties,
              flatEventWitnesses = witnessParties,
              signatories = createSignatories.toSet,
              observers = createObservers.toSet,
              createArgument = createArgument,
              createArgumentCompression = createArgumentCompression,
              createKeyMaintainers = createKeyMaintainers.map(_.toSet[String]).getOrElse(Set.empty),
              createKeyValue = createKeyValue,
              createKeyValueCompression = createKeyValueCompression,
              ledgerEffectiveTime = ledgerEffectiveTime,
              createKeyHash = createKeyHash,
              authenticationData = authenticationData,
              internalContractId = internalContractId,
            ),
          ),
          // TODO(i26562) Assignments are not externally signed
          externalTransactionHash = None,
        )
    }

  def unassignEventRow(stringInterning: StringInterning) =
    str("command_id").? ~
      str("workflow_id").? ~
      long("event_offset") ~
      int("source_synchronizer_id") ~
      int("target_synchronizer_id") ~
      byteArray("reassignment_id") ~
      int("submitter").? ~
      long("reassignment_counter") ~
      updateId("update_id") ~
      contractId("contract_id") ~
      int("template_id") ~
      int("package_id") ~
      parties(stringInterning)("flat_event_witnesses") ~
      timestampFromMicros("assignment_exclusivity").? ~
      byteArray("trace_context").? ~
      timestampFromMicros("record_time") ~
      long("event_sequential_id") ~
      int("node_id")

  private def unassignEventParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[Entry[RawUnassignEventLegacy]] =
    unassignEventRow(stringInterning) map {
      case commandId ~
          workflowId ~
          offset ~
          sourceSynchronizerId ~
          targetSynchronizerId ~
          reassignmentId ~
          submitter ~
          reassignmentCounter ~
          updateId ~
          contractId ~
          templateId ~
          packageId ~
          flatEventWitnesses ~
          assignmentExclusivity ~
          traceContext ~
          recordTime ~
          eventSequentialId ~
          nodeId =>
        Entry(
          offset = offset,
          nodeId = nodeId,
          updateId = updateId.toHexString,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = None, // Not applicable
          commandId = filteredCommandId(
            commandId,
            submitter.map(s => Seq(stringInterning.party.externalize(s))),
            allQueryingPartiesO,
          ),
          workflowId = workflowId,
          synchronizerId = stringInterning.synchronizerId.unsafe.externalize(sourceSynchronizerId),
          traceContext = traceContext,
          recordTime = recordTime,
          event = RawUnassignEventLegacy(
            sourceSynchronizerId =
              stringInterning.synchronizerId.unsafe.externalize(sourceSynchronizerId),
            targetSynchronizerId =
              stringInterning.synchronizerId.unsafe.externalize(targetSynchronizerId),
            reassignmentId = ReassignmentId.assertFromBytes(reassignmentId).toProtoPrimitive,
            submitter = submitter.map(stringInterning.party.unsafe.externalize),
            reassignmentCounter = reassignmentCounter,
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            witnessParties = filterWitnesses(
              allQueryingPartiesO,
              flatEventWitnesses,
            ),
            assignmentExclusivity = assignmentExclusivity,
          ),
          // TODO(i26562) Unassignments are not externally signed
          externalTransactionHash = None,
        )
    }

  def assignActiveContractRow(stringInterning: StringInterning) =
    str("workflow_id").? ~
      int("target_synchronizer_id") ~
      long("reassignment_counter") ~
      updateId("update_id") ~
      long("event_offset") ~
      contractId("contract_id") ~
      int("template_id") ~
      int("package_id") ~
      parties(stringInterning)("flat_event_witnesses") ~
      parties(stringInterning)("create_signatories") ~
      parties(stringInterning)("create_observers") ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      parties(stringInterning)("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("authentication_data") ~
      long("event_sequential_id") ~
      int("node_id") ~
      long("internal_contract_id")

  private def assignActiveContractParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[RawActiveContractLegacy] =
    assignActiveContractRow(stringInterning) map {
      case workflowId ~
          targetSynchronizerId ~
          reassignmentCounter ~
          updateId ~
          offset ~
          contractId ~
          templateId ~
          packageId ~
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
          authenticationData ~
          eventSequentialId ~
          nodeId ~
          internalContractId =>
        val witnessParties = filterWitnesses(
          allQueryingPartiesO,
          flatEventWitnesses,
        )
        RawActiveContractLegacy(
          workflowId = workflowId,
          synchronizerId = stringInterning.synchronizerId.unsafe.externalize(targetSynchronizerId),
          reassignmentCounter = reassignmentCounter,
          rawCreatedEvent = RawCreatedEventLegacy(
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            // TODO(#27872): Use the assignment representative package ID when available
            representativePackageId = stringInterning.packageId.externalize(packageId),
            witnessParties = witnessParties,
            flatEventWitnesses = witnessParties,
            signatories = createSignatories.toSet,
            observers = createObservers.toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            createKeyMaintainers = createKeyMaintainers.map(_.toSet[String]).getOrElse(Set.empty),
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            authenticationData = authenticationData,
            internalContractId = internalContractId,
          ),
          eventSequentialId = eventSequentialId,
          nodeId = nodeId,
          offset = offset,
        )
    }

  def createActiveContractRow(stringInterning: StringInterning) =
    str("workflow_id").? ~
      int("synchronizer_id") ~
      updateId("update_id") ~
      long("event_offset") ~
      contractId("contract_id") ~
      int("template_id") ~
      int("package_id") ~
      int("representative_package_id") ~
      parties(stringInterning)("flat_event_witnesses") ~
      parties(stringInterning)("create_signatories") ~
      parties(stringInterning)("create_observers") ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      parties(stringInterning)("create_key_maintainers").? ~
      timestampFromMicros("ledger_effective_time") ~
      hashFromHexString("create_key_hash").? ~
      byteArray("authentication_data") ~
      long("event_sequential_id") ~
      int("node_id") ~
      long("internal_contract_id")

  private def createActiveContractParser(
      allQueryingPartiesO: Option[Set[Party]],
      stringInterning: StringInterning,
  ): RowParser[RawActiveContractLegacy] =
    createActiveContractRow(stringInterning) map {
      case workflowId ~
          targetSynchronizerId ~
          updateId ~
          offset ~
          contractId ~
          templateId ~
          packageId ~
          representativePackageId ~
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
          authenticationData ~
          eventSequentialId ~
          nodeId ~
          internalContractId =>
        val witnessParties = filterWitnesses(
          allQueryingPartiesO,
          flatEventWitnesses,
        )
        RawActiveContractLegacy(
          workflowId = workflowId,
          synchronizerId = stringInterning.synchronizerId.unsafe.externalize(targetSynchronizerId),
          reassignmentCounter = 0L, // zero for create
          rawCreatedEvent = RawCreatedEventLegacy(
            contractId = contractId,
            templateId = stringInterning.templateId
              .externalize(templateId)
              .toFullIdentifier(stringInterning.packageId.externalize(packageId)),
            representativePackageId =
              stringInterning.packageId.externalize(representativePackageId),
            witnessParties = witnessParties,
            flatEventWitnesses = witnessParties,
            signatories = createSignatories.toSet,
            observers = createObservers.toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createKeyMaintainers = createKeyMaintainers.map(_.toSet[String]).getOrElse(Set.empty),
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyHash = createKeyHash,
            authenticationData = authenticationData,
            internalContractId = internalContractId,
          ),
          eventSequentialId = eventSequentialId,
          nodeId = nodeId,
          offset = offset,
        )
    }

  private def filterWitnesses(
      allQueryingPartiesO: Option[Set[Party]],
      witnesses: Seq[Party],
  ): Set[String] =
    allQueryingPartiesO
      .fold(witnesses)(allQueryingParties =>
        witnesses
          .filter(allQueryingParties)
      )
      .toSet

  private def filteredCommandId(
      commandId: Option[String],
      submitters: Option[Seq[Party]],
      allQueryingPartiesO: Option[Set[Party]],
  ): Option[String] = {
    def submittersInQueryingParties: Boolean = allQueryingPartiesO match {
      case Some(allQueryingParties) =>
        submitters.getOrElse(Seq.empty).exists(allQueryingParties)
      case None => submitters.nonEmpty
    }
    commandId.filter(_ != "" && submittersInQueryingParties)
  }

  private def synchronizerOffsetParser(
      offsetColumnName: String,
      stringInterning: StringInterning,
  ): RowParser[SynchronizerOffset] =
    offset(offsetColumnName) ~
      int("synchronizer_id") ~
      timestampFromMicros("record_time") ~
      timestampFromMicros("publication_time") map {
        case offset ~ internedSynchronizerId ~ recordTime ~ publicationTime =>
          SynchronizerOffset(
            offset = offset,
            synchronizerId = stringInterning.synchronizerId.externalize(internedSynchronizerId),
            recordTime = recordTime,
            publicationTime = publicationTime,
          )
      }

  private def completionSynchronizerOffsetParser(
      stringInterning: StringInterning
  ): RowParser[SynchronizerOffset] =
    synchronizerOffsetParser("completion_offset", stringInterning)

  private def metaSynchronizerOffsetParser(
      stringInterning: StringInterning
  ): RowParser[SynchronizerOffset] =
    synchronizerOffsetParser("event_offset", stringInterning)

}

abstract class EventStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    val loggerFactory: NamedLoggerFactory,
) extends EventStorageBackend
    with NamedLogging {
  import EventStorageBackendTemplate.*
  import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

  override def updatePointwiseQueries: UpdatePointwiseQueries =
    new UpdatePointwiseQueries(ledgerEndCache)

  override def updateStreamingQueries: UpdateStreamingQueries =
    new UpdateStreamingQueries(stringInterning, queryStrategy)

  override def eventReaderQueries: EventReaderQueries =
    new EventReaderQueries(stringInterning)

  // Improvement idea: Implement pruning queries in terms of event sequential id in order to be able to drop offset based indices.
  /** Deletes a subset of the indexed data (up to the pruning offset) in the following order and in
    * the manner specified:
    *   1. entries from filter for create stakeholders for which there is an archive for the
    *      corresponding create event or the corresponding create event is immediately divulged,
    *   1. entries from filter for create non-stakeholder informees for which there is an archive
    *      for the corresponding create event or the corresponding create event is immediately
    *      divulged,
    *   1. all entries from filter for consuming stakeholders,
    *   1. all entries from filter for consuming non-stakeholders informees,
    *   1. all entries from filter for non-consuming informees,
    *   1. create events table for which there is an archive event or create events which did not
    *      have a locally hosted party before their creation offset (immediate divulgence),
    *   1. all consuming events,
    *   1. all non-consuming events,
    *   1. transaction meta entries for which there exists at least one create event.
    */
  override def pruneEventsLegacy(
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit connection: Connection, traceContext: TraceContext): Unit = {
    val _ =
      SQL"""
          -- Create temporary table for storing incomplete reassignment offsets
          CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS temp_incomplete_reassignment_offsets (
            incomplete_offset bigint PRIMARY KEY NOT NULL
          ) ON COMMIT DELETE ROWS
          """.execute()
    val incompleteOffsetBatches = incompleteReassignmentOffsets.distinct
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
      s"Populated temp_incomplete_reassignment_offsets table with ${incompleteReassignmentOffsets.size} entries"
    )

    pruneIdFilterTables(pruneUpToInclusive)

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (for contracts deactivated or immediately divulged before the specified offset)
          delete from lapi_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            ${createIsPrunable("delete_events", pruneUpToInclusive)}"""
    }

    pruneWithLogging(queryDescription = "Assign events pruning") {
      SQL"""
          -- Assigned events
          delete from lapi_events_assign delete_events
          where
            -- do not prune incomplete
            ${reassignmentIsNotIncomplete("delete_events")}
            -- only prune if it is archived in same synchronizer, or unassigned later in the same synchronizer
            and ${assignIsPrunable("delete_events", pruneUpToInclusive)}
            and delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from lapi_events_consuming_exercise delete_events
          where
            -- do not prune if it is preceded in the same synchronizer by an incomplete assign
            -- this is needed so that incomplete assign is not resulting in an active contract
            ${deactivationIsNotDirectlyPrecededByIncompleteAssign(
          "delete_events",
          "synchronizer_id",
        )}
            and delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from lapi_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }

    pruneWithLogging(queryDescription = "Update Meta pruning") {
      SQL"""
           DELETE FROM
              lapi_update_meta m
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
            -- do not prune if it is preceded in the same synchronizer by an incomplete assign
            -- this is needed so that incomplete assign is not resulting in an active contract
            and ${deactivationIsNotDirectlyPrecededByIncompleteAssign(
          "delete_events",
          "source_synchronizer_id",
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
              AND ${createIsPrunable("c", pruneUpToInclusive)}
              AND c.event_sequential_id = id_filter.event_sequential_id
            )"""

    // Improvement idea:
    // In order to prune an id filter table we query an events table to discover
    // the event offset corresponding.
    // This query can be simplified not to query the events table at all
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
              ${deactivationIsNotDirectlyPrecededByIncompleteAssign("events", "synchronizer_id")}
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
              AND ${assignIsPrunable("assign", pruneUpToInclusive)}
              AND ${reassignmentIsNotIncomplete("assign")}
              AND assign.event_sequential_id = id_filter.event_sequential_id
          )"""
    }
    pruneWithLogging("Pruning id filter unassign stakeholder table") {
      SQL"""
          DELETE FROM lapi_pe_reassignment_id_filter_stakeholder id_filter
          WHERE EXISTS (
            SELECT * from lapi_events_unassign unassign
            WHERE
            unassign.event_offset <= $pruneUpToInclusive
            AND ${reassignmentIsNotIncomplete("unassign")}
            AND ${deactivationIsNotDirectlyPrecededByIncompleteAssign(
          "unassign",
          "source_synchronizer_id",
        )}
            AND unassign.event_sequential_id = id_filter.event_sequential_id
          )"""
    }
  }

  private def createIsPrunable(
      createEventTableName: String,
      pruneUpToInclusive: Offset,
  ): CompositeSql =
    cSQL"""
          ((${eventIsArchivedOrUnassigned(
        createEventTableName,
        pruneUpToInclusive,
        "synchronizer_id",
      )}
          and ${activationIsNotDirectlyFollowedByIncompleteUnassign(
        createEventTableName,
        "synchronizer_id",
        pruneUpToInclusive,
      )}) or length(#$createEventTableName.flat_event_witnesses) <= 1)
          """

  private def assignIsPrunable(
      assignEventTableName: String,
      pruneUpToInclusive: Offset,
  ): CompositeSql =
    cSQL"""
      ${eventIsArchivedOrUnassigned(
        assignEventTableName,
        pruneUpToInclusive,
        "target_synchronizer_id",
      )}
      and ${activationIsNotDirectlyFollowedByIncompleteUnassign(
        assignEventTableName,
        "target_synchronizer_id",
        pruneUpToInclusive,
      )}
      """

  private def eventIsArchivedOrUnassigned(
      eventTableName: String,
      pruneUpToInclusive: Offset,
      eventSynchronizerName: String,
  ): CompositeSql =
    cSQL"""
          (
            exists (
              SELECT 1 FROM lapi_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive
                -- please note: this is the only indexed constraint, this is enough since there can be at most one archival
                AND archive_events.contract_id = #$eventTableName.contract_id
                AND archive_events.synchronizer_id = #$eventTableName.#$eventSynchronizerName
            )
            or
            exists (
              SELECT 1 FROM lapi_events_unassign unassign_events
              WHERE
                unassign_events.event_offset <= $pruneUpToInclusive
                AND unassign_events.contract_id = #$eventTableName.contract_id
                AND unassign_events.source_synchronizer_id = #$eventTableName.#$eventSynchronizerName
                -- with this constraint the index (contract_id, synchronizer_id, event_sequential_id) can be used
                -- and what we only need is one unassign later in the same synchronizer
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

  // the not exists (select where in (select limit 1)) construction is the one which is compatible with H2
  // other similar constructions with CTE/subqueries are working just fine with PG but not with H2 due
  // to some impediment/bug not being able to recognize references to the deactivationTableName (it is also
  // an experimental feature in H2)
  // in case the PG version produces inefficient plans, the implementation need to be made polymorphic accordingly
  // authors hope is that the in (select limit 1) clause will be materialized only once due to no relation to the
  // incomplete temp table
  // Please note! The limit clause is essential, otherwise contracts which move frequently across synchronizers can
  // cause quadratic increase in query cost.
  private def deactivationIsNotDirectlyPrecededByIncompleteAssign(
      deactivationTableName: String,
      deactivationSynchronizerColumnName: String,
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
                  AND assign_events.target_synchronizer_id = #$deactivationTableName.#$deactivationSynchronizerColumnName
                  AND assign_events.event_sequential_id < #$deactivationTableName.event_sequential_id
                ORDER BY event_sequential_id DESC
                ${QueryStrategy.limitClause(Some(1))}
              )
          )"""
  private def activationIsNotDirectlyFollowedByIncompleteUnassign(
      activationTableName: String,
      activationSynchronizerColumnName: String,
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
                  -- this one is backed by a (contract_id, synchronizer_id, event_sequential_id) index
                  unassign_events.contract_id = #$activationTableName.contract_id
                  AND unassign_events.source_synchronizer_id = #$activationTableName.#$activationSynchronizerColumnName
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
        lapi_update_meta
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

  override def assignEventBatchLegacy(
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawAssignEventLegacy]] =
    SQL"""
        SELECT *
        FROM lapi_events_assign assign_evs
        WHERE ${queryStrategy.inBatch("assign_evs.event_sequential_id", eventSequentialIds)}
        ORDER BY assign_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(fetchSize(eventSequentialIds)))
      .asVectorOf(assignEventParser(allFilterParties, stringInterning))(connection)

  override def unassignEventBatchLegacy(
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawUnassignEventLegacy]] =
    SQL"""
          SELECT *
          FROM lapi_events_unassign unassign_evs
          WHERE ${queryStrategy.inBatch("unassign_evs.event_sequential_id", eventSequentialIds)}
          ORDER BY unassign_evs.event_sequential_id -- deliver in index order
          """
      .withFetchSize(Some(fetchSize(eventSequentialIds)))
      .asVectorOf(unassignEventParser(allFilterParties, stringInterning))(connection)

  override def activeContractBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawThinActiveContract] =
    RowDefs
      .rawThinActiveContractParser(stringInterning, allFilterParties)
      .queryMultipleRows(columns =>
        SQL"""
        SELECT $columns
        FROM lapi_events_activate_contract
        WHERE
          event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY event_sequential_id -- deliver in index order
        """
          .withFetchSize(Some(eventSequentialIds.size))
      )(connection)

  override def activeContractAssignEventBatchLegacy(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContractLegacy] =
    SQL"""
        SELECT *
        FROM lapi_events_assign assign_evs
        WHERE
          assign_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY assign_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(assignActiveContractParser(allFilterParties, stringInterning))(connection)

  override def activeContractCreateEventBatchLegacy(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContractLegacy] =
    SQL"""
        SELECT *
        FROM lapi_events_create create_evs
        WHERE
          create_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY create_evs.event_sequential_id -- deliver in index order
        """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(createActiveContractParser(allFilterParties, stringInterning))(connection)

  override def fetchAssignEventIdsForStakeholderLegacy(
      stakeholderO: Option[Party],
      templateId: Option[NameTypeConRef],
  )(connection: Connection): PaginationInput => Vector[Long] =
    UpdateStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_assign_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateId,
      idFilter = None,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = true,
    )(connection)

  override def fetchUnassignEventIdsForStakeholderLegacy(
      stakeholderO: Option[Party],
      templateId: Option[NameTypeConRef],
  )(connection: Connection): PaginationInput => Vector[Long] =
    UpdateStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_reassignment_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateId,
      idFilter = None,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = true,
    )(connection)

  override def lookupAssignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_activate_contract
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupAssignSequentialIdByOffsetLegacy(
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
        FROM lapi_events_deactivate_contract
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupUnassignSequentialIdByOffsetLegacy(
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

  // it is called with a list of tuple of search parameters for the contract, the synchronizer and the sequential ids
  // it finds the sequential id of the assign that has the same contract and synchronizer ids and has the largest
  // sequential id < sequential id given
  // it returns the mapping from the tuple of the search parameters to the corresponding sequential id (if exists)
  override def lookupAssignSequentialIdByLegacy(
      unassignProperties: Iterable[UnassignProperties]
  )(connection: Connection): Map[UnassignProperties, Long] =
    unassignProperties.flatMap {
      case params @ UnassignProperties(contractId, synchronizerIdString, sequentialId) =>
        val synchronizerIdO = SynchronizerId
          .fromString(synchronizerIdString)
          .toOption
          .flatMap(stringInterning.synchronizerId.tryInternalize)
        synchronizerIdO match {
          case Some(synchronizerId) =>
            SQL"""
              SELECT MAX(assign_evs.event_sequential_id) AS event_sequential_id
              FROM lapi_events_assign assign_evs
              WHERE assign_evs.contract_id = ${contractId.toBytes.toByteArray}
              AND assign_evs.target_synchronizer_id = $synchronizerId
              AND assign_evs.event_sequential_id < $sequentialId
            """
              .asSingle(long("event_sequential_id").?)(connection)
              .map((params, _))
          case None => None
        }
    }.toMap

  override def lookupCreateSequentialIdByContractIdLegacy(
      contractIds: Iterable[ContractId]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_create
        WHERE
          contract_id ${queryStrategy.anyOfBinary(contractIds.map(_.toBytes.toByteArray))}
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def firstSynchronizerOffsetAfterOrAt(
      synchronizerId: SynchronizerId,
      afterOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[SynchronizerOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY synchronizer_id ASC, record_time ASC, completion_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY synchronizer_id ASC, record_time ASC, event_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten
      .minByOption(_.recordTime)
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if the first is after LedgerEnd, then we have none

  override def lastSynchronizerOffsetBeforeOrAt(
      synchronizerIdO: Option[SynchronizerId],
      beforeOrAtOffsetInclusive: Offset,
  )(connection: Connection): Option[SynchronizerOffset] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)
    val safeBeforeOrAtOffset =
      if (Option(beforeOrAtOffsetInclusive) > ledgerEndOffset) ledgerEndOffset
      else Some(beforeOrAtOffsetInclusive)
    val (synchronizerIdFilter, synchronizerIdOrdering) = synchronizerIdO match {
      case Some(synchronizerId) =>
        (
          cSQL"synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND",
          cSQL"synchronizer_id,",
        )

      case None =>
        (
          cSQL"",
          cSQL"",
        )
    }
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            $synchronizerIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("completion_offset", safeBeforeOrAtOffset)}
          ORDER BY $synchronizerIdOrdering completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            $synchronizerIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("event_offset", safeBeforeOrAtOffset)}
          ORDER BY $synchronizerIdOrdering event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  // Note: Added for offline party replication as CN is using it.
  override def lastSynchronizerOffsetBeforeOrAtRecordTime(
      synchronizerId: SynchronizerId,
      beforeOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[SynchronizerOffset] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time <= ${beforeOrAtRecordTimeInclusive.micros} AND
            ${QueryStrategy.offsetIsLessOrEqual("completion_offset", ledgerEndOffset)}
          ORDER BY synchronizer_id DESC, record_time DESC, completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time <= ${beforeOrAtRecordTimeInclusive.micros} AND
            ${QueryStrategy.offsetIsLessOrEqual("event_offset", ledgerEndOffset)}
          ORDER BY synchronizer_id DESC, record_time DESC, event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  override def synchronizerOffset(offset: Offset)(
      connection: Connection
  ): Option[SynchronizerOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            completion_offset = $offset
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            event_offset = $offset
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten.headOption // if both present they should be the same
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // only offset allow before or at ledger end

  override def firstSynchronizerOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset] =
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, completion_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, event_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten
      .minByOption(_.offset)
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if first offset is beyond the ledger-end then we have no such

  override def lastSynchronizerOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset] = {
    val ledgerEndPublicationTime =
      ledgerEndCache().map(_.lastPublicationTime).getOrElse(CantonTimestamp.MinValue).underlying
    val safePublicationTime =
      if (beforeOrAtPublicationTimeInclusive > ledgerEndPublicationTime)
        ledgerEndPublicationTime
      else
        beforeOrAtPublicationTimeInclusive
    List(
      SQL"""
          SELECT completion_offset, record_time, publication_time, synchronizer_id
          FROM lapi_command_completions
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(completionSynchronizerOffsetParser(stringInterning))(connection),
      SQL"""
          SELECT event_offset, record_time, publication_time, synchronizer_id
          FROM lapi_update_meta
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """.asSingleOpt(metaSynchronizerOffsetParser(stringInterning))(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  override def prunableContracts(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[Long] = {
    val fromExclusiveSeqId =
      fromExclusive
        .map(from => maxEventSequentialId(Some(from))(connection))
        .getOrElse(-1L)
    val toInclusiveSeqId = maxEventSequentialId(Some(toInclusive))(connection)
    val archivals = SQL"""
        SELECT internal_contract_id
        FROM lapi_events_deactivate_contract
        WHERE
          event_sequential_id > $fromExclusiveSeqId AND
          event_sequential_id <= $toInclusiveSeqId AND
          event_type = ${PersistentEventType.ConsumingExercise.asInt}
        """
      .asVectorOf(long("internal_contract_id").?)(connection)
    val divulgedAndTransientContracts = SQL"""
        SELECT internal_contract_id
        FROM lapi_events_various_witnessed
        WHERE
          event_sequential_id > $fromExclusiveSeqId AND
          event_sequential_id <= $toInclusiveSeqId AND
          event_type = ${PersistentEventType.WitnessedCreate.asInt}
        """
      .asVectorOf(long("internal_contract_id").?)(connection)
    archivals.iterator
      .++(divulgedAndTransientContracts.iterator)
      .flatten
      .toSet
  }

  override def archivalsLegacy(fromExclusive: Option[Offset], toInclusive: Offset)(
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
      party: Option[Party]
  )(connection: Connection): PaginationInput => Vector[Long] =
    UpdateStreamingQueries.fetchEventIds(
      tableName = "lapi_events_party_to_participant",
      witnessO = party,
      templateIdO = None,
      idFilter = None,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = false,
    )(connection)

  override def topologyPartyEventBatch(
      eventSequentialIds: SequentialIdBatch
  )(connection: Connection): Vector[EventStorageBackend.RawParticipantAuthorization] =
    SQL"""
          SELECT *
          FROM lapi_events_party_to_participant e
          WHERE ${queryStrategy.inBatch("e.event_sequential_id", eventSequentialIds)}
          ORDER BY e.event_sequential_id -- deliver in index order
          """
      .withFetchSize(Some(fetchSize(eventSequentialIds)))
      .asVectorOf(partyToParticipantEventParser(stringInterning))(connection)

  override def topologyEventOffsetPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(connection: Connection): Option[Offset] =
    stringInterning.synchronizerId
      .tryInternalize(synchronizerId)
      .flatMap(synchronizerInternedId =>
        SQL"""
          SELECT event_offset
          FROM lapi_events_party_to_participant
          WHERE record_time = ${recordTime.toMicros}
                AND synchronizer_id = $synchronizerInternedId
          ORDER BY synchronizer_id ASC, record_time ASC
          ${QueryStrategy.limitClause(Some(1))}
          """
          .asVectorOf(offset("event_offset"))(connection)
          .headOption
          .filter(offset => Option(offset) <= ledgerEndCache().map(_.lastOffset))
      )

  private def fetchByEventSequentialIds(
      tableName: String,
      eventSequentialIds: SequentialIdBatch,
  )(columns: CompositeSql): SimpleSql[Row] =
    SQL"""
        SELECT $columns
        FROM #$tableName
        WHERE ${queryStrategy.inBatch("event_sequential_id", eventSequentialIds)}
        ORDER BY event_sequential_id
        """.withFetchSize(Some(fetchSize(eventSequentialIds)))

  override def fetchEventPayloadsAcsDelta(target: EventPayloadSourceForUpdatesAcsDelta)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinAcsDeltaEvent] =
    target match {
      case EventPayloadSourceForUpdatesAcsDelta.Activate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.Create -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              witnessIsAcsDelta = true,
              eventIsAcsDelta = true,
            ),
            PersistentEventType.Assign -> RowDefs.rawThinAssignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_activate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesAcsDelta.Deactivate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.ConsumingExercise -> RowDefs.rawArchivedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              acsDelta = true,
            ),
            PersistentEventType.Unassign -> RowDefs.rawUnassignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_deactivate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
    }

  override def fetchEventPayloadsLedgerEffects(target: EventPayloadSourceForUpdatesLedgerEffects)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinLedgerEffectsEvent] =
    target match {
      case EventPayloadSourceForUpdatesLedgerEffects.Activate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.Create -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              witnessIsAcsDelta = false,
              eventIsAcsDelta = true,
            ),
            PersistentEventType.Assign -> RowDefs.rawThinAssignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_activate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.Deactivate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.ConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              eventIsAcsDelta = true,
            ),
            PersistentEventType.Unassign -> RowDefs.rawUnassignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_deactivate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed =>
        RowDefs.eventType
          .branch(
            PersistentEventType.WitnessedCreate -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              witnessIsAcsDelta = false,
              eventIsAcsDelta = false,
            ),
            PersistentEventType.WitnessedConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              eventIsAcsDelta = false,
            ),
            PersistentEventType.NonConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingParties,
              eventIsAcsDelta = false,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_various_witnessed",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
    }

  private def fetchAcsDeltaEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawAcsDeltaEventLegacy]] =
    SQL"""
        SELECT
          #$selectColumns,
          flat_event_witnesses as event_witnesses,
          command_id
        FROM
          #$tableName
        WHERE
          ${queryStrategy.inBatch("event_sequential_id", eventSequentialIds)}
        ORDER BY
          event_sequential_id
      """
      .withFetchSize(Some(fetchSize(eventSequentialIds)))
      .asVectorOf(rawAcsDeltaEventParser(allFilterParties, stringInterning))(connection)

  override def fetchEventPayloadsAcsDeltaLegacy(target: EventPayloadSourceForUpdatesAcsDeltaLegacy)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawAcsDeltaEventLegacy]] =
    target match {
      case EventPayloadSourceForUpdatesAcsDeltaLegacy.Consuming =>
        fetchAcsDeltaEvents(
          tableName = "lapi_events_consuming_exercise",
          selectColumns = selectColumnsForFlatTransactionsExercise,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = requestingParties,
        )(connection)
      case EventPayloadSourceForUpdatesAcsDeltaLegacy.Create =>
        fetchAcsDeltaEvents(
          tableName = "lapi_events_create",
          selectColumns = selectColumnsForFlatTransactionsCreate,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = requestingParties,
        )(connection)
    }

  private def fetchLedgerEffectsEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawLedgerEffectsEventLegacy]] =
    SQL"""
        SELECT
          #$selectColumns,
          tree_event_witnesses as event_witnesses,
          command_id
        FROM
          #$tableName
        WHERE
          ${queryStrategy.inBatch("event_sequential_id", eventSequentialIds)}
        ORDER BY
          event_sequential_id
      """
      .withFetchSize(Some(fetchSize(eventSequentialIds)))
      .asVectorOf(rawLedgerEffectsEventParser(allFilterParties, stringInterning))(connection)

  override def fetchEventPayloadsLedgerEffectsLegacy(
      target: EventPayloadSourceForUpdatesLedgerEffectsLegacy
  )(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawLedgerEffectsEventLegacy]] =
    target match {
      case EventPayloadSourceForUpdatesLedgerEffectsLegacy.Consuming =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_consuming_exercise",
          selectColumns =
            s"${selectColumnsForTransactionTreeExercise(includeFlatEventWitnesses = true)}, ${QueryStrategy
                .constBooleanSelect(true)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = requestingParties,
        )(connection)
      case EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = requestingParties,
        )(connection)
      case EventPayloadSourceForUpdatesLedgerEffectsLegacy.NonConsuming =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_non_consuming_exercise",
          selectColumns =
            s"${selectColumnsForTransactionTreeExercise(includeFlatEventWitnesses = false)}, ${QueryStrategy
                .constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = requestingParties,
        )(connection)
    }

  private def fetchSize(eventSequentialIds: SequentialIdBatch): Int =
    eventSequentialIds match {
      case SequentialIdBatch.IdRange(fromInclusive, toInclusive) =>
        Math.min(toInclusive - fromInclusive + 1, Int.MaxValue).toInt
      case SequentialIdBatch.Ids(ids) => ids.size
    }

}
