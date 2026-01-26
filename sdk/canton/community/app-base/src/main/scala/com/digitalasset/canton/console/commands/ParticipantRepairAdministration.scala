// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionValidation,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.admin.participant.v30.{ExportAcsOldResponse, ExportAcsResponse}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.grpc.FileStreamObserver
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.{
  ActiveContractOld,
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ReassignmentCounter, SequencerCounter, SynchronizerAlias}
import com.google.protobuf.ByteString
import io.grpc.Context

import java.time.Instant
import java.util.UUID
import scala.util.{Failure, Success, Try}

class ParticipantRepairAdministration(
    runner: AdminCommandRunner,
    val loggerFactory: NamedLoggerFactory,
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends FeatureFlagFilter
    with NoTracing
    with Helpful {
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  @Help.Summary("Purge contracts with specified Contract IDs from local participant")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in
      |which participant contracts have somehow gotten out of sync and need to be manually
      |purged, or in situations in which stakeholders are no longer available to agree to their
      |archival. The participant needs to be disconnected from the synchronizer on which the
      |contracts with "contractIds" reside at the time of the call, and as of now the
      |synchronizer cannot have had any inflight requests.
      |The effects of the command will take affect upon reconnecting to the synchronizer.
      |The "ignoreAlreadyPurged" flag makes it possible to invoke the command multiple times
      |with the same parameters in case an earlier command invocation has failed.
      |As repair commands are powerful tools to recover from unforeseen data corruption, but
      |dangerous under normal operation, use of this command requires (temporarily) enabling the
      |"features.enable-repair-commands" configuration. In addition repair commands can run for
      |an unbounded time depending on the number of contract ids passed in. Be sure to not
      |connect the participant to the synchronizer until the call returns.
      """
  )
  def purge(
      synchronizerAlias: SynchronizerAlias,
      contractIds: Seq[LfContractId],
      ignoreAlreadyPurged: Boolean = true,
  ): Unit = check(FeatureFlag.Repair) {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement.PurgeContracts(
          synchronizerAlias = synchronizerAlias,
          contracts = contractIds,
          ignoreAlreadyPurged = ignoreAlreadyPurged,
        )
      )
    }
  }

  @Help.Summary("Migrate contracts from one synchronizer to another one", FeatureFlag.Repair)
  @Help.Description(
    """Migrates all contracts associated with a synchronizer to a new synchronizer.
      |This method will register the new synchronizer, connect to it and then re-associate all
      |contracts from the source synchronizer to the target synchronizer. Please note that this
      |migration needs to be done by all participants at the same time. The target synchronizer
      |should only be used once all participants have finished their migration.
      |
      |WARNING: The migration does not start in case of in-flight transactions on the source
      |synchronizer. Forcing the migration may lead to a ledger fork! Instead of forcing the
      |migration, ensure the source synchronizer has no in-flight transactions by reconnecting
      |all participants to the source synchronizer, halting activity on these participants and
      |waiting for the in-flight transactions to complete or time out. Forcing a migration is
      |intended for disaster recovery when a source synchronizer cannot be recovered anymore.
      |
      |Parameters:
      |- source: The synchronizer alias of the source synchronizer.
      |- target: The configuration for the target synchronizer.
      |- force: If true, migration is forced ignoring in-flight transactions. Defaults to false.
      """
  )
  def migrate_synchronizer(
      source: SynchronizerAlias,
      target: SynchronizerConnectionConfig,
      force: Boolean = false,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .MigrateSynchronizer(source, target.toInternal, force = force)
        )
      }
    }

  @Help.Summary("Change assignation of contracts from one synchronizer to another")
  @Help.Description(
    """This is a last resort command to recover from data corruption in scenarios in which a
      |synchronizer is irreparably broken and formerly connected participants need to change the
      |assignation of contracts to another, healthy synchronizer. The participant needs to be
      |disconnected from both the "sourceSynchronizer" and the "targetSynchronizer".
      |The target synchronizer cannot have had any inflight requests.
      |Contracts already assigned to the target synchronizer will be skipped, and this makes it
      |possible to invoke this command in an "idempotent" fashion in case an earlier attempt had
      |resulted in an error.
      |The "skipInactive" flag makes it possible to only change the assignment of active
      |contracts in the "sourceSynchronizer".
      |As repair commands are powerful tools to recover from unforeseen data corruption, but
      |dangerous under normal operation, use of this command requires (temporarily) enabling the
      |"features.enable-repair-commands" configuration. In addition repair commands can run for
      |an unbounded time depending on the number of contract ids passed in. Be sure to not
      |connect the participant to either synchronizer until the call returns.
      |
      |Parameters:
      |- contractsIds: Set of contract ids that should change assignation to the new
      |  synchronizer.
      |- sourceSynchronizerAlias: Alias of the source synchronizer.
      |- targetSynchronizerAlias: Alias of the target synchronizer.
      |- reassignmentCounterOverride: By default, the reassignment counter is increased by one
      |  during the change assignation procedure if the value of the reassignment counter needs
      |  to be forced, the new value can be passed in the map.
      |- skipInactive: (default true) whether to skip inactive contracts mentioned in the
      |  contractIds list
      """
  )
  def change_assignation(
      contractsIds: Seq[LfContractId],
      sourceSynchronizerAlias: SynchronizerAlias,
      targetSynchronizerAlias: SynchronizerAlias,
      reassignmentCounterOverride: Map[LfContractId, ReassignmentCounter] = Map.empty,
      skipInactive: Boolean = true,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .ChangeAssignation(
              sourceSynchronizerAlias = sourceSynchronizerAlias,
              targetSynchronizerAlias = targetSynchronizerAlias,
              skipInactive = skipInactive,
              contracts = contractsIds.map(cid => (cid, reassignmentCounterOverride.get(cid))),
            )
        )
      }
    }

  // TODO(#24610) – Remove, replaced by `export_acs`
  @Help.Summary("Export active contracts for the given set of parties to a file (DEPRECATED)")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to
        |ACS snapshot file. Afterwards, the 'import_acs_old' command allows importing it into a
        |participant's ACS again. Such ACS export (and import) is interesting for recovery and
        |operational purposes only.
        |
        |Note that the 'export_acs_old' command execution may take a long time to complete and may
        |require significant resources.
        |
        |DEPRECATION NOTICE: A future release removes this command, use `export_acs` instead.
        |
        |
        |Parameters:
        |- parties: identifying contracts having at least one stakeholder from the given set.
        |  If empty, contracts of all parties will be exported.
        |- partiesOffboarding: True if the parties will be offboarded (party migration).
        |- outputFile: The output file name where to store the data.
        |- filterSynchronizerId: Restrict the export to a given synchronizer.
        |- timestamp: Optionally a timestamp for which we should take the state (useful to
        |  reconcile states of a synchronizer).
        |- contractSynchronizerRenames: As part of the export, allow to rename the associated
        |  synchronizer id of contracts from one synchronizer to another based on the mapping.
        |- force: If is set to true, then the check that the timestamp is clean will not be done.
        |  For this option to yield a consistent snapshot, you need to wait at least
        |  confirmationResponseTimeout + mediatorReactionTimeout after the last submitted request.
        """
  )
  @deprecated(
    "Method export_acs_old has been deprecated. Use acs_export_instead. For party replication, see participant.parties.export_acs",
    since = "3.4",
  )
  def export_acs_old(
      parties: Set[PartyId],
      partiesOffboarding: Boolean,
      outputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      filterSynchronizerId: Option[SynchronizerId] = None,
      timestamp: Option[Instant] = None,
      force: Boolean = false,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        val file = File(outputFile)
        val responseObserver = new FileStreamObserver[ExportAcsOldResponse](file, _.chunk)

        def call: ConsoleCommandResult[Context.CancellableContext] =
          runner.adminCommand(
            ParticipantAdminCommands.ParticipantRepairManagement
              .ExportAcsOld(
                parties,
                partiesOffboarding = partiesOffboarding,
                filterSynchronizerId,
                timestamp,
                responseObserver,
                force = force,
              )
          )

        processResult(
          call,
          responseObserver.result,
          timeout,
          request = "exporting Acs",
          cleanupOnError = () => file.delete(),
        )
      }
    }

  // TODO(#24610) – Remove, replaced by `import_acs`
  @Help.Summary(
    "Import active contracts from an Active Contract Set (ACS) snapshot file. (DEPRECATED)"
  )
  @Help.Description(
    """This command imports contracts from an ACS snapshot file into the participant's ACS. The
        |given ACS snapshot file needs to be the resulting file from a previous 'export_acs_old'
        |command invocation.
        |
        |The contract IDs of the imported contracts will be checked ahead of starting the
        |process. If any contract ID doesn't match the contract ID scheme associated to the
        |synchronizer where the contract is assigned to, the whole import process will fail.
        |
        |DEPRECATION NOTICE: A future release removes this command, use `import_acs` instead.
        """
  )
  @deprecated(
    "Method import_acs_old has been deprecated. Use import_acs instead. For party replication, see participant.parties.import_party_acs",
    since = "3.4",
  )
  def import_acs_old(
      inputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      workflowIdPrefix: String = "",
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcsOld(
            ByteString.copyFrom(File(inputFile).loadBytes),
            if (workflowIdPrefix.nonEmpty) workflowIdPrefix else s"import-${UUID.randomUUID}",
          )
        )
      }
    }

  @Help.Summary("Export active contracts for the given set of parties to a file")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to a
      |GZIP compressed ACS snapshot file. Afterwards, the `import_acs` repair command imports it
      |into a participant's ACS again.
      |
      |Parameters:
      |- parties: Identifying contracts having at least one stakeholder from the given set.
      |- ledgerOffset: The offset at which the ACS snapshot is exported.
      |- exportFilePath: The path denoting the file where the ACS snapshot will be stored.
      |- excludedStakeholders: When defined, any contract that has one or more of these parties
      |  as a stakeholder will be omitted from the ACS snapshot.
      |- synchronizerId: When defined, restricts the export to the given synchronizer.
      |- contractSynchronizerRenames: Changes the associated synchronizer id of contracts from
      |  one synchronizer to another based on the mapping.
      |- timeout: A timeout for this operation to complete.
      """
  )
  def export_acs(
      parties: Set[PartyId],
      ledgerOffset: Long,
      exportFilePath: String = "canton-acs-export.gz",
      excludedStakeholders: Set[PartyId] = Set.empty,
      synchronizerId: Option[SynchronizerId] = None,
      contractSynchronizerRenames: Map[SynchronizerId, SynchronizerId] = Map.empty,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = File(exportFilePath)
      val responseObserver = new FileStreamObserver[ExportAcsResponse](file, _.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ExportAcs(
            parties,
            synchronizerId,
            ledgerOffset,
            responseObserver,
            contractSynchronizerRenames,
            excludedStakeholders,
          )
        )

      processResult(
        call,
        responseObserver.result,
        timeout,
        request = "exporting acs",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary("Write active contracts to a file")
  @Help.Description(
    """The file can be imported using command `import_acs`.
      |
      |Parameters:
      |- contracts: Contracts to be written.
      |- protocolVersion: Protocol version of the synchronizer of the contracts.
      """
  )
  def write_contracts_to_file(
      contracts: Seq[com.daml.ledger.api.v2.state_service.ActiveContract],
      protocolVersion: ProtocolVersion,
      exportFilePath: String = "canton-acs-export.gz",
  ): Unit = {
    val output = File(exportFilePath).newGzipOutputStream()
    val res = contracts.traverse_ { lapiContract =>
      val contract = com.digitalasset.canton.participant.admin.data.ActiveContract
        .create(lapiContract)(protocolVersion)

      Try(contract.writeDelimitedTo(output))
    }
    output.close()

    res match {
      case Failure(exception) =>
        consoleEnvironment.raiseError(
          s"Unable to write contracts to file $exportFilePath: ${exception.getMessage}"
        )
      case Success(()) =>
    }
  }

  @Help.Summary("Import active contracts from an Active Contract Set (ACS) snapshot file")
  @Help.Description(
    """This command imports contracts from an ACS snapshot file into the participant's ACS. It
      |expects the given ACS snapshot file to be the result of a previous `export_acs` command
      |invocation.
      |
      |The contract IDs of the imported contracts may be checked ahead of starting the process.
      |If any contract ID doesn't match the contract ID scheme associated to the synchronizer
      |where the contract is assigned to, the whole import process fails depending on the value
      |of `contractImportMode`.
      |
      |By default `contractImportMode` is set to `ContractImportMode.Validation`.
      |
      |Expert only: As validation of contract IDs may lengthen the import
      |significantly, you have the option to simply accept the contract IDs as they are using
      |`ContractImportMode.Accept`.
      |
      |Parameters:
      |- importFilePath: The path denoting the file from where the ACS snapshot will be read.
      |  Defaults to "canton-acs-export.gz" when undefined.
      |- workflowIdPrefix: Sets a custom prefix for the workflow ID to easily identify all
      |  transactions generated by this import. Defaults to "import-<random_UUID>" when
      |  unspecified.
      |- contractImportMode: Governs contract authentication processing on import. Options
      |  include Validation (default), [Accept].
      |- representativePackageIdOverride: Defines override mappings for assigning
      |  representative package IDs to contracts upon ACS import.
      |- excludedStakeholders: When defined, any contract that has one or more of these
      |  parties as a stakeholder will be omitted from the import.
      """
  )
  def import_acs(
      importFilePath: String = "canton-acs-export.gz",
      workflowIdPrefix: String = "",
      contractImportMode: ContractImportMode = ContractImportMode.Validation,
      representativePackageIdOverride: RepresentativePackageIdOverride =
        RepresentativePackageIdOverride.NoOverride,
      excludedStakeholders: Set[PartyId] = Set.empty,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcs(
            ByteString.copyFrom(File(importFilePath).loadBytes),
            if (workflowIdPrefix.nonEmpty) workflowIdPrefix else s"import-${UUID.randomUUID}",
            contractImportMode,
            representativePackageIdOverride,
            excludedStakeholders,
          )
        )
      }
    }

  // TODO(#30342) - Consolidate with import_acs, or separate it clearly
  @Help.Summary("Import active contracts from an Active Contract Set (ACS) snapshot file")
  @Help.Description(
    """This command imports contracts from an ACS snapshot file into the participant's ACS. It
      |expects the given ACS snapshot file to be the result of a previous `export_acs` command
      |invocation.
      |Unlike the `import_acs` it does not read the full snapshot into memory.
      |
      |The contract IDs of the imported contracts may be checked ahead of starting the process.
      |If any contract ID doesn't match the contract ID scheme associated to the synchronizer
      |where the contract is assigned to, the whole import process fails depending on the value
      |of `contractImportMode`.
      |
      |By default `contractImportMode` is set to `ContractImportMode.Validation`.
      |
      |Expert only: As validation of contract IDs may lengthen the import
      |significantly, you have the option to simply accept the contract IDs as they are using
      |`ContractImportMode.Accept`.
      |
      |The arguments are:
      |- importFilePath: The path denoting the file from where the ACS snapshot will be read.
      |                  Defaults to "canton-acs-export.gz" when undefined.
      |- synchronizerId: The identifier of the synchronizer managing the contract to be
      |                  imported. If a contract has a different synchronizer, import will fail.
      |- workflowIdPrefix: Sets a custom prefix for the workflow ID to easily identify all
      |                  transactions generated by this import.
      |                  Defaults to "import-<random_UUID>" when unspecified.
      |- contractImportMode: Governs contract authentication processing on import. Options
      |                      include Validation (default), [Accept].
      |- representativePackageIdOverride: Defines override mappings for assigning representative
      |                                   package IDs to contracts upon ACS import.
      |- excludedStakeholders: When defined, any contract that has one or more of these
      |                        parties as a stakeholder will be omitted from the import.
      """
  )
  def import_acsV2(
      importFilePath: String = "canton-acs-export.gz",
      synchronizerId: SynchronizerId,
      workflowIdPrefix: String = "",
      contractImportMode: ContractImportMode = ContractImportMode.Validation,
      representativePackageIdOverride: RepresentativePackageIdOverride =
        RepresentativePackageIdOverride.NoOverride,
      excludedStakeholders: Set[PartyId] = Set.empty,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcsV2(
            new java.io.File(importFilePath),
            if (workflowIdPrefix.nonEmpty) workflowIdPrefix else s"import-${UUID.randomUUID}",
            contractImportMode,
            representativePackageIdOverride,
            excludedStakeholders,
            synchronizerId,
          )
        )
      }
    }

  @Help.Summary("Add specified contracts to a specific synchronizer on the participant")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which
      |participant contracts have somehow gotten out of sync and need to be manually created.
      |
      |The participant needs to be disconnected from the specified "synchronizer" at the time of
      |the call, and as of now the synchronizer cannot have had any inflight requests.
      |
      |The effects of the command will take affect upon reconnecting to the synchronizer.
      |
      |As repair commands are powerful tools to recover from unforeseen data corruption, but
      |dangerous under normal operation, use of this command requires (temporarily) enabling the
      |`features.enable-repair-commands` configuration. In addition repair commands can run for
      |an unbounded time depending on the number of contracts passed in. Be sure to not connect
      |the participant to the synchronizer until the call returns.
      |
      |Parameters:
      |- synchronizerId: The ID of the synchronizer to which to add the contract.
      |- protocolVersion: The protocol version used by the synchronizer.
      |- contracts: List of contracts to add with witness information.
      """
  )
  def add(
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      contracts: Seq[RepairContract],
  ): Unit = {

    val temporaryFile = File.newTemporaryFile(suffix = ".gz")
    val outputStream = temporaryFile.newGzipOutputStream()

    ResourceUtil.withResource(outputStream) { outputStream =>
      contracts
        .traverse_ { repairContract =>
          for {
            serializableContract <- ContractInstance.toSerializableContract(repairContract.contract)
            activeContract = ActiveContractOld.create(
              synchronizerId,
              serializableContract,
              repairContract.reassignmentCounter,
            )(protocolVersion)
            _ <- activeContract.writeDelimitedTo(outputStream)
          } yield outputStream.flush()
        }
        .valueOr(err => throw new RuntimeException(s"Unable to add contract data to stream: $err"))
    }

    val bytes = ByteString.copyFrom(temporaryFile.loadBytes)
    temporaryFile.delete(swallowIOExceptions = true)

    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcsOld(
            bytes,
            workflowIdPrefix = s"import-${UUID.randomUUID}",
          )
        )
      }
    }
  }

  @Help.Summary("Purge the data of a deactivated synchronizer")
  @Help.Description(
    """This command deletes synchronizer data and helps to ensure that stale data in the
       |specified, deactivated synchronizer is not acted upon anymore. The specified synchronizer
       |needs to be in the `Inactive` status for purging to occur.
       |Purging a deactivated synchronizer is typically performed automatically as part of a hard
       |synchronizer migration via ``repair.migrate_synchronizer``.
       """
  )
  def purge_deactivated_synchronizer(synchronizerAlias: SynchronizerAlias): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.PurgeDeactivatedSynchronizer(
            synchronizerAlias
          )
        )
      }
    }

  @Help.Summary("Mark sequenced events as ignored")
  @Help.Description(
    """This is the last resort to ignore events that the participant is unable to process.
      |Ignoring events may lead to subsequent failures, e.g., if the event creating a contract
      |is ignored and that contract is subsequently used. It may also lead to ledger forks if
      |other participants still process the ignored events.
      |It is possible to mark events as ignored that the participant has not yet received.
      |
      |The command will fail, if marking events between `fromInclusive` and `toInclusive` as
      |ignored would result in a gap in sequencer counters, namely if `from <= to` and `from` is
      |greater than `maxSequencerCounter + 1`, where `maxSequencerCounter` is the greatest
      |sequencer counter of a sequenced event stored by the underlying participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer
      |counter of the last event that has been marked as clean. (Ignoring such events would
      |normally have no effect, as they have already been processed.)
      """
  )
  def ignore_events(
      physicalSynchronizerId: PhysicalSynchronizerId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean = false,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .IgnoreEvents(physicalSynchronizerId, fromInclusive, toInclusive, force)
        )
      }
    }

  @Help.Summary("Remove the ignored status from sequenced events")
  @Help.Description(
    """This command has no effect on ordinary (i.e., not ignored) events and on events that do
      |not exist.
      |
      |The command will fail, if marking events between `fromInclusive` and `toInclusive` as
      |unignored would result in a gap in sequencer counters, namely if there is one empty
      |ignored event with sequencer counter between `from` and `to` and another empty ignored
      |event with sequencer counter greater than `to`.
      |An empty ignored event is an event that has been marked as ignored and not yet received
      |by the participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer
      |counter of the last event that has been marked as clean. (Unignoring such events would
      |normally have no effect, as they have already been processed.)
      """
  )
  def unignore_events(
      physicalSynchronizerId: PhysicalSynchronizerId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean = false,
  ): Unit = check(FeatureFlag.Repair) {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement
          .UnignoreEvents(physicalSynchronizerId, fromInclusive, toInclusive, force)
      )
    }
  }

  @Help.Summary("Rollback an unassignment by re-assigning the contract to the source synchronizer")
  @Help.Description(
    """This is a last resort command to recover from an unassignment that cannot be completed on
      |the target synchronizer.
      |
      |Parameters:
      |- reassignmentId: Set of contract IDs that should change assignation to the new
      |  synchronizer.
      |- source: The source synchronizer ID.
      |- target: Alias of the target synchronizer.
      """
  )
  def rollback_unassignment(
      reassignmentId: String,
      source: SynchronizerId,
      target: SynchronizerId,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .RollbackUnassignment(
              reassignmentId = reassignmentId,
              source = source,
              target = target,
            )
        )
      }
    }

  // TODO(#28972) Remove preview flag
  @Help.Summary("Perform a logical synchronizer upgrade")
  @Help.Description(
    """This command allows to perform an offline logical synchronizer upgrade.
       |It should only be used if the node was offline at the time of the upgrade and the
       |synchronizer was decommissioned.
       |
       |Parameters:
       |- currentPhysicalSynchronizerId: ID of the synchronizer that should be upgraded.
       |- successorPhysicalSynchronizerId: ID of the new synchronizer.
       |- announcedUpgradeTime: Time at which the upgrade happened.
       |- successorConfig: configuration to connect to the new synchronizer.
       |- validation: The validations which need to be done to the connection.
      """
  )
  def perform_synchronizer_upgrade(
      currentPhysicalSynchronizerId: PhysicalSynchronizerId,
      successorPhysicalSynchronizerId: PhysicalSynchronizerId,
      announcedUpgradeTime: CantonTimestamp,
      successorConfig: SynchronizerConnectionConfig,
      validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
  ): Unit = check(FeatureFlag.Preview) {
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .PerformSynchronizerUpgrade(
              currentPSId = currentPhysicalSynchronizerId,
              successorPSId = successorPhysicalSynchronizerId,
              upgradeTime = announcedUpgradeTime,
              successorConfig = successorConfig.toInternal,
              sequencerConnectionValidation = validation.toInternal,
            )
        )
      }
    }
  }
}

object ParticipantRepairAdministration {
  private val ExportAcsDefaultFile = "canton-acs-export.gz"
}
