// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands
import com.digitalasset.canton.admin.participant.v30.ExportAcsResponse
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
import com.digitalasset.canton.data.RepairContract
import com.digitalasset.canton.grpc.FileStreamObserver
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, SequencerCounter}
import com.google.protobuf.ByteString
import io.grpc.Context

import java.time.Instant
import java.util.UUID

class ParticipantRepairAdministration(
    val consoleEnvironment: ConsoleEnvironment,
    runner: AdminCommandRunner,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with NoTracing
    with Helpful {
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  @Help.Summary("Purge contracts with specified Contract IDs from local participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
      |contracts have somehow gotten out of sync and need to be manually purged, or in situations in which
      |stakeholders are no longer available to agree to their archival. The participant needs to be disconnected from
      |the domain on which the contracts with "contractIds" reside at the time of the call, and as of now the domain
      |cannot have had any inflight requests.
      |The effects of the command will take affect upon reconnecting to the sync domain.
      |The "ignoreAlreadyPurged" flag makes it possible to invoke the command multiple times with the same
      |parameters in case an earlier command invocation has failed.
      |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
      |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
      |configuration. In addition repair commands can run for an unbounded time depending on the number of
      |contract ids passed in. Be sure to not connect the participant to the domain until the call returns."""
  )
  def purge(
      domain: DomainAlias,
      contractIds: Seq[LfContractId],
      ignoreAlreadyPurged: Boolean = true,
  ): Unit =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement.PurgeContracts(
          domain = domain,
          contracts = contractIds,
          ignoreAlreadyPurged = ignoreAlreadyPurged,
        )
      )
    }

  @Help.Summary("Migrate contracts from one domain to another one.")
  @Help.Description(
    """Migrates all contracts associated with a domain to a new domain.
        |This method will register the new domain, connect to it and then re-associate all contracts from the source
        |domain to the target domain. Please note that this migration needs to be done by all participants
        |at the same time. The target domain should only be used once all participants have finished their migration.
        |
        |WARNING: The migration does not start in case of in-flight transactions on the source domain. Forcing the
        |migration may lead to a ledger fork! Instead of forcing the migration, ensure the source domain has no
        |in-flight transactions by reconnecting all participants to the source domain, halting activity on these
        |participants and waiting for the in-flight transactions to complete or time out.
        |Forcing a migration is intended for disaster recovery when a source domain cannot be recovered anymore.
        |
        |The arguments are:
        |source: the domain alias of the source domain
        |target: the configuration for the target domain
        |force: if true, migration is forced ignoring in-flight transactions. Defaults to false.
        """
  )
  def migrate_domain(
      source: DomainAlias,
      target: DomainConnectionConfig,
      force: Boolean = false,
  ): Unit =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement
          .MigrateDomain(source, target, force = force)
      )
    }

  @Help.Summary("Export active contracts for the given set of parties to a file.")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to ACS snapshot file.
        |Afterwards, the 'import_acs' command allows importing it into a participant's ACS again.
        |Such ACS export (and import) is interesting for recovery and operational purposes only.
        |Note that the 'export_acs' command execution may take a long time to complete and may require significant
        |resources.
        |
        |

        |The arguments are:
        |- parties: identifying contracts having at least one stakeholder from the given set
        |- partiesOffboarding: true if the parties will be offboarded (party migration)
        |- outputFile: the output file name where to store the data. Use .gz as a suffix to get a  compressed file (recommended)
        |- filterDomainId: restrict the export to a given domain
        |- timestamp: optionally a timestamp for which we should take the state (useful to reconcile states of a domain)
        |- contractDomainRenames: As part of the export, allow to rename the associated domain id of contracts from one domain to another based on the mapping.
        |- force: if is set to true, then the check that the timestamp is clean will not be done.
        |         For this option to yield a consistent snapshot, you need to wait at least
        |         confirmationResponseTimeout + mediatorReactionTimeout after the last submitted request.
        """
  )
  def export_acs(
      parties: Set[PartyId],
      partiesOffboarding: Boolean,
      outputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      filterDomainId: Option[DomainId] = None,
      timestamp: Option[Instant] = None,
      contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)] = Map.empty,
      force: Boolean = false,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        val file = File(outputFile)
        val responseObserver = new FileStreamObserver[ExportAcsResponse](file, _.chunk)

        def call: ConsoleCommandResult[Context.CancellableContext] =
          runner.adminCommand(
            ParticipantAdminCommands.ParticipantRepairManagement
              .ExportAcs(
                parties,
                partiesOffboarding = partiesOffboarding,
                filterDomainId,
                timestamp,
                responseObserver,
                contractDomainRenames,
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

  @Help.Summary("Import active contracts from an Active Contract Set (ACS) snapshot file.")
  @Help.Description(
    """This command imports contracts from an ACS snapshot file into the participant's ACS.
        |The given ACS snapshot file needs to be the resulting file from a previous 'export_acs' command invocation.
        |
        |The contract IDs of the imported contracts will be checked ahead of starting the process. If any contract
        |ID doesn't match the contract ID scheme associated to the domain where the contract is assigned to, the
        |whole import process will fail depending on the value of `allowContractIdSuffixRecomputation`.
        |
        |By default `allowContractIdSuffixRecomputation` is set to `false`. If set to `true`, any contract ID
        |that wouldn't pass the check above will be recomputed. Note that the recomputation of contract IDs will
        |fail under the following circumstances:
        | - the contract salt used to compute the contract ID is missing
        | - the contract ID discriminator version is unknown
        |
        |Note that only the Canton-specific contract ID suffix will be recomputed. The discriminator cannot be
        |recomputed and will be left as is.
        |
        |The recomputation will not be performed on contract IDs referenced in the payload of some imported contract
        |but is missing from the import itself (this should mean that the contract was archived, which makes
        |recomputation unnecessary).
        |
        |If the import process succeeds, the mapping from the old contract IDs to the new contract IDs will be returned.
        |An empty map means that all contract IDs were valid and no contract ID was recomputed.
        """
  )
  def import_acs(
      inputFile: String = ParticipantRepairAdministration.ExportAcsDefaultFile,
      workflowIdPrefix: String = "",
      allowContractIdSuffixRecomputation: Boolean = false,
  ): Map[LfContractId, LfContractId] =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcs(
            ByteString.copyFrom(File(inputFile).loadBytes),
            if (workflowIdPrefix.nonEmpty) workflowIdPrefix else s"import-${UUID.randomUUID}",
            allowContractIdSuffixRecomputation = allowContractIdSuffixRecomputation,
          )
        )
      }
    }

  @Help.Summary("Add specified contracts to a specific domain on the participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually created. The participant needs to be
        |disconnected from the specified "domain" at the time of the call, and as of now the domain cannot have had
        |any inflight requests.
        |The effects of the command will take affect upon reconnecting to the sync domain.
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contracts passed in. Be sure to not connect the participant to the domain until the call returns.
        |
        The arguments are:
        - domainId: the id of the domain to which to add the contract
        - protocolVersion: to protocol version used by the domain
        - contracts: list of contracts to add with witness information
        """
  )
  def add(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      contracts: Seq[RepairContract],
      allowContractIdSuffixRecomputation: Boolean = false,
  ): Map[LfContractId, LfContractId] = {

    val temporaryFile = File.newTemporaryFile(suffix = ".gz")
    val outputStream = temporaryFile.newGzipOutputStream()

    ResourceUtil.withResource(outputStream) { outputStream =>
      contracts
        .traverse_ { repairContract =>
          val activeContract = ActiveContract
            .create(domainId, repairContract.contract, repairContract.reassignmentCounter)(
              protocolVersion
            )
          activeContract.writeDelimitedTo(outputStream).map(_ => outputStream.flush())
        }
        .valueOr(err => throw new RuntimeException(s"Unable to add contract data to stream: $err"))
    }

    val bytes = ByteString.copyFrom(temporaryFile.loadBytes)
    temporaryFile.delete(swallowIOExceptions = true)

    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.ImportAcs(
            bytes,
            workflowIdPrefix = s"import-${UUID.randomUUID}",
            allowContractIdSuffixRecomputation = allowContractIdSuffixRecomputation,
          )
        )
      }
    }
  }

  @Help.Summary("Purge the data of a deactivated domain.")
  @Help.Description(
    """This command deletes domain data and helps to ensure that stale data in the specified, deactivated domain
       |is not acted upon anymore. The specified domain needs to be in the `Inactive` status for purging to occur.
       |Purging a deactivated domain is typically performed automatically as part of a hard domain migration via
       |``repair.migrate_domain``."""
  )
  def purge_deactivated_domain(domain: DomainAlias): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement.PurgeDeactivatedDomain(domain)
        )
      }
    }

  @Help.Summary("Mark sequenced events as ignored.")
  @Help.Description(
    """This is the last resort to ignore events that the participant is unable to process.
      |Ignoring events may lead to subsequent failures, e.g., if the event creating a contract is ignored and
      |that contract is subsequently used. It may also lead to ledger forks if other participants still process
      |the ignored events.
      |It is possible to mark events as ignored that the participant has not yet received.
      |
      |The command will fail, if marking events between `fromInclusive` and `toInclusive` as ignored would result in a gap in sequencer counters,
      |namely if `from <= to` and `from` is greater than `maxSequencerCounter + 1`,
      |where `maxSequencerCounter` is the greatest sequencer counter of a sequenced event stored by the underlying participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
      |that has been marked as clean.
      |(Ignoring such events would normally have no effect, as they have already been processed.)"""
  )
  def ignore_events(
      domainId: DomainId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean = false,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .IgnoreEvents(domainId, fromInclusive, toInclusive, force)
        )
      }
    }

  @Help.Summary("Remove the ignored status from sequenced events.")
  @Help.Description(
    """This command has no effect on ordinary (i.e., not ignored) events and on events that do not exist.
      |
      |The command will fail, if marking events between `fromInclusive` and `toInclusive` as unignored would result in a gap in sequencer counters,
      |namely if there is one empty ignored event with sequencer counter between `from` and `to` and
      |another empty ignored event with sequencer counter greater than `to`.
      |An empty ignored event is an event that has been marked as ignored and not yet received by the participant.
      |
      |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
      |that has been marked as clean.
      |(Unignoring such events would normally have no effect, as they have already been processed.)"""
  )
  def unignore_events(
      domainId: DomainId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean = false,
  ): Unit = check(FeatureFlag.Repair) {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.ParticipantRepairManagement
          .UnignoreEvents(domainId, fromInclusive, toInclusive, force)
      )
    }
  }
}

abstract class LocalParticipantRepairAdministration(
    override val consoleEnvironment: ConsoleEnvironment,
    runner: AdminCommandRunner,
    override val loggerFactory: NamedLoggerFactory,
) extends ParticipantRepairAdministration(
      consoleEnvironment = consoleEnvironment,
      runner = runner,
      loggerFactory = loggerFactory,
    ) {

  protected def access[T](handler: ParticipantNode => T): T

  private def runRepairCommand[T](command: TraceContext => Either[String, T]): T =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        ConsoleCommandResult.fromEither {
          // Ensure that admin repair commands have a non-empty trace context.
          TraceContext.withNewTraceContext(command(_))
        }
      }
    }

  @Help.Summary("Change assignation of contracts from one domain to another.")
  @Help.Description(
    """This is a last resort command to recover from data corruption in scenarios in which a domain is
        |irreparably broken and formerly connected participants need to change the assignation of contracts to another,
        |healthy domain. The participant needs to be disconnected from both the "sourceDomain" and the "targetDomain".
        |The target domain cannot have had any inflight requests.
        |Contracts already assigned to the target domain will be skipped, and this makes it possible to invoke this
        |command in an "idempotent" fashion in case an earlier attempt had resulted in an error.
        |The "skipInactive" flag makes it possible to only change the assignment of active contracts in the "sourceDomain".
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contract ids passed in. Be sure to not connect the participant to either domain until the call returns.

        Arguments:
        - contractIds - set of contract ids that should change assignation to the new domain
        - sourceDomain - alias of the source domain
        - targetDomain - alias of the target domain
        - skipInactive - (default true) whether to skip inactive contracts mentioned in the contractIds list
        - batchSize - (default 100) how many contracts to write at once to the database"""
  )
  def change_assignation(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean = true,
      batchSize: Int = 100,
  ): Unit =
    runRepairCommand(tc =>
      access(
        _.sync.repairService.changeAssignationAwait(
          contractIds,
          sourceDomain,
          targetDomain,
          skipInactive,
          PositiveInt.tryCreate(batchSize),
        )(tc)
      )
    )

  @Help.Summary("Rollback an unassignment by re-assigning the contract to the source domain.")
  @Help.Description(
    """This is a last resort command to recover from an unassignment that cannot be completed on the target domain.
        Arguments:
        - unassignId - set of contract ids that should change assignation to the new domain
        - source - the source domain id
        - target - alias of the target domain"""
  )
  def rollback_unassignment(
      unassignId: String,
      source: DomainId,
      target: DomainId,
  ): Unit =
    check(FeatureFlag.Repair) {
      consoleEnvironment.run {
        runner.adminCommand(
          ParticipantAdminCommands.ParticipantRepairManagement
            .RollbackUnassignment(unassignId = unassignId, source = source, target = target)
        )
      }
    }
}

object ParticipantRepairAdministration {
  private val ExportAcsDefaultFile = "canton-acs-export.gz"
}
