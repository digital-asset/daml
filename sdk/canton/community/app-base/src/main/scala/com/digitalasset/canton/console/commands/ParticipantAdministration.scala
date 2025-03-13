// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Package.DarData
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Pruning.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Resources.{
  GetResourceLimits,
  SetResourceLimits,
}
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.admin.api.client.data.PackageDescription.PackageContents
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.PruningServiceGrpc
import com.digitalasset.canton.admin.participant.v30.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  ConsoleCommandTimeout,
  NonNegativeDuration,
  SynchronizerTimeTrackerConfig,
}
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  BaseInspection,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  LedgerApiCommandRunner,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.grpc.ByteStringStreamObserver
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  ReceivedCmtState,
  SentCmtState,
}
import com.digitalasset.canton.participant.pruning.{
  CommitmentContractMetadata,
  CommitmentInspectContract,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfContractId, LfVersionedTransaction, SerializableContract}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.*
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias, config}
import io.grpc.Context
import spray.json.DeserializationException

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

sealed trait SynchronizerChoice
object SynchronizerChoice {
  object All extends SynchronizerChoice
  final case class Only(aliases: Seq[SynchronizerAlias]) extends SynchronizerChoice
}

private[console] object ParticipantCommands {

  object dars {

    def upload(
        runner: AdminCommandRunner,
        path: String,
        description: String,
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        expectedMainPackageId: String,
        requestHeaders: Map[String, String],
        logger: TracedLogger,
    ): ConsoleCommandResult[String] =
      runner
        .adminCommand(
          ParticipantAdminCommands.Package
            .UploadDar(
              path,
              vetAllPackages,
              synchronizeVetting,
              description,
              expectedMainPackageId,
              requestHeaders,
              logger,
            )
        )
        .flatMap {
          case Seq(mainPackageId) =>
            CommandSuccessful(mainPackageId)
          case Seq() =>
            GenericCommandError(
              "Uploading the DAR unexpectedly did not return a DAR main package ID"
            )
          case many =>
            GenericCommandError(
              s"Uploading the DAR unexpectedly returned multiple DAR main package IDs: $many"
            )
        }

    def upload_many(
        runner: AdminCommandRunner,
        paths: Seq[String],
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        requestHeaders: Map[String, String],
        logger: TracedLogger,
    ): ConsoleCommandResult[Seq[String]] =
      runner.adminCommand(
        ParticipantAdminCommands.Package
          .UploadDar(
            paths.map(DarData(_, "", "")),
            vetAllPackages,
            synchronizeVetting,
            requestHeaders,
            logger,
          )
      )

    def validate(
        runner: AdminCommandRunner,
        path: String,
        logger: TracedLogger,
    ): ConsoleCommandResult[String] =
      runner.adminCommand(
        ParticipantAdminCommands.Package
          .ValidateDar(Some(path), logger)
      )

  }

  object synchronizers {

    def reference_to_config(
        sequencers: NonEmpty[Map[SequencerAlias, SequencerReference]],
        synchronizerAlias: SynchronizerAlias,
        manualConnect: Boolean = false,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        priority: Int = 0,
        sequencerTrustThreshold: PositiveInt = PositiveInt.one,
        submissionRequestAmplification: SubmissionRequestAmplification =
          SubmissionRequestAmplification.NoAmplification,
    ): SynchronizerConnectionConfig =
      SynchronizerConnectionConfig(
        synchronizerAlias,
        SequencerConnections.tryMany(
          sequencers.toSeq.map { case (alias, sequencer) =>
            sequencer.sequencerConnection.withAlias(alias)
          },
          sequencerTrustThreshold,
          submissionRequestAmplification,
        ),
        manualConnect = manualConnect,
        None,
        priority,
        None,
        maxRetryDelay,
        SynchronizerTimeTrackerConfig(),
      )

    def to_config(
        synchronizerAlias: SynchronizerAlias,
        connection: String,
        manualConnect: Boolean = false,
        synchronizerId: Option[SynchronizerId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        timeTrackerConfig: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    ): SynchronizerConnectionConfig = {
      // architecture-handbook-entry-begin: OnboardParticipantToConfig
      val certificates = OptionUtil.emptyStringAsNone(certificatesPath).map { path =>
        BinaryFileUtil.readByteStringFromFile(path) match {
          case Left(err) => throw new IllegalArgumentException(s"failed to load $path: $err")
          case Right(bs) => bs
        }
      }
      SynchronizerConnectionConfig.grpc(
        SequencerAlias.Default,
        synchronizerAlias,
        connection,
        manualConnect,
        synchronizerId,
        certificates,
        priority,
        initialRetryDelay,
        maxRetryDelay,
        timeTrackerConfig,
      )
      // architecture-handbook-entry-end: OnboardParticipantToConfig
    }

    def register(
        runner: AdminCommandRunner,
        config: SynchronizerConnectionConfig,
        performHandshake: Boolean,
        validation: SequencerConnectionValidation,
    ): ConsoleCommandResult[Unit] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity
          .RegisterSynchronizer(config, performHandshake = performHandshake, validation)
      )

    def connect(
        runner: AdminCommandRunner,
        config: SynchronizerConnectionConfig,
        validation: SequencerConnectionValidation,
    ): ConsoleCommandResult[Unit] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.ConnectSynchronizer(config, validation)
      )

    def reconnect(
        runner: AdminCommandRunner,
        synchronizerAlias: SynchronizerAlias,
        retry: Boolean,
    ): ConsoleCommandResult[Boolean] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity
          .ReconnectSynchronizer(synchronizerAlias, retry)
      )

    def list_connected(
        runner: AdminCommandRunner
    ): ConsoleCommandResult[Seq[ListConnectedSynchronizersResult]] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.ListConnectedSynchronizers()
      )

    def reconnect_all(
        runner: AdminCommandRunner,
        ignoreFailures: Boolean,
    ): ConsoleCommandResult[Unit] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.ReconnectSynchronizers(ignoreFailures)
      )

    def disconnect(
        runner: AdminCommandRunner,
        synchronizerAlias: SynchronizerAlias,
    ): ConsoleCommandResult[Unit] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.DisconnectSynchronizer(synchronizerAlias)
      )

    def disconnect_all(
        runner: AdminCommandRunner
    ): ConsoleCommandResult[Unit] =
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.DisconnectAllSynchronizers()
      )

  }
}

class ParticipantTestingGroup(
    participantRef: ParticipantReference,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {
  import participantRef.*

  @Help.Summary(
    "Send a bong to a set of target parties over the ledger. Levels > 0 leads to an exploding ping with exponential number of contracts. " +
      "Throw a RuntimeException in case of failure.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """Initiates a racy ping to multiple participants,
     measuring the roundtrip time of the fastest responder, with an optional timeout.
     Grace-period is the time the bong will wait for a duplicate spent (which would indicate an error in the system) before exiting.
     If levels > 0, the ping command will lead to a binary explosion and subsequent dilation of
     contracts, where ``level`` determines the number of levels we will explode. As a result, the system will create
     (2^(L+2) - 3) contracts (where L stands for ``level``).
     Normally, only the initiator is a validator. Additional validators can be added using the validators argument.
     The bong command comes handy to run a burst test against the system and quickly leads to an overloading state."""
  )
  def bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Int = 0,
      synchronizerId: Option[SynchronizerId] = None,
      workflowId: String = "",
      id: String = "",
  ): Duration =
    consoleEnvironment.runE(
      maybe_bong(targets, validators, timeout, levels, synchronizerId, workflowId, id)
        .toRight(
          s"Unable to bong $targets with $levels levels within ${LoggerUtil.roundDurationForHumans(timeout.duration)}"
        )
    )

  @Help.Summary("Like bong, but returns None in case of failure.", FeatureFlag.Testing)
  def maybe_bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Int = 0,
      synchronizerId: Option[SynchronizerId] = None,
      workflowId: String = "",
      id: String = "",
  ): Option[Duration] =
    check(FeatureFlag.Testing)(consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            targets.map(_.adminParty.toLf),
            validators.map(_.adminParty.toLf),
            timeout,
            levels,
            synchronizerId,
            workflowId,
            id,
          )
      )
    }).toOption

  @Help.Summary("Fetch the current time from the given synchronizer", FeatureFlag.Testing)
  def fetch_synchronizer_time(
      synchronizerAlias: SynchronizerAlias,
      timeout: NonNegativeDuration,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      val id = participantRef.synchronizers.id_of(synchronizerAlias)
      fetch_synchronizer_time(id, timeout)
    }

  @Help.Summary("Fetch the current time from the given synchronizer", FeatureFlag.Testing)
  def fetch_synchronizer_time(
      synchronizerId: SynchronizerId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          SynchronizerTimeCommands.FetchTime(
            synchronizerId.some,
            NonNegativeFiniteDuration.Zero,
            timeout,
          )
        )
      }.timestamp
    }

  @Help.Summary("Fetch the current time from all connected synchronizers", FeatureFlag.Testing)
  def fetch_synchronizer_times(
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand
  ): Unit =
    check(FeatureFlag.Testing) {
      participantRef.synchronizers.list_connected().foreach { item =>
        fetch_synchronizer_time(item.synchronizerId, timeout).discard[CantonTimestamp]
      }
    }

  @Help.Summary(
    "Await for the given time to be reached on the given synchronizer",
    FeatureFlag.Testing,
  )
  def await_synchronizer_time(
      synchronizerAlias: SynchronizerAlias,
      time: CantonTimestamp,
      timeout: NonNegativeDuration,
  ): Unit =
    check(FeatureFlag.Testing) {
      val id = participantRef.synchronizers.id_of(synchronizerAlias)
      await_synchronizer_time(id, time, timeout)
    }

  @Help.Summary(
    "Await for the given time to be reached on the given synchronizer",
    FeatureFlag.Testing,
  )
  def await_synchronizer_time(
      synchronizerId: SynchronizerId,
      time: CantonTimestamp,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): Unit =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          SynchronizerTimeCommands.AwaitTime(
            synchronizerId.some,
            time,
            timeout,
          )
        )
      }
    }
}

class LocalParticipantTestingGroup(
    participantRef: ParticipantReference with BaseInspection[ParticipantNode],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends ParticipantTestingGroup(participantRef, consoleEnvironment, loggerFactory)
    with FeatureFlagFilter
    with NoTracing {

  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  import participantRef.*
  @Help.Summary("Lookup contracts in the Private Contract Store", FeatureFlag.Testing)
  @Help.Description("""Get raw access to the PCS of the given synchronizer sync controller.
  The filter commands will check if the target value ``contains`` the given string.
  The arguments can be started with ``^`` such that ``startsWith`` is used for comparison or ``!`` to use ``equals``.
  The ``activeSet`` argument allows to restrict the search to the active contract set.
  For contract ID filtration only exact match is supported.
  """)
  def pcs_search(
      synchronizerAlias: SynchronizerAlias,
      exactId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      // only include active contracts
      activeSet: Boolean = false,
      limit: PositiveInt = defaultLimit,
  ): List[(Boolean, SerializableContract)] = {
    def toOpt(str: String) = OptionUtil.emptyStringAsNone(str)

    val pcs = state_inspection
      .findContracts(
        synchronizerAlias,
        toOpt(exactId),
        toOpt(filterPackage),
        toOpt(filterTemplate),
        limit.value,
      )
    if (activeSet) pcs.filter { case (isActive, _) => isActive }
    else pcs
  }

  @Help.Summary("Lookup of active contracts", FeatureFlag.Testing)
  def acs_search(
      synchronizerAlias: SynchronizerAlias,
      exactId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      filterStakeholder: Option[PartyId] = None,
      limit: PositiveInt = defaultLimit,
  ): List[SerializableContract] = {
    val predicate = (c: SerializableContract) =>
      filterStakeholder.forall(s => c.metadata.stakeholders.contains(s.toLf))

    check(FeatureFlag.Testing) {
      pcs_search(
        synchronizerAlias,
        exactId,
        filterPackage,
        filterTemplate,
        activeSet = true,
        limit,
      )
        .map(_._2)
        .filter(predicate)
    }
  }

  @Help.Summary("Retrieve all sequencer messages", FeatureFlag.Testing)
  @Help.Description("""Optionally allows filtering for sequencer from a certain time span (inclusive on both ends) and
      |limiting the number of displayed messages. The returned messages will be ordered on most synchronizer ledger implementations
      |if a time span is given.
      |
      |Fails if the participant has never connected to the synchronizer.""")
  def sequencer_messages(
      synchronizerAlias: SynchronizerAlias,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[PossiblyIgnoredProtocolEvent] =
    state_inspection
      .findMessages(synchronizerAlias, from, to, Some(limit.value))
      .map(_.valueOr(e => throw new IllegalStateException(e.toString)))

  @Help.Summary(
    "Return the sync crypto api provider, which provides access to all cryptographic methods",
    FeatureFlag.Testing,
  )
  def crypto_api(): SyncCryptoApiParticipantProvider = check(FeatureFlag.Testing) {
    access(node => node.sync.syncCrypto)
  }

  @Help.Summary(
    "The latest timestamp before or at the given one for which no commitment is outstanding",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The latest timestamp before or at the given one for which no commitment is outstanding.
      |Note that this doesn't imply that pruning is possible at this timestamp, as the system might require some
      |additional data for crash recovery. Thus, this is useful for testing commitments; use the commands in the pruning
      |group for pruning.
      |Additionally, the result needn't fall on a "commitment tick" as specified by the reconciliation interval."""
  )
  def find_clean_commitments_timestamp(
      synchronizerAlias: SynchronizerAlias,
      beforeOrAt: CantonTimestamp = CantonTimestamp.now(),
  ): Option[CantonTimestamp] =
    state_inspection.noOutstandingCommitmentsTs(synchronizerAlias, beforeOrAt)

  @Help.Summary(
    "Obtain access to the state inspection interface. Use at your own risk.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The state inspection methods can fatally and permanently corrupt the state of a participant.
      |The API is subject to change in any way."""
  )
  def state_inspection: SyncStateInspection = check(FeatureFlag.Testing)(stateInspection)

  private def stateInspection: SyncStateInspection = access(node => node.sync.stateInspection)

  @Help.Summary("Lookup of accepted transactions by update ID", FeatureFlag.Testing)
  def lookup_transaction(updateId: String): Option[LfVersionedTransaction] =
    check(FeatureFlag.Testing)(
      access(
        _.sync.ledgerApiIndexer.asEval.value.onlyForTestingTransactionInMemoryStore match {
          case Some(onlyForTestingTransactionInMemoryStore) =>
            onlyForTestingTransactionInMemoryStore.get(updateId)

          case None =>
            throw new IllegalStateException(
              "lookup_transaction not supported: to use this feature please enable indexer configuration only-for-testing-enable-in-memory-transaction-store"
            )
        }
      )
    )
}

class ParticipantPruningAdministrationGroup(
    runner: LedgerApiCommandRunner & AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends PruningSchedulerAdministration(
      runner,
      consoleEnvironment,
      new PruningSchedulerCommands[PruningServiceStub](
        PruningServiceGrpc.stub,
        _.setSchedule(_),
        _.clearSchedule(_),
        _.setCron(_),
        _.setMaxDuration(_),
        _.setRetention(_),
        _.getSchedule(_),
      ),
      loggerFactory,
    )
    with FeatureFlagFilter
    with Helpful {

  import runner.*

  @Help.Summary("Prune the ledger up to the specified offset inclusively.")
  @Help.Description(
    """Prunes the participant ledger up to the specified offset inclusively returning ``Unit`` if the ledger has been
      |successfully pruned.
      |Note that upon successful pruning, subsequent attempts to read transactions via ``ledger_api.transactions.flat`` or
      |``ledger_api.transactions.trees`` or command completions via ``ledger_api.completions.list`` by specifying a begin offset
      |lower than the returned pruning offset will result in a ``NOT_FOUND`` error.
      |The ``prune`` operation performs a "full prune" freeing up significantly more space and also
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any synchronizer with events preceding the pruning offset."""
  )
  def prune(pruneUpTo: Long): Unit =
    consoleEnvironment.run(
      ledgerApiCommand(LedgerApiCommands.ParticipantPruningService.Prune(pruneUpTo))
    )

  @Help.Summary(
    "Return the highest participant ledger offset whose record time is before or at the given one (if any) at which pruning is safely possible",
    FeatureFlag.Preview,
  )
  def find_safe_offset(beforeOrAt: Instant = Instant.now()): Option[Long] =
    check(FeatureFlag.Preview) {
      val ledgerEnd = consoleEnvironment.run(
        ledgerApiCommand(LedgerApiCommands.StateService.LedgerEnd())
      )

      consoleEnvironment
        .run(
          adminCommand(
            ParticipantAdminCommands.Pruning
              .GetSafePruningOffsetCommand(beforeOrAt, ledgerEnd)
          )
        )
    }

  @Help.Summary(
    "Prune only internal ledger state up to the specified offset inclusively.",
    FeatureFlag.Preview,
  )
  @Help.Description(
    """Special-purpose variant of the ``prune`` command that prunes only partial,
      |internal participant ledger state freeing up space not needed for serving ``ledger_api.transactions``
      |and ``ledger_api.completions`` requests. In conjunction with ``prune``, ``prune_internally`` enables pruning
      |internal ledger state more aggressively than externally observable data via the ledger api. In most use cases
      |``prune`` should be used instead. Unlike ``prune``, ``prune_internally`` has no visible effect on the Ledger API.
      |The command returns ``Unit`` if the ledger has been successfully pruned or an error if the timestamp
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any synchronizer with events preceding the pruning offset."""
  )
  def prune_internally(pruneUpTo: Long): Unit =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run(
        adminCommand(ParticipantAdminCommands.Pruning.PruneInternallyCommand(pruneUpTo))
      )
    }

  @Help.Summary(
    "Activate automatic pruning according to the specified schedule with participant-specific options."
  )
  @Help.Description(
    """Refer to the ``set_schedule`` description for information about the "cron", "max_duration", and "retention"
      |parameters. Setting the "prune_internally_only" flag causes pruning to only remove internal state as described in
      |more detail in the ``prune_internally`` command description.
  """
  )
  def set_participant_schedule(
      cron: String,
      maxDuration: config.PositiveDurationSeconds,
      retention: config.PositiveDurationSeconds,
      pruneInternallyOnly: Boolean = false,
  ): Unit =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run(
        runner.adminCommand(
          SetParticipantScheduleCommand(
            cron,
            maxDuration,
            retention,
            pruneInternallyOnly,
          )
        )
      )
    }

  @Help.Summary("Inspect the automatic, participant-specific pruning schedule.")
  @Help.Description(
    """The schedule consists of a "cron" expression and "max_duration" and "retention" durations as described in the
      |``get_schedule`` command description. Additionally "prune_internally" indicates if the schedule mandates
      |pruning of internal state.
  """
  )
  def get_participant_schedule(): Option[ParticipantPruningSchedule] =
    consoleEnvironment.run(
      runner.adminCommand(GetParticipantScheduleCommand())
    )

  @Help.Summary(
    "Identify the participant ledger offset to prune up to based on the specified timestamp."
  )
  @Help.Description(
    """Return the largest participant ledger offset that has been processed before or at the specified timestamp.
      |The time is measured on the participant's local clock at some point while the participant has processed the
      |the event. Returns ``None`` if no such offset exists.
    """
  )
  def get_offset_by_time(upToInclusive: Instant): Option[Long] =
    consoleEnvironment.run(
      adminCommand(
        ParticipantAdminCommands.Inspection.LookupOffsetByTime(
          ProtoConverter.InstantConverter.toProtoPrimitive(upToInclusive)
        )
      )
    )
}

class LocalCommitmentsAdministrationGroup(
    runner: AdminCommandRunner with BaseInspection[ParticipantNode],
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends CommitmentsAdministrationGroup(runner, consoleEnvironment, loggerFactory) {

  import runner.*

  @Help.Summary(
    "Lookup ACS commitments received from other participants as part of the reconciliation protocol"
  )
  @Help.Description("""The arguments are:
       - synchronizerAlias: the alias of the synchronizer
       - start: lowest time exclusive
       - end: highest time inclusive
       - counterParticipant: optionally filter by counter participant
      """)
  def received(
      synchronizerAlias: SynchronizerAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[SignedProtocolMessage[AcsCommitment]] =
    access(node =>
      node.sync.stateInspection
        .findReceivedCommitments(
          synchronizerAlias,
          timestampFromInstant(start),
          timestampFromInstant(end),
          counterParticipant,
        )
    )

  @Help.Summary("Lookup ACS commitments locally computed as part of the reconciliation protocol")
  def computed(
      synchronizerAlias: SynchronizerAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.HashedCommitmentType)] =
    access { node =>
      node.sync.stateInspection.findComputedCommitments(
        synchronizerAlias,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

  def outstanding(
      synchronizerAlias: SynchronizerAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] =
    access { node =>
      node.sync.stateInspection.outstandingCommitments(
        synchronizerAlias,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

  def buffered(
      synchronizerAlias: SynchronizerAlias,
      endAtOrBefore: Instant,
  ): Iterable[AcsCommitment] =
    access { node =>
      node.sync.stateInspection.bufferedCommitments(
        synchronizerAlias,
        timestampFromInstant(endAtOrBefore),
      )
    }

  def lastComputedAndSent(
      synchronizerAlias: SynchronizerAlias
  ): Option[CantonTimestampSecond] =
    access { node =>
      node.sync.stateInspection.findLastComputedAndSent(synchronizerAlias)
    }
}

class CommitmentsAdministrationGroup(
    runner: AdminCommandRunner with BaseInspection[ParticipantNode],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful
    with NoTracing {

  import runner.*

  // TODO(#9557) R2
  @Help.Summary(
    "Opens a commitment by retrieving the metadata of active contracts shared with the counter-participant.",
    FeatureFlag.Preview,
  )
  @Help.Description(
    """ Retrieves the contract ids and the reassignment counters of the shared active contracts at the given timestamp
      | and on the given synchronizer.
      | Returns an error if the participant cannot retrieve the data for the given commitment anymore.
      | The arguments are:
      | - commitment: The commitment to be opened
      | - synchronizerId: The synchronizer for which the commitment was computed
      | - timestamp: The timestamp of the commitment. Needs to correspond to a commitment tick.
      | - counterParticipant: The counter participant to whom we previously sent the commitment
      | - timeout: Time limit for the grpc call to complete
      """
  )
  def open_commitment(
      commitment: AcsCommitment.HashedCommitmentType,
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
      counterParticipant: ParticipantId,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Seq[CommitmentContractMetadata] =
    check(FeatureFlag.Preview) {
      val counterContracts = consoleEnvironment.run {
        val responseObserver =
          new ByteStringStreamObserver[v30.OpenCommitmentResponse](_.chunk)

        def call: ConsoleCommandResult[Context.CancellableContext] =
          adminCommand(
            ParticipantAdminCommands.Inspection.OpenCommitment(
              responseObserver,
              commitment,
              synchronizerId,
              counterParticipant,
              timestamp,
            )
          )

        processResult(
          call,
          responseObserver.resultBytes,
          timeout,
          "Retrieving the shared contract metadata",
        )
      }

      val counterContractsMetadata =
        GrpcStreamingUtils.parseDelimitedFromTrusted[CommitmentContractMetadata](
          counterContracts.newInput(),
          CommitmentContractMetadata,
        ) match {
          case Left(msg) => throw DeserializationException(msg)
          case Right(output) => output
        }
      logger.debug(
        s"Retrieved metadata for ${counterContractsMetadata.size} contracts shared with $counterParticipant at time $timestamp on synchronizer $synchronizerId"
      )
      counterContractsMetadata
    }

  @Help.Summary(
    "Download states of contracts and contract payloads necessary for commitment inspection and reconciliation",
    FeatureFlag.Preview,
  )
  @Help.Description(
    """ Returns the contract states (created, assigned, unassigned, archived, unknown) of the given contracts on
      | all synchronizers the participant knows from the beginning of time until the present time on each synchronizer.
      | The command returns best-effort the contract changes available. Specifically, it does not fail if the ACS
      | and/or reassignment state has been pruned during the time interval, or if parts of the time interval
      | are ahead of the clean ACS state.
      | Optionally returns the contract payload if requested and available.
      | The arguments are:
      | - contracts: The contract ids whose state and payload we want to fetch
      | - timestamp: The timestamp when some counter-participants reported the given contracts as active on the
      | expected synchronizer.
      | - expectedSynchronizerId: The synchronizer that the contracts are expected to be active on
      | - downloadPayload: If true, the payload of the contracts is also downloaded
      | - timeout: Time limit for the grpc call to complete
      """
  )
  def inspect_commitment_contracts(
      contracts: Seq[LfContractId],
      timestamp: CantonTimestamp,
      expectedSynchronizerId: SynchronizerId,
      downloadPayload: Boolean = false,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Seq[CommitmentInspectContract] = {

    val contractsData = consoleEnvironment.run {
      val responseObserver =
        new ByteStringStreamObserver[v30.InspectCommitmentContractsResponse](_.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        adminCommand(
          ParticipantAdminCommands.Inspection.CommitmentContracts(
            responseObserver,
            contracts,
            expectedSynchronizerId,
            timestamp,
            downloadPayload,
          )
        )

      processResult(
        call,
        responseObserver.resultBytes,
        timeout,
        "Retrieving the shared contract metadata",
      )
    }

    val parsedContractsData =
      GrpcStreamingUtils
        .parseDelimitedFromTrusted[CommitmentInspectContract](
          contractsData.newInput(),
          CommitmentInspectContract,
        )
        .valueOr(msg => throw DeserializationException(msg))
    logger.debug(
      s"Requested data for ${contracts.size}, retrieved data for ${parsedContractsData.size} contracts at time $timestamp on any synchronizer"
    )
    parsedContractsData
  }

  @Help.Summary(
    "List the counter-participants of a participant and the ACS commitments received from them together with" +
      "the commitment state."
  )
  @Help.Description(
    """Optional filtering through the arguments:
      | synchronizerTimeRanges: Lists commitments received on the given synchronizers whose period overlaps with any of the given
      |  time ranges per synchronizer.
      |  If the list is empty, considers all synchronizers the participant is connected to.
      |  For synchronizers with an empty time range, considers the latest period the participant knows of for that synchronizer.
      |  Synchronizers can appear multiple times in the list with various time ranges, in which case we consider the
      |  union of the time ranges.
      |counterParticipants: Lists commitments received only from the given counter-participants. If a counter-participant
      |  is not a counter-participant on some synchronizer, no commitments appear in the reply from that counter-participant
      |  on that synchronizer.
      |commitmentState: Lists commitments that are in one of the given states. By default considers all states:
      |   - MATCH: the remote commitment matches the local commitment
      |   - MISMATCH: the remote commitment does not match the local commitment
      |   - BUFFERED: the remote commitment is buffered because the corresponding local commitment has not been computed yet
      |   - OUTSTANDING: we expect a remote commitment that has not yet been received
      |verboseMode: If false, the reply does not contain the commitment bytes. If true, the reply contains:
      |   - In case of a mismatch, the reply contains both the received and the locally computed commitment that do not match.
      |   - In case of outstanding, the reply does not contain any commitment.
      |   - In all other cases (match and buffered), the reply contains the received commitment.
           """
  )
  def lookup_received_acs_commitments(
      synchronizerTimeRanges: Seq[SynchronizerTimeRange],
      counterParticipants: Seq[ParticipantId],
      commitmentState: Seq[ReceivedCmtState],
      verboseMode: Boolean,
  ): Map[SynchronizerId, Seq[ReceivedAcsCmt]] =
    consoleEnvironment.run(
      runner.adminCommand(
        LookupReceivedAcsCommitments(
          synchronizerTimeRanges,
          counterParticipants,
          commitmentState,
          verboseMode,
        )
      )
    )

  @Help.Summary(
    "List the counter-participants of a participant and the ACS commitments that the participant computed and sent to" +
      "them, together with the commitment state."
  )
  @Help.Description(
    """Optional filtering through the arguments:
          | synchronizerTimeRanges: Lists commitments received on the given synchronizers whose period overlap with any of the
          |  given time ranges per synchronizer.
          |  If the list is empty, considers all synchronizers the participant is connected to.
          |  For synchronizers with an empty time range, considers the latest period the participant knows of for that synchronizer.
          |  Synchronizers can appear multiple times in the list with various time ranges, in which case we consider the
          |  union of the time ranges.
          |counterParticipants: Lists commitments sent only to the given counter-participants. If a counter-participant
          |  is not a counter-participant on some synchronizer, no commitments appear in the reply for that counter-participant
          |  on that synchronizer.
          |commitmentState: Lists sent commitments that are in one of the given states. By default considers all states:
          |   - MATCH: the local commitment matches the remote commitment
          |   - MISMATCH: the local commitment does not match the remote commitment
          |   - NOT_COMPARED: the local commitment has been computed and sent but no corresponding remote commitment has
          |     been received
          |verboseMode: If false, the reply does not contain the commitment bytes. If true, the reply contains:
          |   - In case of a mismatch, the reply contains both the received and the locally computed commitment that
          |     do not match.
          |   - In all other cases (match and not compared), the reply contains the sent commitment.
           """
  )
  def lookup_sent_acs_commitments(
      synchronizerTimeRanges: Seq[SynchronizerTimeRange],
      counterParticipants: Seq[ParticipantId],
      commitmentState: Seq[SentCmtState],
      verboseMode: Boolean,
  ): Map[SynchronizerId, Seq[SentAcsCmt]] =
    consoleEnvironment.run(
      runner.adminCommand(
        LookupSentAcsCommitments(
          synchronizerTimeRanges,
          counterParticipants,
          commitmentState,
          verboseMode,
        )
      )
    )

  @Help.Summary(
    "Disable waiting for commitments from the given counter-participants."
  )
  @Help.Description(
    """Disabling waiting for commitments disregards these counter-participants w.r.t. pruning,
      |which gives up non-repudiation for those counter-participants, but increases pruning resilience
      |to failures and slowdowns of those counter-participants and/or the network.
      |If the participant set is empty, the command does nothing."""
  )
  def set_no_wait_commitments_from(
      counterParticipants: Seq[ParticipantId],
      synchronizerIds: Seq[SynchronizerId],
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SetNoWaitCommitmentsFrom(
          counterParticipants,
          synchronizerIds,
        )
      )
    )

  @Help.Summary(
    "Enable waiting for commitments from the given counter-participants. " +
      "Waiting for commitments from all counter-participants is the default behavior; explicitly enabling waiting" +
      "for commitments is only necessary if it was previously disabled."
  )
  @Help.Description(
    """Enables waiting for commitments, which blocks pruning at offsets where commitments from these counter-participants
      |are missing.
      |If the participant set is empty or the synchronizer set is empty, the command does nothing."""
  )
  def set_wait_commitments_from(
      counterParticipants: Seq[ParticipantId],
      synchronizerIds: Seq[SynchronizerId],
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SetWaitCommitmentsFrom(
          counterParticipants,
          synchronizerIds,
        )
      )
    )

  @Help.Summary(
    "Retrieves the latest (i.e., w.r.t. the query execution time) configuration of waiting for commitments from counter-participants."
  )
  @Help.Description(
    """The configuration for waiting for commitments from counter-participants is returned as two sets:
      |a set of ignored counter-participants, the synchronizers and the timestamp, and a set of not-ignored
      |counter-participants and the synchronizers.
      |Filters by the specified counter-participants and synchronizers. If the counter-participant and / or
      |synchronizers are empty, it considers all synchronizers and participants known to the participant, regardless of
      |whether they share contracts with the participant.
      |Even if some participants may not be connected to some synchronizers at the time the query executes, the response still
      |includes them if they are known to the participant or specified in the arguments."""
  )
  def get_wait_commitments_config_from(
      synchronizers: Seq[SynchronizerId],
      counterParticipants: Seq[ParticipantId],
  ): (Seq[NoWaitCommitments], Seq[WaitCommitments]) =
    consoleEnvironment.run(
      runner.adminCommand(
        GetNoWaitCommitmentsFrom(
          synchronizers,
          counterParticipants,
        )
      )
    )

  @Help.Summary(
    "Configure metrics for slow counter-participants (i.e., that are behind in sending commitments) and" +
      "configure thresholds for when a counter-participant is deemed slow."
  )
  @Help.Description("""The configurations are per synchronizer or set of synchronizers and concern the following metrics
        |issued per synchronizer:
        | - The maximum number of intervals that a distinguished participant falls
        | behind. All participants that are not in the distinguished or the individual group are automatically part of the default group
        | - The maximum number of intervals that a participant in the default groups falls behind
        | - The number of participants in the distinguished group that are behind by at least `thresholdDistinguished`
        | reconciliation intervals.
        | - The number of participants not in the distinguished or the individual group that are behind by at least `thresholdDefault`
        | reconciliation intervals.
        | - Separate metric for each participant in `individualMetrics` argument tracking how many intervals that
        |participant is behind""")
  def set_config_for_slow_counter_participants(
      configs: Seq[SlowCounterParticipantSynchronizerConfig]
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SetConfigForSlowCounterParticipants(
          configs
        )
      )
    )

  @Help.Summary(
    "Add additional distinguished counter participants to already existing slow counter participant configuration."
  )
  @Help.Description(
    """The configuration can be extended by adding additional counter participants to existing synchronizers.
      | if a given synchronizer is not already configured then it will be ignored without error.
      |"""
  )
  def add_config_distinguished_slow_counter_participants(
      counterParticipantsDistinguished: Seq[ParticipantId],
      synchronizers: Seq[SynchronizerId],
  ): Unit = consoleEnvironment.run {
    val result = runner.adminCommand(
      GetConfigForSlowCounterParticipants(synchronizers)
    )
    result.toEither match {
      case Left(err) => sys.error(err)
      case Right(configs) =>
        val missing = synchronizers
          .flatMap(x => counterParticipantsDistinguished.map(y => (x, y)))
          .filter { case (synchronizerId, participantId) =>
            configs.exists(slowCp =>
              slowCp.synchronizerIds.contains(synchronizerId) && !slowCp.distinguishedParticipants
                .contains(
                  participantId
                )
            )
          }
          .groupBy { case (synchronizerId, _) => synchronizerId }
          .map { case (synchronizerId, participantSeq) =>
            synchronizerId -> participantSeq.map { case (_, participantId) => participantId }
          }
        if (missing.nonEmpty) {
          val newConfigs = missing.flatMap { case (synchronizerId, participantSeq) =>
            val existing = configs.find(slowCp => slowCp.synchronizerIds.contains(synchronizerId))
            existing match {
              case None => None
              case Some(config) =>
                Some(
                  new SlowCounterParticipantSynchronizerConfig(
                    Seq(synchronizerId),
                    config.distinguishedParticipants ++ participantSeq,
                    config.thresholdDistinguished,
                    config.thresholdDefault,
                    config.participantsMetrics,
                  )
                )
            }
          }
          runner.adminCommand(
            SetConfigForSlowCounterParticipants(
              newConfigs.toSeq
            )
          )
        } else CommandSuccessful.apply()

    }
  }

  @Help.Summary(
    "removes existing configurations from synchronizers and distinguished counter participants."
  )
  @Help.Description("""The configurations can be removed from distinguished counter participant and synchronizers
      | use empty sequences correlates to selecting all, so removing all distinguished participants
      | from a synchronizer can be done with Seq.empty for 'counterParticipantsDistinguished' and Seq(SynchronizerId) for synchronizers.
      | Leaving both sequences empty clears all configs on all synchronizers.
      |""")
  def remove_config_for_slow_counter_participants(
      counterParticipantsDistinguished: Seq[ParticipantId],
      synchronizers: Seq[SynchronizerId],
  ): Unit = consoleEnvironment.run {
    val result = runner.adminCommand(
      GetConfigForSlowCounterParticipants(synchronizers)
    )
    result.toEither match {
      case Left(err) => sys.error(err)
      case Right(configs) =>
        val toBeRemoved = synchronizers
          .zip(counterParticipantsDistinguished)
          .filter { case (synchronizerId, participantId) =>
            configs.exists(slowCp =>
              slowCp.synchronizerIds.contains(synchronizerId) && slowCp.distinguishedParticipants
                .contains(
                  participantId
                )
            )
          }
          .groupBy { case (synchronizerId, _) => synchronizerId }
          .view
          .mapValues(_.map { case (_, participantId) => participantId })
          .toMap

        val newConfigs = toBeRemoved.flatMap { case (synchronizerId, participantSeq) =>
          val existing = configs.find(slowCp => slowCp.synchronizerIds.contains(synchronizerId))
          existing match {
            case None => None
            case Some(config) =>
              Some(
                new SlowCounterParticipantSynchronizerConfig(
                  Seq(synchronizerId),
                  config.distinguishedParticipants.diff(participantSeq),
                  config.thresholdDistinguished,
                  config.thresholdDefault,
                  config.participantsMetrics,
                )
              )
          }
        }
        runner.adminCommand(
          SetConfigForSlowCounterParticipants(
            newConfigs.toSeq
          )
        )
    }
  }

  @Help.Summary(
    "Add additional individual metrics participants to already existing slow counter participant configuration."
  )
  @Help.Description(
    """The configuration can be extended by adding additional counter participants to existing synchronizers.
      | if a given synchronizer is not already configured then it will be ignored without error.
      |"""
  )
  def add_participant_to_individual_metrics(
      individualMetrics: Seq[ParticipantId],
      synchronizers: Seq[SynchronizerId],
  ): Unit = consoleEnvironment.run {
    val result = runner.adminCommand(
      GetConfigForSlowCounterParticipants(synchronizers)
    )
    result.toEither match {
      case Left(err) => sys.error(err)
      case Right(configs) =>
        val missing = synchronizers
          .zip(individualMetrics)
          .filter { case (synchronizerId, participantId) =>
            configs.exists(slowCp =>
              slowCp.synchronizerIds.contains(synchronizerId) && !slowCp.participantsMetrics
                .contains(
                  participantId
                )
            )
          }
          .groupBy { case (synchronizerId, _) => synchronizerId }
          .view
          .mapValues(_.map { case (_, participantId) => participantId })
          .toMap

        val newConfigs = missing.flatMap { case (synchronizerId, participantSeq) =>
          val existing = configs.find(slowCp => slowCp.synchronizerIds.contains(synchronizerId))
          existing match {
            case None => None
            case Some(config) =>
              Some(
                new SlowCounterParticipantSynchronizerConfig(
                  Seq(synchronizerId),
                  config.distinguishedParticipants,
                  config.thresholdDistinguished,
                  config.thresholdDefault,
                  config.participantsMetrics ++ participantSeq,
                )
              )
          }
        }
        runner.adminCommand(
          SetConfigForSlowCounterParticipants(
            newConfigs.toSeq
          )
        )
    }
  }
  @Help.Summary(
    "removes existing configurations from synchronizers and individual metrics participants."
  )
  @Help.Description(
    """The configurations can be removed from individual metrics counter participant and synchronizers
      | use empty sequences correlates to selecting all, so removing all individual metrics participants
      | from a synchronizer can be done with Seq.empty for 'individualMetrics' and Seq(SynchronizerId) for synchronizers.
      | Leaving both sequences empty clears all configs on all synchronizers.
      |"""
  )
  def remove_participant_from_individual_metrics(
      individualMetrics: Seq[ParticipantId],
      synchronizers: Seq[SynchronizerId],
  ): Unit = consoleEnvironment.run {
    val result = runner.adminCommand(
      GetConfigForSlowCounterParticipants(synchronizers)
    )
    result.toEither match {
      case Left(err) => sys.error(err)
      case Right(configs) =>
        val toBeRemoved = synchronizers
          .zip(individualMetrics)
          .filter { case (synchronizerId, participantId) =>
            configs.exists(slowCp =>
              slowCp.synchronizerIds.contains(synchronizerId) && slowCp.participantsMetrics
                .contains(
                  participantId
                )
            )
          }
          .groupBy { case (synchronizerId, _) => synchronizerId }
          .view
          .mapValues(_.map { case (_, participantId) => participantId })
          .toMap

        val newConfigs = toBeRemoved.flatMap { case (synchronizerId, participantSeq) =>
          val existing = configs.find(slowCp => slowCp.synchronizerIds.contains(synchronizerId))
          existing match {
            case None => None
            case Some(config) =>
              Some(
                new SlowCounterParticipantSynchronizerConfig(
                  Seq(synchronizerId),
                  config.distinguishedParticipants,
                  config.thresholdDistinguished,
                  config.thresholdDefault,
                  config.participantsMetrics.diff(participantSeq),
                )
              )
          }
        }
        runner.adminCommand(
          SetConfigForSlowCounterParticipants(
            newConfigs.toSeq
          )
        )
    }
  }

  @Help.Summary(
    "Lists for the given synchronizers the configuration of metrics for slow counter-participants (i.e., that" +
      "are behind in sending commitments)"
  )
  @Help.Description("""Lists the following config per synchronizer. If `synchronizers` is empty, the command lists config for all
                       synchronizers:
      "| - The participants in the distinguished group, which have two metrics:
      the maximum number of intervals that a participant is behind, and the number of participants that are behind
      by at least `thresholdDistinguished` reconciliation intervals
      | - The participants not in the distinguished group, which have two metrics: the maximum number of intervals that a participant
      | is behind, and the number of participants that are behind by at least `thresholdDefault` reconciliation intervals
      | - Parameters `thresholdDistinguished` and `thresholdDefault`
      | - The participants in `individualMetrics`, which have individual metrics per participant showing how many
      reconciliation intervals that participant is behind""")
  def get_config_for_slow_counter_participants(
      synchronizers: Seq[SynchronizerId]
  ): Seq[SlowCounterParticipantSynchronizerConfig] =
    consoleEnvironment.run(
      runner.adminCommand(
        GetConfigForSlowCounterParticipants(synchronizers)
      )
    )

  def get_config_for_slow_counter_participant(
      synchronizers: Seq[SynchronizerId],
      counterParticipants: Seq[ParticipantId],
  ): Seq[SlowCounterParticipantSynchronizerConfig] =
    get_config_for_slow_counter_participants(synchronizers).filter(config =>
      config.participantsMetrics.exists(metricParticipant =>
        counterParticipants.contains(metricParticipant) ||
          config.distinguishedParticipants.exists(distinguished =>
            counterParticipants.contains(distinguished)
          )
      )
    )

  @Help.Summary(
    "Lists for every participant and synchronizer the number of intervals that the participant is behind in sending commitments" +
      "if that participant is behind by at least threshold intervals."
  )
  @Help.Description(
    """If `counterParticipants` is empty, the command considers all counter-participants.
      |If `synchronizers` is empty, the command considers all synchronizers.
      |If `threshold` is not set, the command considers 0.
      |For counter-participant that never sent a commitment, the output shows they are
      |behind by MaxInt"""
  )
  def get_intervals_behind_for_counter_participants(
      counterParticipants: Seq[ParticipantId],
      synchronizers: Seq[SynchronizerId],
      threshold: Option[NonNegativeInt],
  ): Seq[CounterParticipantInfo] =
    consoleEnvironment.run(
      runner.adminCommand(
        GetIntervalsBehindForCounterParticipants(
          counterParticipants,
          synchronizers,
          threshold.getOrElse(NonNegativeInt.zero),
        )
      )
    )

  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
}

class ParticipantReplicationAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  @Help.Summary("Set the participant replica to passive")
  @Help.Description(
    """Trigger a graceful fail-over from this active replica to another passive replica.
      |The command completes after the replica had a chance to become active.
      |After this command you need to check the health status of this replica to ensure it is not active anymore."""
  )
  def set_passive(): Unit =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Replication.SetPassiveCommand()
      )
    }
}

/** Administration commands supported by a participant.
  */
trait ParticipantAdministration extends FeatureFlagFilter {
  this: AdminCommandRunner
    with LedgerApiCommandRunner
    with LedgerApiAdministration
    with NamedLogging =>

  import ConsoleEnvironment.Implicits.*
  implicit protected val consoleEnvironment: ConsoleEnvironment

  private val runner = this

  def id: ParticipantId

  protected def waitPackagesVetted(
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
  ): Unit

  protected def participantIsActiveOnSynchronizer(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
  ): Boolean

  @Help.Summary("Manage DAR packages")
  @Help.Group("DAR Management")
  object dars extends Helpful {
    @Help.Summary(
      "Remove a DAR from the participant",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """Can be used to remove a DAR from the participant, if the following conditions are satisfied:
        |1. The main package of the DAR must be unused -- there should be no active contract from this package
        |
        |2. All package dependencies of the DAR should either be unused or contained in another of the participant node's uploaded DARs. Canton uses this restriction to ensure that the package dependencies of the DAR don't become "stranded" if they're in use.
        |
        |3. The main package of the dar should not be vetted. If it is vetted, Canton will try to automatically
        |   revoke the vetting for the main package of the DAR, but this automatic vetting revocation will only succeed if the
        |   main package vetting originates from a standard ``dars.upload``. Even if the automatic revocation fails, you can
        |   always manually revoke the package vetting.
        |
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the synchronizer.
        |"""
    )
    def remove(mainPackageId: String, synchronizeVetting: Boolean = true): Unit = {
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemoveDar(mainPackageId))
      })
      if (synchronizeVetting) {
        packages.synchronize_vetting()
      }
    }

    @Help.Summary("List installed DAR files")
    @Help.Description("""List DARs installed on this participant
      |The arguments are:
      |  filterName: filter by name
      |  filterDescription: filter by description
      |  limit: Limit number of results (default none)
      """)
    def list(
        limit: PositiveInt = defaultLimit,
        filterName: String = "",
        filterDescription: String = "",
    ): Seq[DarDescription] =
      consoleEnvironment
        .run {
          adminCommand(ParticipantAdminCommands.Package.ListDars(filterName, limit))
        }
        .filter(_.description.startsWith(filterDescription))

    @Help.Summary("List contents of DAR files")
    def get_contents(mainPackageId: String): DarContents = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Package.GetDarContents(mainPackageId)
      )
    }

    @Help.Summary("Upload a DAR to Canton")
    @Help.Description("""Daml code is normally shipped as a Dar archive and must explicitly be uploaded to a participant.
        |A Dar is a collection of LF-packages, the native binary representation of Daml smart contracts.
        |
        |The Dar can be provided either as a link to a local file or as a URL. If a URL is provided, then
        |any request headers can be provided as a map. The Dar will be downloaded and then uploaded to the participant.
        |
        |In order to use Daml templates on a participant, the Dar must first be uploaded and then
        |vetted by the participant. Vetting will ensure that other participants can check whether they
        |can actually send a transaction referring to a particular Daml package and participant.
        |Vetting is done by registering a VettedPackages topology transaction with the topology manager.
        |By default, vetting happens automatically and this command waits for
        |the vetting transaction to be successfully registered on all connected synchronizers.
        |This is the safe default setting minimizing race conditions.
        |
        |If vetAllPackages is true (default), the packages will all be vetted on all synchronizers the participant is registered.
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the synchronizer.
        |
        |Note that synchronize vetting might block on permissioned synchronizers that do not just allow participants to update the topology state.
        |In such cases, synchronizeVetting should be turned off.
        |Synchronize vetting can be invoked manually using $participant.package.synchronize_vettings()
        |""")
    def upload(
        path: String,
        description: String = "",
        vetAllPackages: Boolean = true,
        synchronizeVetting: Boolean = true,
        expectedMainPackageId: String = "",
        requestHeaders: Map[String, String] = Map(),
    ): String = {
      val res = consoleEnvironment.runE {
        for {
          mainPackageId <- ParticipantCommands.dars
            .upload(
              runner,
              path = path,
              description = description,
              vetAllPackages = vetAllPackages,
              synchronizeVetting = synchronizeVetting,
              expectedMainPackageId = expectedMainPackageId,
              requestHeaders = requestHeaders,
              logger,
            )
            .toEither
        } yield mainPackageId
      }
      if (synchronizeVetting && vetAllPackages) {
        packages.synchronize_vetting()
      }
      res
    }

    @Help.Summary("Upload many DARs to Canton")
    @Help.Description("""Daml code is normally shipped as a Dar archive and must explicitly be uploaded to a participant.
        |A Dar is a collection of LF-packages, the native binary representation of Daml smart contracts.
        |
        |The Dars can be provided either as a link to a local file or as a URL. If a URL is provided, then
        |any request headers can be provided as a map. The Dars will be downloaded and then uploaded to the participant.
        |
        |In order to use Daml templates on a participant, the Dars must first be uploaded and then
        |vetted by the participant. Vetting will ensure that other participants can check whether they
        |can actually send a transaction referring to a particular Daml package and participant.
        |Vetting is done by registering a VettedPackages topology transaction with the topology manager.
        |By default, vetting happens automatically and this command waits for
        |the vetting transaction to be successfully registered on all connected synchronizers.
        |This is the safe default setting minimizing race conditions.
        |
        |If vetAllPackages is true (default), the packages will all be vetted on all synchronizers the participant is registered.
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the synchronizer.
        |
        |Note that synchronize vetting might block on permissioned synchronizers that do not just allow participants to update the topology state.
        |In such cases, synchronizeVetting should be turned off.
        |Synchronize vetting can be invoked manually using $participant.package.synchronize_vettings()
        |""")
    def upload_many(
        paths: Seq[String],
        vetAllPackages: Boolean = true,
        synchronizeVetting: Boolean = true,
        requestHeaders: Map[String, String] = Map(),
    ): Seq[String] = {
      val res = consoleEnvironment.runE {
        for {
          mainPackageId <- ParticipantCommands.dars
            .upload_many(
              runner,
              paths,
              vetAllPackages = vetAllPackages,
              synchronizeVetting = synchronizeVetting,
              requestHeaders = requestHeaders,
              logger,
            )
            .toEither
        } yield mainPackageId
      }
      if (synchronizeVetting && vetAllPackages) {
        packages.synchronize_vetting()
      }
      res
    }

    @Help.Summary("Validate DARs against the current participants' state")
    @Help.Description(
      """Performs the same DAR and Daml package validation checks that the upload call performs,
         but with no effects on the target participants: the DAR is not persisted or vetted."""
    )
    def validate(path: String): String =
      consoleEnvironment.runE {
        for {
          hash <- ParticipantCommands.dars
            .validate(runner, path, logger)
            .toEither
        } yield hash
      }

    @Help.Summary("Downloads the DAR file with the provided main package-id to the given directory")
    def download(mainPackageId: String, directory: String): Unit = {
      val _ = consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Package.GetDar(mainPackageId, directory, logger)
        )
      }
    }

    @Help.Summary("Change DAR vetting status")
    @Help.Group("Vetting")
    object vetting extends Helpful {
      @Help.Summary(
        "Vet all packages contained in the DAR archive identified by the provided main package-id."
      )
      def enable(mainPackageId: String, synchronize: Boolean = true): Unit =
        check(FeatureFlag.Preview)(consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Package.VetDar(mainPackageId, synchronize))
        })

      @Help.Summary(
        "Revoke vetting for all packages contained in the DAR archive identified by the provided main package-id."
      )
      @Help.Description("""This command succeeds if the vetting command used to vet the DAR's packages
          |was symmetric and resulted in a single vetting topology transaction for all the packages in the DAR.
          |This command is potentially dangerous and misuse
          |can lead the participant to fail in processing transactions""")
      def disable(mainPackageId: String): Unit =
        check(FeatureFlag.Preview)(consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Package.UnvetDar(mainPackageId))
        })
    }

  }

  @Help.Summary("Manage raw Daml-LF packages")
  @Help.Group("Package Management")
  object packages extends Helpful {

    @Help.Summary("List packages stored on the participant")
    @Help.Description("""Supported arguments:

        limit - Limit on the number of packages returned (defaults to canton.parameters.console.default-limit)
        """)
    def list(filterName: String = "", limit: PositiveInt = defaultLimit): Seq[PackageDescription] =
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.List(filterName = filterName, limit = limit))
      }

    @Help.Summary("Get the package contents")
    def get_contents(packageId: String): PackageContents = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.GetContents(packageId))
    }

    @Help.Summary("Returns the list of DARs referencing the given package")
    def get_references(packageId: String): Seq[DarDescription] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.GetReferences(packageId))
    }

    @Help.Summary("Find packages that contain a module with the given name")
    def find_by_module(
        moduleName: String,
        limitPackages: PositiveInt = defaultLimit,
    ): Seq[PackageDescription] = consoleEnvironment.runE {
      val packageC = adminCommand(
        ParticipantAdminCommands.Package.List(filterName = "", limit = limitPackages)
      ).toEither
      val matchingC = packageC
        .flatMap { packages =>
          packages.traverse(x =>
            adminCommand(ParticipantAdminCommands.Package.GetContents(x.packageId)).toEither.map(
              r => (x, r)
            )
          )
        }
      matchingC.map(_.filter { case (_, content) =>
        content.modules.contains(moduleName)
      }.map(_._1))
    }

    @Help.Summary(
      "Remove the package from Canton's package store.",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """The standard operation of this command checks that a package is unused and unvetted, and if so
        |removes the package. The force flag can be used to disable the checks, but do not use the force flag unless
        |you're certain you know what you're doing. """
    )
    def remove(packageId: String, force: Boolean = false): Unit =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemovePackage(packageId, force))
      })

    @Help.Summary(
      "Ensure that all vetting transactions issued by this participant have been observed by all configured participants"
    )
    @Help.Description("""Sometimes, when scripting tests and demos, a dar or package is uploaded and we need to ensure
        |that commands are only submitted once the package vetting has been observed by some other connected participant
        |known to the console. This command can be used in such cases.""")
    def synchronize_vetting(
        timeout: config.NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit =
      waitPackagesVetted(timeout)
  }

  @Help.Summary("Manage synchronizer connections")
  @Help.Group("Synchronizers")
  object synchronizers extends Helpful {

    @Help.Summary("Returns the id of the given synchronizer alias")
    def id_of(synchronizerAlias: SynchronizerAlias): SynchronizerId =
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.SynchronizerConnectivity.GetSynchronizerId(synchronizerAlias)
        )
      }

    @Help.Summary(
      "Test whether a participant is connected to and permissioned on a synchronizer."
    )
    @Help.Description(
      """Yields false, if the synchronizer is not connected or not healthy.
        |Yields false, if the synchronizer is configured in the Canton configuration and
        |the participant is not active from the perspective of the synchronizer."""
    )
    def active(synchronizerAlias: SynchronizerAlias): Boolean =
      list_connected().exists { r =>
        r.synchronizerAlias == synchronizerAlias &&
        r.healthy &&
        participantIsActiveOnSynchronizer(r.synchronizerId, id)
      }

    @Help.Summary(
      "Test whether a participant is connected to a synchronizer"
    )
    def is_connected(synchronizerId: SynchronizerId): Boolean =
      list_connected().exists(_.synchronizerId == synchronizerId)

    @Help.Summary(
      "Test whether a participant is connected to a synchronizer"
    )
    def is_connected(synchronizerAlias: SynchronizerAlias): Boolean =
      list_connected().exists(_.synchronizerAlias == synchronizerAlias)

    @Help.Summary(
      "Macro to connect a participant to a locally configured synchronizer given by sequencer reference"
    )
    @Help.Description("""
        The arguments are:
          sequencer - A local sequencer reference
          alias - The name you will be using to refer to this synchronizer. Can not be changed anymore.
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          maxRetryDelayMillis - Maximal amount of time (in milliseconds) between two connection attempts.
          priority - The priority of the synchronizer. The higher the more likely a synchronizer will be used.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        sequencer: SequencerReference,
        alias: SynchronizerAlias,
        manualConnect: Boolean = false,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit = {
      val config = ParticipantCommands.synchronizers.reference_to_config(
        NonEmpty.mk(Seq, SequencerAlias.Default -> sequencer).toMap,
        alias,
        manualConnect,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.tryOfMillis),
        priority,
      )
      connect_by_config(config, validation, synchronize)
    }

    @Help.Summary(
      "Macro to register a locally configured synchronizer given by sequencer reference"
    )
    @Help.Description("""
        The arguments are:
          sequencer - A local sequencer reference
          alias - The name you will be using to refer to this synchronizer. Cannot be changed anymore.
          performHandshake - If true (default), will perform a handshake with the synchronizer. If no, will only store the configuration without any query to the synchronizer.
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          maxRetryDelayMillis - Maximal amount of time (in milliseconds) between two connection attempts.
          priority - The priority of the synchronizer. The higher the more likely a synchronizer will be used.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
          validation - Whether to validate the connectivity and ids of the given sequencers (default All)
        """)
    def register(
        sequencer: SequencerReference,
        alias: SynchronizerAlias,
        performHandshake: Boolean = true,
        manualConnect: Boolean = false,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit = {
      val config = ParticipantCommands.synchronizers.reference_to_config(
        NonEmpty.mk(Seq, SequencerAlias.Default -> sequencer).toMap,
        alias,
        manualConnect = manualConnect,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.tryOfMillis),
        priority,
      )
      register_by_config(config, performHandshake = performHandshake, validation, synchronize)
    }

    @Help.Summary(
      "Macro to register a locally configured synchronizer"
    )
    @Help.Description("""
        The arguments are:
          config - Config for the synchronizer connection
          performHandshake - If true (default), will perform handshake with the synchronizer. If no, will only store configuration without any query to the synchronizer.
          validation - Whether to validate the connectivity and ids of the given sequencers (default All)
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def register_by_config(
        config: SynchronizerConnectionConfig,
        performHandshake: Boolean = true,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val current = this.config(config.synchronizerAlias)
      // if the config is not found, then we register the synchronizer
      if (current.isEmpty) {
        // register the synchronizer configuration
        consoleEnvironment.run {
          ParticipantCommands.synchronizers.register(
            runner,
            config,
            performHandshake = performHandshake,
            validation,
          )
        }
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    def connect_local_bft(
        synchronizer: NonEmpty[Map[SequencerAlias, SequencerReference]],
        alias: SynchronizerAlias,
        manualConnect: Boolean = false,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        sequencerTrustThreshold: PositiveInt = PositiveInt.one,
        submissionRequestAmplification: SubmissionRequestAmplification =
          SubmissionRequestAmplification.NoAmplification,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit = {
      val config = ParticipantCommands.synchronizers.reference_to_config(
        synchronizer,
        alias,
        manualConnect,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.tryOfMillis),
        priority,
        sequencerTrustThreshold,
        submissionRequestAmplification,
      )
      connect_by_config(config, validation, synchronize)
    }

    @Help.Summary("Macro to connect a participant to a synchronizer given by connection")
    @Help.Description("""This variant of connect expects a synchronizer connection config.
        |Otherwise the behaviour is equivalent to the connect command with explicit
        |arguments. If the synchronizer is already configured, the synchronizer connection
        |will be attempted. If however the synchronizer is offline, the command will fail.
        |Generally, this macro should only be used for the first connection to a new synchronizer. However, for
        |convenience, we support idempotent invocations where subsequent calls just ensure
        |that the participant reconnects to the synchronizer.

        validation - Whether to validate the connectivity and ids of the given sequencers (default all)
        |""")
    def connect_by_config(
        config: SynchronizerConnectionConfig,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit = {
      val current = this.config(config.synchronizerAlias)

      if (current.isEmpty) {
        // architecture-handbook-entry-begin: OnboardParticipantConnect
        // connect to the new synchronizer
        consoleEnvironment.run {
          ParticipantCommands.synchronizers.connect(runner, config, validation)
        }
        // architecture-handbook-entry-end: OnboardParticipantConnect
      } else {
        reconnect(config.synchronizerAlias, retry = false).discard
      }

      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Macro to connect a participant to a synchronizer given by instance")
    @Help.Description("""This variant of connect expects an instance with a sequencer connection.
        |Otherwise the behaviour is equivalent to the connect command with explicit
        |arguments. If the synchronizer is already configured, the synchronizer connection
        |will be attempted. If however the synchronizer is offline, the command will fail.
        |Generally, this macro should only be used for the first connection to a new synchronizer. However, for
        |convenience, we support idempotent invocations where subsequent calls just ensure
        |that the participant reconnects to the synchronizer.
        |""")
    def connect(
        instance: SequencerReference,
        synchronizerAlias: SynchronizerAlias,
    ): Unit =
      connect_by_config(
        SynchronizerConnectionConfig(
          synchronizerAlias,
          SequencerConnections.single(instance.sequencerConnection),
        )
      )

    @Help.Summary("Macro to connect a participant to a synchronizer given by connection")
    @Help.Description("""The connect macro performs a series of commands in order to connect this participant to a synchronizer.
        |First, `register` will be invoked with the given arguments, but first registered
        |with manualConnect = true. If you already set manualConnect = true, then nothing else
        |will happen and you will have to do the remaining steps yourselves.
        |Finally, the command will invoke `reconnect` to startup the connection.
        |If the reconnect succeeded, the registered configuration will be updated
        |with manualStart = true. If anything fails, the synchronizer will remain registered with `manualConnect = true` and
        |you will have to perform these steps manually.
        The arguments are:
          synchronizerAlias - The name you will be using to refer to this synchronizer. Cannot be changed anymore.
          connection - The connection string to connect to this synchronizer. I.e. https://url:port
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          synchronizerId - Optionally the synchronizerId you expect to see on this synchronizer.
          certificatesPath - Path to TLS certificate files to use as a trust anchor.
          priority - The priority of the synchronizer. The higher the more likely a synchronizer will be used.
          timeTrackerConfig - The configuration for the synchronizer time tracker.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
          validation - Whether to validate the connectivity and ids of the given sequencers (default All)
        """)
    def connect(
        synchronizerAlias: SynchronizerAlias,
        connection: String,
        manualConnect: Boolean = false,
        synchronizerId: Option[SynchronizerId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        timeTrackerConfig: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): SynchronizerConnectionConfig = {
      val config = ParticipantCommands.synchronizers.to_config(
        synchronizerAlias,
        connection,
        manualConnect,
        synchronizerId,
        certificatesPath,
        priority,
        timeTrackerConfig = timeTrackerConfig,
      )
      connect_by_config(config, validation, synchronize)
      config
    }

    @Help.Summary(
      "Macro to connect a participant to a synchronizer that supports connecting via many endpoints"
    )
    @Help.Description("""Synchronizers can provide many endpoints to connect to for availability and performance benefits.
        This version of connect allows specifying multiple endpoints for a single synchronizer connection:
           connect_multi("mysynchronizer", Seq(sequencer1, sequencer2))
           or:
           connect_multi("mysynchronizer", Seq("https://host1.mysynchronizer.net", "https://host2.mysynchronizer.net", "https://host3.mysynchronizer.net"))

        To create a more advanced connection config use synchronizers.toConfig with a single host,
        |then use config.addConnection to add additional connections before connecting:
           config = myparticipaint.synchronizers.toConfig("mysynchronizer", "https://host1.mysynchronizer.net", ...otherArguments)
           config = config.addConnection("https://host2.mysynchronizer.net", "https://host3.mysynchronizer.net")
           myparticipant.synchronizers.connect(config)

        The arguments are:
          synchronizerAlias - The name you will be using to refer to this synchronizer. Cannot be changed anymore.
          connections - The sequencer connection definitions (can be an URL) to connect to this synchronizer. I.e. https://url:port
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
          validation - Whether to validate the connectivity and ids of the given sequencers (default All)
        """)
    def connect_multi(
        synchronizerAlias: SynchronizerAlias,
        connections: Seq[SequencerConnection],
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): SynchronizerConnectionConfig = {
      val sequencerConnection =
        SequencerConnection.merge(connections).getOrElse(sys.error("Invalid sequencer connection"))
      val sequencerConnections =
        SequencerConnections.single(sequencerConnection)
      val config = SynchronizerConnectionConfig(
        synchronizerAlias,
        sequencerConnections,
      )
      connect_by_config(config, validation, synchronize)
      config
    }

    @Help.Summary("Reconnect this participant to the given synchronizer")
    @Help.Description("""Idempotent attempts to re-establish a connection to a certain synchronizer.
        |If retry is set to false, the command will throw an exception if unsuccessful.
        |If retry is set to true, the command will terminate after the first attempt with the result,
        |but the server will keep on retrying to connect to the synchronizer.
        |
        The arguments are:
          synchronizerAlias - The name you will be using to refer to this synchronizer. Cannot be changed anymore.
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisily if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect(
        synchronizerAlias: SynchronizerAlias,
        retry: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = {
      val ret = consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.SynchronizerConnectivity
            .ReconnectSynchronizer(synchronizerAlias, retry)
        )
      }
      if (ret) {
        synchronize.foreach { timeout =>
          ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
        }
      }
      ret
    }

    @Help.Summary("Reconnect this participant to the given local synchronizer")
    @Help.Description("""Idempotent attempts to re-establish a connection to the given local synchronizer.
        |Same behaviour as generic reconnect.

        The arguments are:
          ref - The synchronizer reference to connect to
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisily if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect_local(
        ref: SequencerReference
    ): Boolean = reconnect(ref.name)

    @Help.Summary("Reconnect this participant to the given local synchronizer")
    @Help.Description("""Idempotent attempts to re-establish a connection to the given local synchronizer.
        |Same behaviour as generic reconnect.

        The arguments are:
          synchronizerAlias - The synchronizer alias to connect to
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisily if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect_local(
        synchronizerAlias: SynchronizerAlias,
        retry: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = reconnect(synchronizerAlias, retry, synchronize)

    @Help.Summary(
      "Reconnect this participant to all synchronizer which are not marked as manual start"
    )
    @Help.Description("""
      The arguments are:
          ignoreFailures - If set to true (default), we'll attempt to connect to all, ignoring any failure
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
    """)
    def reconnect_all(
        ignoreFailures: Boolean = true,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.SynchronizerConnectivity.ReconnectSynchronizers(ignoreFailures)
        )
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Disconnect this participant from the given synchronizer")
    def disconnect(synchronizerAlias: SynchronizerAlias): Unit = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.DisconnectSynchronizer(synchronizerAlias)
      )
    }

    @Help.Summary("Disconnect this participant from all connected synchronizers")
    def disconnect_all(): Unit =
      list_connected().foreach { connected =>
        disconnect(connected.synchronizerAlias)
      }

    @Help.Summary("Disconnect this participant from the given local synchronizer")
    def disconnect_local(synchronizerAlias: SynchronizerAlias): Unit = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.DisconnectSynchronizer(synchronizerAlias)
      )
    }

    @Help.Summary("List the connected synchronizers of this participant")
    def list_connected(): Seq[ListConnectedSynchronizersResult] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.SynchronizerConnectivity.ListConnectedSynchronizers())
    }

    @Help.Summary("List the configured synchronizer of this participant")
    @Help.Description(
      "For each returned synchronizer, the boolean indicates whether the participant is currently connected to the synchronizer."
    )
    def list_registered(): Seq[(SynchronizerConnectionConfig, Boolean)] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers)
    }

    @Help.Summary("Returns true if a synchronizer is registered using the given alias")
    def is_registered(synchronizerAlias: SynchronizerAlias): Boolean =
      config(synchronizerAlias).nonEmpty

    @Help.Summary("Returns the current configuration of a given synchronizer")
    def config(synchronizerAlias: SynchronizerAlias): Option[SynchronizerConnectionConfig] =
      list_registered().map(_._1).find(_.synchronizerAlias == synchronizerAlias)

    @Help.Summary("Modify existing synchronizer connection")
    def modify(
        synchronizerAlias: SynchronizerAlias,
        modifier: SynchronizerConnectionConfig => SynchronizerConnectionConfig,
        validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
    ): Unit =
      consoleEnvironment.runE {
        for {
          registeredSynchronizers <- adminCommand(
            ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers
          ).toEither
          cfg <- registeredSynchronizers
            .collectFirst {
              case (config, _) if config.synchronizerAlias == synchronizerAlias => config
            }
            .toRight(s"No such synchronizer $synchronizerAlias configured")
          newConfig = modifier(cfg)
          _ <- Either.cond(
            newConfig.synchronizerAlias == cfg.synchronizerAlias,
            (),
            "We don't support modifying the synchronizer alias of a SynchronizerConnectionConfig.",
          )
          _ <- adminCommand(
            ParticipantAdminCommands.SynchronizerConnectivity.ModifySynchronizerConnection(
              modifier(cfg),
              validation,
            )
          ).toEither
        } yield ()
      }

    @Help.Summary(
      "Revoke this participant's authentication tokens and close all the sequencer connections in the given synchronizer"
    )
    @Help.Description("""
      synchronizerAlias: the synchronizer alias from which to logout
      On all the sequencers from the specified synchronizer, all existing authentication tokens for this participant
      will be revoked.
      Note that the participant is not disconnected from the synchronizer; only the connections to the sequencers are closed.
      The participant will automatically reopen connections, perform a challenge-response and obtain new tokens.
      """)
    def logout(synchronizerAlias: SynchronizerAlias): Unit = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.Logout(synchronizerAlias)
      )
    }
  }

  @Help.Summary("Functionality for managing resources")
  @Help.Group("Resource Management")
  object resources extends Helpful {

    @Help.Summary("Set resource limits for the participant.")
    @Help.Description(
      """While a resource limit is attained or exceeded, the participant will reject any additional submission with GRPC status ABORTED.
        |Most importantly, a submission will be rejected **before** it consumes a significant amount of resources.
        |
        |There are three kinds of limits: `maxInflightValidationRequests`,  `maxSubmissionRate` and `maxSubmissionBurstFactor`.
        |The number of inflight validation requests of a participant P covers (1) requests initiated by P as well as
        |(2) requests initiated by participants other than P that need to be validated by P.
        |Compared to the maximum rate, the maximum number of inflight validation requests reflects the load on the participant more accurately.
        |However, the maximum number of inflight validation requests alone does not protect the system from "bursts":
        |If an application submits a huge number of commands at once, the maximum number of inflight validation requests will likely
        |be exceeded, as the system is registering inflight validation requests only during validation and not already during
        |submission.
        |
        |The maximum rate is a hard limit on the rate of commands submitted to this participant through the Ledger API.
        |As the rate of commands is checked and updated immediately after receiving a new command submission,
        |an application cannot exceed the maximum rate.
        |
        |The `maxSubmissionBurstFactor` parameter (positive, default 0.5) allows to configure how permissive the rate limitation should be
        |with respect to bursts. The rate limiting will be enforced strictly after having observed `max_burst` * `max_submission_rate` commands.
        |
        |For the sake of illustration, let's assume the configured rate limit is ``100 commands/s`` with a burst ratio of 0.5.
        |If an application submits 100 commands within a single second, waiting exactly 10 milliseconds between consecutive commands,
        |then the participant will accept all commands.
        |With a `maxSubmissionBurstFactor` of 0.5, the participant will accept the first 50 commands and reject the remaining 50.
        |If the application then waits another 500 ms, it may submit another burst of 50 commands. If it waits 250 ms,
        |it may submit only a burst of 25 commands.
        |
        |Resource limits can only be changed, if the server runs Canton enterprise.
        |In the community edition, the server uses fixed limits that cannot be changed."""
    )
    def set_resource_limits(limits: ResourceLimits): Unit =
      consoleEnvironment.run(adminCommand(SetResourceLimits(limits)))

    @Help.Summary("Get the resource limits of the participant.")
    def resource_limits(): ResourceLimits = consoleEnvironment.run {
      adminCommand(GetResourceLimits())
    }

  }
}

trait ParticipantHealthAdministrationCommon extends FeatureFlagFilter {
  this: HealthAdministration[ParticipantStatus] =>

  protected def runner: AdminCommandRunner

  // Single internal implementation so that `maybe_ping`
  // can be hidden behind the `testing` feature flag
  private def ping_internal(
      participantId: ParticipantId,
      timeout: NonNegativeDuration,
      synchronizerId: Option[SynchronizerId],
      id: String,
  ): Either[String, Duration] =
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            Set[String](participantId.adminParty.toLf),
            Set(),
            timeout,
            0,
            synchronizerId,
            "",
            id,
          )
      )
    }

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. " +
      "Yields the duration in case of success and throws a RuntimeException in case of failure."
  )
  def ping(
      participantId: ParticipantId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ping,
      synchronizerId: Option[SynchronizerId] = None,
      id: String = "",
  ): Duration = {
    val adminApiRes: Either[String, Duration] =
      ping_internal(participantId, timeout, synchronizerId, id)
    consoleEnvironment.runE(
      adminApiRes.leftMap { reason =>
        s"Unable to ping $participantId within ${LoggerUtil
            .roundDurationForHumans(timeout.duration)}: $reason"
      }
    )

  }

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. Yields Some(duration) in case of success and None in case of failure.",
    FeatureFlag.Testing,
  )
  def maybe_ping(
      participantId: ParticipantId,
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.ping,
      synchronizerId: Option[SynchronizerId] = None,
      id: String = "",
  ): Option[Duration] = check(FeatureFlag.Testing) {
    ping_internal(participantId, timeout, synchronizerId, id).toOption
  }
}

class ParticipantHealthAdministration(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends HealthAdministration[ParticipantStatus](
      runner,
      consoleEnvironment,
    )
    with FeatureFlagFilter
    with ParticipantHealthAdministrationCommon {
  override protected def nodeStatusCommand: GrpcAdminCommand[?, ?, NodeStatus[ParticipantStatus]] =
    ParticipantAdminCommands.Health.ParticipantStatusCommand()

  @Help.Summary("Counts pending command submissions and transactions on a synchronizer.")
  @Help.Description(
    """This command finds the current number of pending command submissions and transactions on a selected synchronizer.
      |
      |There is no synchronization between pending command submissions and transactions. And the respective
      |counts are an indication only!
      |
      |This command is in particular useful to re-assure oneself that there are currently no in-flight submissions
      |or transactions present for the selected synchronizer. Such re-assurance is then helpful to proceed with repair
      |operations, for example."""
  )
  def count_in_flight(synchronizerAlias: SynchronizerAlias): InFlightCount = {
    val synchronizerId = consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.SynchronizerConnectivity.GetSynchronizerId(synchronizerAlias)
      )
    }

    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Inspection.CountInFlight(synchronizerId)
      )
    }
  }
}
