// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.LoadSequencerEndpointInformationResult
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.SyncServiceErrorGroup
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.{
  CantonError,
  CombinedError,
  ContextualizedCantonError,
  ParentCantonError,
  TransactionErrorImpl,
}
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LedgerSubmissionId, SynchronizerAlias}
import com.google.rpc.status.Status
import io.grpc.Status.Code
import org.slf4j.event.Level

trait SyncServiceError extends Serializable with Product with ContextualizedCantonError

object SyncServiceInjectionError extends InjectionErrorGroup {

  import com.digitalasset.daml.lf.data.Ref.{UserId, CommandId}

  @Explanation("This error results if a command is submitted to the passive replica.")
  @Resolution("Send the command to the active replica.")
  object PassiveReplica
      extends ErrorCode(
        id = "NODE_IS_PASSIVE_REPLICA",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error(userId: UserId, commandId: CommandId)
        extends TransactionErrorImpl(
          cause = "Cannot process submitted command. This participant is the passive replica."
        )
  }

  @Explanation(
    "This error results when specific requests are submitted to a participant that is not connected to any synchronizer."
  )
  @Resolution("Connect your participant to a synchronizer.")
  object NotConnectedToAnySynchronizer
      extends ErrorCode(
        id = "NOT_CONNECTED_TO_ANY_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error()
        extends TransactionErrorImpl(
          cause = "This participant is not connected to any synchronizer."
        )
  }

  @Explanation(
    "This errors results if a command is submitted to a participant that is not connected to a synchronizer needed to process the command."
  )
  @Resolution(
    "Connect your participant to the required synchronizer."
  )
  object NotConnectedToSynchronizer
      extends ErrorCode(
        id = "NOT_CONNECTED_TO_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(synchronizer: String)
        extends TransactionErrorImpl(
          cause = s"This participant is not connected to synchronizer $synchronizer."
        )
  }

  @Explanation("This errors occurs if an internal error results in an exception.")
  @Resolution("Contact support.")
  object InjectionFailure
      extends ErrorCode(
        id = "COMMAND_INJECTION_FAILURE",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(throwable: Throwable)
        extends TransactionErrorImpl(
          cause = "Command failed with an exception",
          throwableO = Some(throwable),
        )
  }

}

object SyncServiceError extends SyncServiceErrorGroup {

  @Explanation(
    "This error results if a synchronizer connectivity command is referring to a synchronizer alias that has not been registered."
  )
  @Resolution(
    "Please confirm the synchronizer alias is correct, or configure the synchronizer before (re)connecting."
  )
  object SyncServiceUnknownSynchronizer
      extends ErrorCode(
        "SYNC_SERVICE_UNKNOWN_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(synchronizerAlias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The synchronizer with alias ${synchronizerAlias.unwrap} is unknown."
        )
        with SyncServiceError
  }

  @Explanation(
    "This error results on an attempt to register a new synchronizer under an alias already in use."
  )
  object SyncServiceAlreadyAdded
      extends ErrorCode(
        "SYNC_SERVICE_ALREADY_ADDED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Error(synchronizerAlias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The synchronizer with the given alias has already been added."
        )
        with SyncServiceError
  }

  @Explanation("This error results if a admin command is submitted to the passive replica.")
  @Resolution("Send the admin command to the active replica.")
  object SyncServicePassiveReplica
      extends ErrorCode(
        id = "SYNC_SERVICE_PASSIVE_REPLICA",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Cannot process submitted command. This participant is the passive replica."
        )
        with SyncServiceError
  }

  @Explanation(
    """This error is reported in case of validation failures when attempting to register new or change existing
       sequencer connections. This can be caused by unreachable nodes, a bad TLS configuration, or in case of
       a mismatch of synchronizer ids reported by the sequencers or mismatched sequencer-ids within a sequencer group."""
  )
  @Resolution(
    """Check that the connection settings provided are correct. If they are but correspond to temporarily
       inactive sequencers, you may also turn off the validation.
      """
  )
  object SyncServiceInconsistentConnectivity
      extends ErrorCode(
        "SYNC_SERVICE_BAD_CONNECTIVITY",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(errors: Seq[LoadSequencerEndpointInformationResult.NotValid])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The provided sequencer connections are inconsistent: $errors."
        )
        with SyncServiceError
  }

  abstract class MigrationErrors extends ErrorGroup()

  abstract class SynchronizerRegistryErrorGroup extends ErrorGroup()

  final case class SyncServiceFailedSynchronizerConnection(
      synchronizerAlias: SynchronizerAlias,
      parent: SynchronizerRegistryError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[SynchronizerRegistryError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] = Map("synchronizer" -> synchronizerAlias.unwrap)

  }

  final case class SyncServiceMigrationError(
      from: Source[SynchronizerAlias],
      to: Target[SynchronizerAlias],
      parent: SynchronizerMigrationError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[SynchronizerMigrationError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] =
      Map("from" -> from.unwrap.unwrap, "to" -> to.unwrap.unwrap)

  }

  @Explanation(
    "This error is logged when the synchronization service shuts down because the remote sequencer API is denying access."
  )
  @Resolution(
    "Contact the sequencer operator and inquire why you are not allowed to connect anymore."
  )
  object SyncServiceSynchronizerDisabledUs
      extends ErrorCode(
        "SYNC_SERVICE_SYNCHRONIZER_DISABLED_US",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(synchronizerAlias: SynchronizerAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$synchronizerAlias rejected our subscription attempt with permission denied."
        )
  }

  @Explanation(
    "This error is logged when a synchronizer has a non-active status."
  )
  @Resolution(
    """If you attempt to connect to a synchronizer that has either been migrated off or has a pending migration,
      |this error will be emitted. Please complete the migration before attempting to connect to it."""
  )
  object SyncServiceSynchronizerIsNotActive
      extends ErrorCode(
        "SYNC_SERVICE_SYNCHRONIZER_STATUS_NOT_ACTIVE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(
        synchronizerAlias: SynchronizerAlias,
        status: SynchronizerConnectionConfigStore.Status,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"$synchronizerAlias has status $status and can therefore not be connected to."
        )
        with SyncServiceError
  }

  final case class SyncServicePurgeSynchronizerError(
      synchronizerAlias: SynchronizerAlias,
      parent: PruningServiceError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[PruningServiceError]

  @Explanation(
    "This error is logged when a connected synchronizer is disconnected because the participant became passive."
  )
  @Resolution("Fail over to the active participant replica.")
  object SyncServiceBecamePassive
      extends ErrorCode(
        "SYNC_SERVICE_BECAME_PASSIVE",
        ErrorCategory.TransientServerFailure,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(synchronizerAlias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$synchronizerAlias disconnected because participant became passive."
        )
  }

  @Explanation(
    "This error is emitted when an operation is attempted such as repair that requires the synchronizers to be disconnected and clean."
  )
  @Resolution("Disconnect the still connected synchronizers before attempting the command.")
  object SyncServiceSynchronizersMustBeOffline
      extends ErrorCode(
        "SYNC_SERVICE_SYNCHRONIZERS_MUST_BE_OFFLINE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(connectedSynchronizers: Seq[SynchronizerAlias])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$connectedSynchronizers must be disconnected for the given operation"
        )
        with SyncServiceError

  }

  @Explanation(
    "This error is emitted when a synchronizer migration is attempted while transactions are still in-flight on the source synchronizer."
  )
  @Resolution(
    """Ensure the source synchronizer has no in-flight transactions by reconnecting participants to the source synchronizer, halting
      |activity on the participants and waiting for in-flight transactions to complete or time out. Afterwards invoke
      |`migrate_synchronizer` again. As a last resort, you may force the synchronizer migration ignoring in-flight transactions using
      |the `force` flag on the command. Be advised, forcing a migration may lead to a ledger fork."""
  )
  object SyncServiceSynchronizerMustNotHaveInFlightTransactions
      extends ErrorCode(
        "SYNC_SERVICE_SYNCHRONIZER_MUST_NOT_HAVE_IN_FLIGHT_TRANSACTIONS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(synchronizerAlias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$synchronizerAlias must not have in-flight transactions"
        )
        with SyncServiceError

  }

  @Explanation(
    "This error is logged when a connected synchronizer is unexpectedly disconnected from the Canton " +
      "sync service (after having previously been connected)"
  )
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceSynchronizerDisconnect
      extends ErrorCode(
        "SYNC_SERVICE_SYNCHRONIZER_DISCONNECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class UnrecoverableError(synchronizerAlias: SynchronizerAlias, _reason: String)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$synchronizerAlias fatally disconnected because of ${_reason}"
        )

    final case class UnrecoverableException(
        synchronizerAlias: SynchronizerAlias,
        throwable: Throwable,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"Synchronizer $synchronizerAlias fatally disconnected because of an exception ${throwable.getMessage}",
          throwableO = Some(throwable),
        )

  }

  @Explanation("This error indicates an internal issue.")
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceInternalError
      extends ErrorCode(
        "SYNC_SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class Failure(synchronizerAlias: SynchronizerAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The synchronizer failed to startup due to an internal error",
          throwableO = Some(throwable),
        )
        with SyncServiceError

    final case class SynchronizerIsMissingInternally(
        synchronizerAlias: SynchronizerAlias,
        where: String,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Failed to await for participant becoming active due to missing synchronizer objects"
        )
        with SyncServiceError
    final case class CleanHeadAwaitFailed(
        synchronizerAlias: SynchronizerAlias,
        ts: CantonTimestamp,
        err: String,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Failed to await for clean-head at $ts: $err"
        )
        with SyncServiceError
  }

  @Explanation("The participant has detected that another node is behaving maliciously.")
  @Resolution("Contact support.")
  object SyncServiceAlarm extends AlarmErrorCode("SYNC_SERVICE_ALARM") {
    final case class Warn(override val cause: String) extends Alarm(cause)
  }

  @Explanation("This error indicates a synchronizer failed to start or initialize properly.")
  @Resolution(
    "Please check the underlying error(s) and retry if possible. If not, contact support and provide the failure reason."
  )
  object SyncServiceStartupError
      extends ErrorCode(
        "SYNC_SERVICE_STARTUP_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class CombinedStartError(override val errors: NonEmpty[Seq[SyncServiceError]])(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CombinedError
        with SyncServiceError

    final case class InitError(
        synchronizerAlias: SynchronizerAlias,
        error: ConnectedSynchronizerInitializationError,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "The synchronizer failed to initialize due to an error")
        with SyncServiceError
  }

  @Explanation(
    """The participant is not connected to a synchronizer and can therefore not allocate a party
    because the party notification is configured as ``party-notification.type = via-synchronizer``."""
  )
  @Resolution(
    "Connect the participant to a synchronizer first or change the participant's party notification config to ``eager``."
  )
  object PartyAllocationNoSynchronizerError
      extends ErrorCode(
        "PARTY_ALLOCATION_WITHOUT_CONNECTED_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(submission_id: LedgerSubmissionId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Cannot allocate a party without being connected to a synchronizer"
        )
  }

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_SYNC_SERVICE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
  }

  object Synchronous {

    val NoErrorDetails = Seq.empty[com.google.protobuf.any.Any]

    val NotSupported: SubmissionResult.SynchronousError = SubmissionResult.SynchronousError(
      Status.of(Code.UNIMPLEMENTED.value, "Not supported", NoErrorDetails)
    )
    val PassiveNode: SubmissionResult.SynchronousError = SubmissionResult.SynchronousError(
      Status.of(Code.UNAVAILABLE.value, "Node is passive", NoErrorDetails)
    )

    def internalError(reason: String): SubmissionResult.SynchronousError =
      SubmissionResult.SynchronousError(Status.of(Code.INTERNAL.value, reason, NoErrorDetails))

    val shutdownError: SubmissionResult.SynchronousError =
      SubmissionResult.SynchronousError(
        Status.of(Code.CANCELLED.value, "Node is shutting down", NoErrorDetails)
      )
  }
}
