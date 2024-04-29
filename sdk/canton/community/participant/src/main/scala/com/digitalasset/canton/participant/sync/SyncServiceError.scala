// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.error.{ErrorCategory, ErrorCode, ErrorGroup, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.LoadSequencerEndpointInformationResult
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.SyncServiceErrorGroup
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.{
  Alarm,
  AlarmErrorCode,
  CantonError,
  CombinedError,
  ParentCantonError,
  TransactionErrorImpl,
}
import com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.domain.DomainRegistryError
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{DomainAlias, LedgerSubmissionId}
import com.google.rpc.status.Status
import io.grpc.Status.Code
import org.slf4j.event.Level

trait SyncServiceError extends Serializable with Product with CantonError

object SyncServiceInjectionError extends InjectionErrorGroup {

  import com.daml.lf.data.Ref.{ApplicationId, CommandId}

  @Explanation("This error results if a command is submitted to the passive replica.")
  @Resolution("Send the command to the active replica.")
  object PassiveReplica
      extends ErrorCode(
        id = "NODE_IS_PASSIVE_REPLICA",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error(applicationId: ApplicationId, commandId: CommandId)
        extends TransactionErrorImpl(
          cause = "Cannot process submitted command. This participant is the passive replica."
        )
  }

  @Explanation(
    "This errors results if a command is submitted to a participant that is not connected to any domain."
  )
  @Resolution(
    "Connect your participant to the domain where the given parties are hosted."
  )
  object NotConnectedToAnyDomain
      extends ErrorCode(
        id = "NOT_CONNECTED_TO_ANY_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error()
        extends TransactionErrorImpl(cause = "This participant is not connected to any domain.")
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
    "This error results if a domain connectivity command is referring to a domain alias that has not been registered."
  )
  @Resolution(
    "Please confirm the domain alias is correct, or configure the domain before (re)connecting."
  )
  object SyncServiceUnknownDomain
      extends ErrorCode(
        "SYNC_SERVICE_UNKNOWN_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = s"The domain with alias ${domain.unwrap} is unknown.")
        with SyncServiceError
  }

  @Explanation(
    "This error results on an attempt to register a new domain under an alias already in use."
  )
  object SyncServiceAlreadyAdded
      extends ErrorCode(
        "SYNC_SERVICE_ALREADY_ADDED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = "The domain with the given alias has already been added.")
        with SyncServiceError
  }

  @Explanation(
    """This error is reported in case of validation failures when attempting to register new or change existing
       sequencer connections. This can be caused by unreachable nodes, a bad TLS configuration, or in case of
       a mismatch of domain-ids reported by the sequencers or mismatched sequencer-ids within a sequencer group."""
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
          cause = s"The provided sequencer connections are inconsistent: ${errors}."
        )
        with SyncServiceError
  }

  abstract class MigrationErrors extends ErrorGroup()

  abstract class DomainRegistryErrorGroup extends ErrorGroup()

  final case class SyncServiceFailedDomainConnection(
      domain: DomainAlias,
      parent: DomainRegistryError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[DomainRegistryError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] = Map("domain" -> domain.unwrap)

  }

  final case class SyncServiceMigrationError(
      from: DomainAlias,
      to: DomainAlias,
      parent: SyncDomainMigrationError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncServiceError
      with ParentCantonError[SyncDomainMigrationError] {

    override def logOnCreation: Boolean = false

    override def mixinContext: Map[String, String] = Map("from" -> from.unwrap, "to" -> to.unwrap)

  }

  @Explanation(
    "This error is logged when the synchronization service shuts down because the remote sequencer API is denying access."
  )
  @Resolution(
    "Contact the sequencer operator and inquire why you are not allowed to connect anymore."
  )
  object SyncServiceDomainDisabledUs
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISABLED_US",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(domain: DomainAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain rejected our subscription attempt with permission denied."
        )
  }

  @Explanation(
    "This error is logged when a sync domain has a non-active status."
  )
  @Resolution(
    """If you attempt to connect to a domain that has either been migrated off or has a pending migration,
      |this error will be emitted. Please complete the migration before attempting to connect to it."""
  )
  object SyncServiceDomainIsNotActive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_STATUS_NOT_ACTIVE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(domain: DomainAlias, status: DomainConnectionConfigStore.Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"$domain has status $status and can therefore not be connected to."
        )
        with SyncServiceError
  }

  @Explanation(
    "This error is logged when a sync domain is disconnected because the participant became passive."
  )
  @Resolution("Fail over to the active participant replica.")
  object SyncServiceDomainBecamePassive
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_BECAME_PASSIVE",
        ErrorCategory.TransientServerFailure,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Error(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"$domain disconnected because participant became passive."
        )
  }

  @Explanation(
    "This error is emitted when an operation is attempted such as repair that requires the domain connection to be disconnected and clean."
  )
  @Resolution("Disconnect the domain before attempting the command.")
  object SyncServiceDomainMustBeOffline
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_MUST_BE_OFFLINE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Error(domain: DomainAlias)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = show"$domain must be disconnected for the given operation")
        with SyncServiceError

  }

  @Explanation(
    "This error is logged when a sync domain is unexpectedly disconnected from the Canton " +
      "sync service (after having previously been connected)"
  )
  @Resolution("Please contact support and provide the failure reason.")
  object SyncServiceDomainDisconnect
      extends ErrorCode(
        "SYNC_SERVICE_DOMAIN_DISCONNECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class UnrecoverableError(domain: DomainAlias, _reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = show"$domain fatally disconnected because of ${_reason}")

    final case class UnrecoverableException(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"Domain $domain fatally disconnected because of an exception ${throwable.getMessage}",
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

    final case class Failure(domain: DomainAlias, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The domain failed to startup due to an internal error",
          throwableO = Some(throwable),
        )
        with SyncServiceError

    final case class DomainIsMissingInternally(domain: DomainAlias, where: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to await for participant becoming active due to missing domain objects"
        )
        with SyncServiceError
    final case class CleanHeadAwaitFailed(domain: DomainAlias, ts: CantonTimestamp, err: String)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Failed to await for clean-head at ${ts}: $err"
        )
        with SyncServiceError
  }

  @Explanation("The participant has detected that another node is behaving maliciously.")
  @Resolution("Contact support.")
  object SyncServiceAlarm extends AlarmErrorCode("SYNC_SERVICE_ALARM") {
    final case class Warn(override val cause: String) extends Alarm(cause)
  }

  @Explanation("This error indicates a sync domain failed to start or initialize properly.")
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
    ) extends CombinedError[SyncServiceError]
        with SyncServiceError

    final case class InitError(
        domain: DomainAlias,
        error: SyncDomainInitializationError,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "The domain failed to initialize due to an error")
        with SyncServiceError
  }

  @Explanation(
    """The participant is not connected to a domain and can therefore not allocate a party
    because the party notification is configured as ``party-notification.type = via-domain``."""
  )
  @Resolution(
    "Connect the participant to a domain first or change the participant's party notification config to ``eager``."
  )
  object PartyAllocationNoDomainError
      extends ErrorCode(
        "PARTY_ALLOCATION_WITHOUT_CONNECTED_DOMAIN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(submission_id: LedgerSubmissionId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Cannot allocate a party without being connected to a domain"
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
