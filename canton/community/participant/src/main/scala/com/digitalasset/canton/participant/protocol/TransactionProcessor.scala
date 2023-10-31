// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import com.daml.error.*
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.SubmissionErrorGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.digitalasset.canton.ledger.participant.state.v2.{
  ChangeId,
  SubmitterInfo,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.ProcessorError
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmitted,
  buildAuthenticator,
}
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.ConfirmationRequestCreationError
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.participant.protocol.submission.{
  ConfirmationRequestFactory,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.protocol.validation.{
  ConfirmationResponseFactory,
  InternalConsistencyChecker,
  ModelConformanceChecker,
}
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SequencerClient}
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.ExecutionContext

class TransactionProcessor(
    override val participantId: ParticipantId,
    confirmationRequestFactory: ConfirmationRequestFactory,
    domainId: DomainId,
    damle: DAMLe,
    staticDomainParameters: StaticDomainParameters,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    metrics: TransactionProcessingMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    skipRecipientsCheck: Boolean,
    enableContractUpgrading: Boolean,
)(implicit val ec: ExecutionContext)
    extends ProtocolProcessor[
      TransactionProcessingSteps.SubmissionParam,
      TransactionSubmitted,
      TransactionViewType,
      TransactionResultMessage,
      TransactionProcessor.TransactionSubmissionError,
    ](
      new TransactionProcessingSteps(
        domainId,
        participantId,
        confirmationRequestFactory,
        new ConfirmationResponseFactory(
          participantId,
          domainId,
          staticDomainParameters.protocolVersion,
          loggerFactory,
        ),
        ModelConformanceChecker(
          damle,
          confirmationRequestFactory.transactionTreeFactory,
          buildAuthenticator(crypto),
          staticDomainParameters.protocolVersion,
          participantId,
          enableContractUpgrading,
          loggerFactory,
        ),
        staticDomainParameters,
        crypto,
        ephemeral.contractStore,
        metrics,
        buildAuthenticator(crypto),
        new AuthenticationValidator(),
        new AuthorizationValidator(participantId, enableContractUpgrading),
        new InternalConsistencyChecker(
          staticDomainParameters.uniqueContractKeys,
          staticDomainParameters.protocolVersion,
          loggerFactory,
        ),
        loggerFactory,
        futureSupervisor,
      ),
      inFlightSubmissionTracker,
      ephemeral,
      crypto,
      sequencerClient,
      domainId,
      staticDomainParameters.protocolVersion,
      loggerFactory,
      futureSupervisor,
      skipRecipientsCheck = skipRecipientsCheck,
    ) {

  def submit(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessor.TransactionSubmissionError,
    FutureUnlessShutdown[
      TransactionSubmitted
    ],
  ] =
    this.submit(
      TransactionProcessingSteps.SubmissionParam(
        submitterInfo,
        transactionMeta,
        keyResolver,
        transaction,
        disclosedContracts,
      )
    )
}

object TransactionProcessor {

  private def buildAuthenticator(
      crypto: DomainSyncCryptoClient
  ): SerializableContractAuthenticatorImpl = new SerializableContractAuthenticatorImpl(
    new UnicumGenerator(crypto.pureCrypto)
  )

  sealed trait TransactionProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None
  }

  trait TransactionSubmissionError extends TransactionProcessorError with TransactionError {
    override def pretty: Pretty[TransactionSubmissionError] = {
      this.prettyOfString(_ =>
        this.code.toMsg(cause, None) + "; " + ContextualizedErrorLogger.formatContextAsString(
          context
        )
      )
    }
  }

  object SubmissionErrors extends SubmissionErrorGroup {

    // TODO(i5990) split the text into sub-categories with codes
    @Explanation(
      """This error has not yet been properly categorised into sub-error codes."""
    )
    object MalformedRequest
        extends ErrorCode(
          id = "MALFORMED_REQUEST",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      // TODO(i5990) properly set `definiteAnswer` where appropriate when sub-categories are created
      final case class Error(message: String, reason: ConfirmationRequestCreationError)
          extends TransactionErrorImpl(cause = "Malformed request")
    }

    @Explanation(
      """This error occurs if a transaction was submitted referring to a package that
        |a receiving participant has not vetted. Any transaction view can only refer to packages that have
        |explicitly been approved by the receiving participants."""
    )
    @Resolution(
      """Ensure that the receiving participant uploads and vets the respective package."""
    )
    object PackageNotVettedByRecipients
        extends ErrorCode(
          id = "PACKAGE_NOT_VETTED_BY_RECIPIENTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(unknownTo: Seq[PackageUnknownTo])
          extends TransactionErrorImpl(
            cause =
              "Not all receiving participants have vetted a package that is referenced by the submitted transaction",
            // Reported asynchronously after in-flight submission checking, so covered by the rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error occurs if a transaction was submitted referring to a contract that
        |is not known on the domain. This can occur in case of race conditions between a transaction and
        |an archival or transfer-out."""
    )
    @Resolution(
      """Check domain for submission and/or re-submit the transaction."""
    )
    object UnknownContractDomain
        extends ErrorCode(
          id = "UNKNOWN_CONTRACT_DOMAIN",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(contractId: LfContractId)
          extends TransactionErrorImpl(
            cause = "Not all receiving participants have the contract in their contract store",
            // Reported asynchronously after in-flight submission checking, so covered by the rank guarantee
            definiteAnswer = true,
          )
    }

    // TODO(#7348) Add the submission rank of the in-flight submission
    final case class SubmissionAlreadyInFlight(
        changeId: ChangeId,
        existingSubmissionId: Option[LedgerSubmissionId],
        existingSubmissionDomain: DomainId,
    ) extends TransactionErrorImpl(cause = "The submission is already in flight")(
          ConsistencyErrors.SubmissionAlreadyInFlight.code
        )
        with TransactionSubmissionError

    @Explanation(
      """This error occurs when the sequencer refuses to accept a command due to backpressure."""
    )
    @Resolution("Wait a bit and retry, preferably with some backoff factor.")
    object DomainBackpressure
        extends ErrorCode(id = "DOMAIN_BACKPRESSURE", ErrorCategory.ContentionOnSharedResources) {
      override def logLevel: Level = Level.INFO

      final case class Rejection(reason: String)
          extends TransactionErrorImpl(
            cause = "The domain is overloaded.",
            // Only reported asynchronously, so covered by submission rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """The participant has rejected all incoming commands during a configurable grace period."""
    )
    @Resolution("""Configure more restrictive resource limits (enterprise only).
        |Change applications to submit commands at a lower rate.
        |Configure a higher value for `myParticipant.parameters.warnIfOverloadedFor`.""")
    object ParticipantOverloaded
        extends ErrorCode(
          id = "PARTICIPANT_OVERLOADED",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      override def logLevel: Level = Level.WARN

      final case class Rejection(duration: Duration)
          extends TransactionErrorImpl(
            cause = show"The participant has been overloaded for $duration."
          )
    }

    @Explanation(
      """This error occurs when a command is submitted while the system is performing a shutdown."""
    )
    @Resolution(
      "Assuming that the participant will restart or failover eventually, retry in a couple of seconds."
    )
    object SubmissionDuringShutdown
        extends ErrorCode(
          id = "SUBMISSION_DURING_SHUTDOWN",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      final case class Rejection()
          extends TransactionErrorImpl(cause = "Command submitted during shutdown.")
          with TransactionSubmissionError
    }

    @Explanation("""This error occurs when the command cannot be sent to the domain.""")
    object SequencerRequest
        extends ErrorCode(
          id = "SEQUENCER_REQUEST_FAILED",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      // TODO(i5990) proper send async client errors
      //  SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded) is already mapped to DomainBackpressure
      final case class Error(sendError: SendAsyncClientError)
          extends TransactionErrorImpl(
            cause = "Failed to send command",
            // Only reported asynchronously via timely rejections, so covered by submission rank guarantee
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error occurs when the transaction was not sequenced within the pre-defined max-sequencing time
        |and has therefore timed out. The max-sequencing time is derived from the transaction's ledger time via
        |the ledger time model skews.
        |"""
    )
    @Resolution(
      """Resubmit if the delay is caused by high load.
        |If the command requires substantial processing on the participant,
        |specify a higher minimum ledger time with the command submission so that a higher max sequencing time is derived.
        |Alternatively, you can increase the dynamic domain parameter ledgerTimeRecordTimeTolerance.
        |"""
    )
    object TimeoutError
        extends ErrorCode(id = "NOT_SEQUENCED_TIMEOUT", ErrorCategory.ContentionOnSharedResources) {
      final case class Error(timestamp: CantonTimestamp)
          extends TransactionErrorImpl(
            cause =
              "Transaction was not sequenced within the pre-defined max sequencing time and has therefore timed out"
          )
          with TransactionSubmissionError
    }

    @Explanation("The participant routed the transaction to a domain without an active mediator.")
    @Resolution("Add a mediator to the domain.")
    object DomainWithoutMediatorError
        extends ErrorCode(
          id = "DOMAIN_WITHOUT_MEDIATOR",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Error(topology_snapshot_timestamp: CantonTimestamp, chosen_domain: DomainId)
          extends TransactionErrorImpl(
            cause = "There are no active mediators on the domain"
          )
          with TransactionSubmissionError
    }

    @Explanation("At least one of the transaction's input contracts could not be authenticated.")
    @Resolution("Retry the submission with correctly authenticated contracts.")
    object ContractAuthenticationFailed extends AlarmErrorCode("CONTRACT_AUTHENTICATION_FAILED") {
      final case class Error(contractId: LfContractId, message: String)
          extends Alarm(
            cause = s"Contract with id (${contractId.coid}) could not be authenticated: $message"
          )
          with TransactionSubmissionError
    }

    @Explanation(
      "The mediator chosen for the transaction got deactivated before the request was sequenced."
    )
    @Resolution("Resubmit.")
    object InactiveMediatorError
        extends ErrorCode(
          id = "CHOSEN_MEDIATOR_IS_INACTIVE",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      final case class Error(chosen_mediator: MediatorRef, timestamp: CantonTimestamp)
          extends TransactionErrorImpl(
            cause = "the chosen mediator is not active on the domain"
          )
    }
  }

  final case class DomainParametersError(domainId: DomainId, context: String)
      extends TransactionProcessorError {
    override def pretty: Pretty[DomainParametersError] = prettyOfClass(
      param("domain", _.domainId),
      param("context", _.context.unquoted),
    )
  }

  final case class GenericStepsError(error: ProcessorError) extends TransactionProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def pretty: Pretty[GenericStepsError] = prettyOfParam(_.error)
  }

  final case class ViewParticipantDataError(
      transactionId: TransactionId,
      viewHash: ViewHash,
      error: String,
  ) extends TransactionProcessorError {
    override def pretty: Pretty[ViewParticipantDataError] = prettyOfClass(
      param("transaction id", _.transactionId),
      param("view hash", _.viewHash),
      param("error", _.error.unquoted),
    )
  }

  final case class FieldConversionError(field: String, error: String)
      extends TransactionProcessorError {
    override def pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }

  case object TransactionSubmitted
  type TransactionSubmitted = TransactionSubmitted.type
}
