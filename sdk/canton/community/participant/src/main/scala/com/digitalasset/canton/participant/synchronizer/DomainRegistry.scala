// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import com.daml.error.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.SequencerInfoLoaderError
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.sync.SyncServiceError.DomainRegistryErrorGroup
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

/** A registry of domains. */
trait DomainRegistry extends AutoCloseable {

  /**  Returns a domain handle that is used to setup a connection to a new domain
    */
  def connect(
      config: SynchronizerConnectionConfig
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[SynchronizerRegistryError, DomainHandle]]

}

sealed trait SynchronizerRegistryError extends Product with Serializable with CantonError

object SynchronizerRegistryError extends DomainRegistryErrorGroup {

  def fromSequencerInfoLoaderError(
      error: SequencerInfoLoaderError
  )(implicit loggingContext: ErrorLoggingContext): SynchronizerRegistryError =
    error match {
      case SequencerInfoLoaderError.DeserializationFailure(cause) =>
        SynchronizerRegistryError.DomainRegistryInternalError.DeserializationFailure(cause)
      case SequencerInfoLoaderError.InvalidResponse(cause) =>
        SynchronizerRegistryError.DomainRegistryInternalError.InvalidResponse(cause, None)
      case SequencerInfoLoaderError.InvalidState(cause) =>
        SynchronizerRegistryError.DomainRegistryInternalError.InvalidState(cause)
      case SequencerInfoLoaderError.SynchronizerIsNotAvailableError(alias, cause) =>
        SynchronizerRegistryError.ConnectionErrors.DomainIsNotAvailable.Error(alias, cause)
      case SequencerInfoLoaderError.HandshakeFailedError(cause) =>
        SynchronizerRegistryError.HandshakeErrors.HandshakeFailed.Error(cause)
      case SequencerInfoLoaderError.SequencersFromDifferentSynchronizersAreConfigured(cause) =>
        SynchronizerRegistryError.ConfigurationErrors.SequencersFromDifferentDomainsAreConfigured
          .Error(cause)
      case SequencerInfoLoaderError.MisconfiguredStaticSynchronizerParameters(cause) =>
        SynchronizerRegistryError.ConfigurationErrors.MisconfiguredStaticSynchronizerParameters
          .Error(cause)
      case SequencerInfoLoaderError.FailedToConnectToSequencers(cause) =>
        SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers.Error(cause)
      case SequencerInfoLoaderError.InconsistentConnectivity(cause) =>
        SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers.Error(cause)
    }

  object ConnectionErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that the participant failed to connect to the sequencers."""
    )
    @Resolution("Inspect the provided reason.")
    object FailedToConnectToSequencers
        extends ErrorCode(
          id = "FAILED_TO_CONNECT_TO_SEQUENCERS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The participant failed to connect to the sequencers")
          with SynchronizerRegistryError
    }

    @Explanation(
      "This error results if the GRPC connection to the domain service fails with GRPC status UNAVAILABLE."
    )
    @Resolution(
      "Check your connection settings and ensure that the domain can really be reached."
    )
    object DomainIsNotAvailable
        extends ErrorCode(id = "DOMAIN_IS_NOT_AVAILABLE", ErrorCategory.TransientServerFailure) {
      final case class Error(alias: SynchronizerAlias, reason: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"Cannot connect to domain $alias")
          with SynchronizerRegistryError
    }

    @Explanation(
      """This error indicates that the connecting participant has either not yet been activated by the domain operator.
        If the participant was previously successfully connected to the domain, then this error indicates that the domain
        operator has deactivated the participant."""
    )
    @Resolution(
      "Contact the domain operator and inquire the permissions your participant node has on the given domain."
    )
    object ParticipantIsNotActive
        extends ErrorCode(
          id = "PARTICIPANT_IS_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(serverResponse: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = "The participant is not yet active")
          with SynchronizerRegistryError
    }

    @Explanation(
      """This error indicates that the participant failed to connect to the sequencer."""
    )
    @Resolution("Inspect the provided reason.")
    object FailedToConnectToSequencer
        extends ErrorCode(
          id = "FAILED_TO_CONNECT_TO_SEQUENCER",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The participant failed to connect to the sequencer")
          with SynchronizerRegistryError
    }
    @Explanation(
      """This error indicates that the participant failed to connect due to a general GRPC error."""
    )
    @Resolution("Inspect the provided reason and contact support.")
    object GrpcFailure
        extends ErrorCode(
          id = "GRPC_CONNECTION_FAILURE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(error: GrpcError)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "The domain connection attempt failed with a GRPC error")
          with SynchronizerRegistryError
    }

  }

  object ConfigurationErrors extends ErrorGroup() {
    @Explanation(
      """This error indicates that the participant is configured to connect to multiple
        |domain sequencers from different domains."""
    )
    @Resolution("Carefully verify the connection settings.")
    object SequencersFromDifferentDomainsAreConfigured
        extends ErrorCode(
          id = "SEQUENCERS_FROM_DIFFERENT_DOMAINS_ARE_CONFIGURED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(override val cause: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause)
          with SynchronizerRegistryError {}
    }

    @Explanation(
      """This error indicates that the participant is configured to connect to multiple sequencers of a synchronizer but their
        |static synchronizer parameters are different from other sequencers."""
    )
    object MisconfiguredStaticSynchronizerParameters
        extends ErrorCode(
          id = "MISCONFIGURED_STATIC_SYNCHRONIZER_PARAMETERS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(override val cause: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause)
          with SynchronizerRegistryError {}
    }

    @Explanation(
      """This error indicates that the participant can not issue a synchronizer trust certificate. Such a certificate is
        |necessary to become active on a synchronizer. Therefore, it must be present in the authorized store of the
        |participant topology manager."""
    )
    @Resolution(
      """Manually upload a valid synchronizer trust certificate for the given synchronizer or upload
        |the necessary certificates such that participant can issue such certificates automatically."""
    )
    object CanNotIssueSynchronizerTrustCertificate
        extends ErrorCode(
          id = "CANNOT_ISSUE_SYNCHRONIZER_TRUST_CERTIFICATE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(
            cause = s"Can not auto-issue a domain-trust certificate on this node: $reason"
          )
          with SynchronizerRegistryError {}
    }

    @Explanation(
      "Error indicating that the synchronizer parameters have been changed, while this isn't supported yet."
    )
    object SynchronizerParametersChanged
        extends ErrorCode(
          id = "SYNCHRONIZER_PARAMETERS_CHANGED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(
          oldParameters: Option[StaticSynchronizerParameters],
          newParameters: StaticSynchronizerParameters,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"The synchronizer parameters have changed")
          with SynchronizerRegistryError

      override def logLevel: Level = Level.WARN

    }
  }

  object HandshakeErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that the domain is using crypto settings which are
                                either not supported or not enabled on this participant."""
    )
    @Resolution(
      "Consult the error message and adjust the supported crypto schemes of this participant."
    )
    object DomainCryptoHandshakeFailed
        extends ErrorCode(
          id = "DOMAIN_CRYPTO_HANDSHAKE_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Crypto method handshake with domain failed")
          with SynchronizerRegistryError
    }

    // TODO(i5990) actually figure out what the failure reasons are and distinguish them between internal and normal
    @Explanation(
      """This error indicates that the participant to domain handshake has failed."""
    )
    @Resolution("Inspect the provided reason for more details and contact support.")
    object HandshakeFailed
        extends ErrorCode(
          id = "DOMAIN_HANDSHAKE_FAILED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Handshake with domain has failed")
          with SynchronizerRegistryError
    }

    @Explanation(
      """This error indicates that the synchronizer id does not match the one that the
        participant expects. If this error happens on a first connect, then the synchronizer id
        defined in the domain connection settings does not match the remote domain.
        If this happens on a reconnect, then the remote domain has been reset for some reason."""
    )
    @Resolution("Carefully verify the connection settings.")
    object SynchronizerIdMismatch
        extends ErrorCode(
          id = "SYNCHRONIZER_ID_MISMATCH",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(expected: SynchronizerId, observed: SynchronizerId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              "The domain reports a different synchronizer id than the participant is expecting"
          )
          with SynchronizerRegistryError
    }

    @Explanation("""This error indicates that the synchronizer alias was previously used to
        connect to a domain with a different synchronizer id. This is a known situation when an existing participant
        is trying to connect to a freshly re-initialised domain.""")
    @Resolution("Carefully verify the connection settings.")
    object SynchronizerAliasDuplication
        extends ErrorCode(
          id = "SYNCHRONIZER_ALIAS_DUPLICATION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(
          synchronizerId: SynchronizerId,
          alias: SynchronizerAlias,
          expectedSynchronizerId: SynchronizerId,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              "The domain with the given alias reports a different synchronizer id than the participant is expecting"
          )
          with SynchronizerRegistryError
    }
  }

  @Explanation(
    "This error indicates that there was an error converting topology transactions during connecting to a domain."
  )
  @Resolution("Contact the operator of the topology management for this node.")
  object TopologyConversionError
      extends ErrorCode(
        id = "TOPOLOGY_CONVERSION_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {

    final case class Error(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SynchronizerRegistryError
  }

  @Explanation("This error indicates that there has been an initial onboarding problem.")
  @Resolution(
    "Check the underlying cause and retry if possible. If not, contact support and provide the failure reason."
  )
  object InitialOnboardingError
      extends ErrorCode(
        id = "INITIAL_ONBOARDING_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SynchronizerRegistryError
  }

  @Explanation(
    """This error indicates that there has been an internal error noticed by Canton."""
  )
  @Resolution("Contact support and provide the failure reason.")
  object DomainRegistryInternalError
      extends ErrorCode(
        id = "DOMAIN_REGISTRY_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class TopologyHandshakeError(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Handshake with remote topology transaction registrations service failed",
          throwableO = Some(throwable),
        )
        with SynchronizerRegistryError
    final case class InvalidResponse(
        override val cause: String,
        override val throwableO: Option[Throwable],
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause, throwableO)
        with SynchronizerRegistryError
    final case class DeserializationFailure(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SynchronizerRegistryError
    final case class InvalidState(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SynchronizerRegistryError
  }

}

/** A context handle serving all necessary information / connectivity utilities for the node to setup a connection to a
  * new domain
  */
trait DomainHandle extends AutoCloseable {

  /** Client to the domain's sequencer. */
  def sequencerClient: RichSequencerClient

  /** Client to the sequencer channel client. */
  def sequencerChannelClientO: Option[SequencerChannelClient]

  def staticParameters: StaticSynchronizerParameters

  def synchronizerId: SynchronizerId

  def synchronizerAlias: SynchronizerAlias

  def topologyClient: SynchronizerTopologyClientWithInit

  def domainPersistentState: SyncDomainPersistentState

  def topologyFactory: TopologyComponentFactory

}
