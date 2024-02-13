// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import com.daml.error.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.SequencerInfoLoaderError
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.sync.SyncServiceError.DomainRegistryErrorGroup
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

/** A registry of domains. */
trait DomainRegistry extends AutoCloseable {

  /**  Returns a domain handle that is used to setup a connection to a new domain
    */
  def connect(
      config: DomainConnectionConfig
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, DomainHandle]]

}

sealed trait DomainRegistryError extends Product with Serializable with CantonError

object DomainRegistryError extends DomainRegistryErrorGroup {

  def fromSequencerInfoLoaderError(
      error: SequencerInfoLoaderError
  )(implicit loggingContext: ErrorLoggingContext): DomainRegistryError = {
    error match {
      case SequencerInfoLoaderError.DeserializationFailure(cause) =>
        DomainRegistryError.DomainRegistryInternalError.DeserializationFailure(cause)
      case SequencerInfoLoaderError.InvalidResponse(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidResponse(cause, None)
      case SequencerInfoLoaderError.InvalidState(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidState(cause)
      case SequencerInfoLoaderError.DomainIsNotAvailableError(alias, cause) =>
        DomainRegistryError.ConnectionErrors.DomainIsNotAvailable.Error(alias, cause)
      case SequencerInfoLoaderError.HandshakeFailedError(cause) =>
        DomainRegistryError.HandshakeErrors.HandshakeFailed.Error(cause)
      case SequencerInfoLoaderError.SequencersFromDifferentDomainsAreConfigured(cause) =>
        DomainRegistryError.ConfigurationErrors.SequencersFromDifferentDomainsAreConfigured
          .Error(cause)
      case SequencerInfoLoaderError.MisconfiguredStaticDomainParameters(cause) =>
        DomainRegistryError.ConfigurationErrors.MisconfiguredStaticDomainParameters
          .Error(cause)
      case SequencerInfoLoaderError.FailedToConnectToSequencers(cause) =>
        DomainRegistryError.ConnectionErrors.FailedToConnectToSequencers.Error(cause)
    }
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
          with DomainRegistryError
    }

    @Explanation(
      "This error results if the GRPC connection to the domain service fails with GRPC status UNAVAILABLE."
    )
    @Resolution(
      "Check your connection settings and ensure that the domain can really be reached."
    )
    object DomainIsNotAvailable
        extends ErrorCode(id = "DOMAIN_IS_NOT_AVAILABLE", ErrorCategory.TransientServerFailure) {
      final case class Error(alias: DomainAlias, reason: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"Cannot connect to domain ${alias}")
          with DomainRegistryError
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
          with DomainRegistryError
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
          with DomainRegistryError
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
          with DomainRegistryError
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
          with DomainRegistryError {}
    }

    @Explanation(
      """This error indicates that the participant is configured to connect to multiple domain sequencers but their
        |static domain parameters are different from other sequencers."""
    )
    object MisconfiguredStaticDomainParameters
        extends ErrorCode(
          id = "MISCONFIGURED_STATIC_DOMAIN_PARAMETERS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(override val cause: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause)
          with DomainRegistryError {}
    }

    @Explanation(
      """This error indicates that the participant can not issue a domain trust certificate. Such a certificate is
        |necessary to become active on a domain. Therefore, it must be present in the authorized store of the
        |participant topology manager."""
    )
    @Resolution(
      """Manually upload a valid domain trust certificate for the given domain or upload
        |the necessary certificates such that participant can issue such certificates automatically."""
    )
    object CanNotIssueDomainTrustCertificate
        extends ErrorCode(
          id = "CANNOT_ISSUE_DOMAIN_TRUST_CERTIFICATE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(
            cause = s"Can not auto-issue a domain-trust certificate on this node: ${reason}"
          )
          with DomainRegistryError {}
    }
    @Explanation(
      """This error indicates there is a validation error with the configured connections for the domain"""
    )
    object InvalidDomainConnections
        extends ErrorCode(
          id = "INVALID_DOMAIN_CONNECTION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(message: String)(implicit val loggingContext: ErrorLoggingContext)
          extends CantonError.Impl(cause = "Configured domain connection is invalid")
          with DomainRegistryError
    }

    @Explanation(
      "Error indicating that the domain parameters have been changed, while this isn't supported yet."
    )
    object DomainParametersChanged
        extends ErrorCode(
          id = "DOMAIN_PARAMETERS_CHANGED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(
          oldParameters: Option[StaticDomainParameters],
          newParameters: StaticDomainParameters,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(cause = s"The domain parameters have changed")
          with DomainRegistryError

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
          with DomainRegistryError
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
          with DomainRegistryError
    }

    @Explanation(
      """This error indicates that the domain-id does not match the one that the
        participant expects. If this error happens on a first connect, then the domain id
        defined in the domain connection settings does not match the remote domain.
        If this happens on a reconnect, then the remote domain has been reset for some reason."""
    )
    @Resolution("Carefully verify the connection settings.")
    object DomainIdMismatch
        extends ErrorCode(
          id = "DOMAIN_ID_MISMATCH",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(expected: DomainId, observed: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "The domain reports a different domain-id than the participant is expecting"
          )
          with DomainRegistryError
    }

    @Explanation("""This error indicates that the domain alias was previously used to
        connect to a domain with a different domain id. This is a known situation when an existing participant
        is trying to connect to a freshly re-initialised domain.""")
    @Resolution("Carefully verify the connection settings.")
    object DomainAliasDuplication
        extends ErrorCode(
          id = "DOMAIN_ALIAS_DUPLICATION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(domainId: DomainId, alias: DomainAlias, expectedDomainId: DomainId)(
          implicit val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              "The domain with the given alias reports a different domain id than the participant is expecting"
          )
          with DomainRegistryError
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
        with DomainRegistryError
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
    final case class InitialOnboardingError(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError

    final case class TopologyHandshakeError(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Handshake with remote topology transaction registrations service failed",
          throwableO = Some(throwable),
        )
        with DomainRegistryError
    final case class InvalidResponse(
        override val cause: String,
        override val throwableO: Option[Throwable],
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause, throwableO)
        with DomainRegistryError
    final case class DeserializationFailure(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError
    final case class InvalidState(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with DomainRegistryError

  }

}

/** A context handle serving all necessary information / connectivity utilities for the node to setup a connection to a
  * new domain
  */
trait DomainHandle extends AutoCloseable {

  /** Client to the domain's sequencer. */
  def sequencerClient: RichSequencerClient

  def staticParameters: StaticDomainParameters

  def domainId: DomainId

  def domainAlias: DomainAlias

  def topologyClient: DomainTopologyClientWithInit

  def domainPersistentState: SyncDomainPersistentState

  def topologyFactory: TopologyComponentFactory

}
