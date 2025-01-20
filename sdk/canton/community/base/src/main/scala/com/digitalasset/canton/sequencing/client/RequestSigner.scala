// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.client.RequestSigner.RequestSignerError
import com.digitalasset.canton.sequencing.client.RequestSigner.RequestSignerError.UnauthenticatedMemberDoNotSign
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SignedContent}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait RequestSigner {
  def signRequest[A <: HasCryptographicEvidence](
      request: A,
      hashPurpose: HashPurpose,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, RequestSignerError, SignedContent[A]]
}

object RequestSigner {

  sealed trait RequestSignerError

  object RequestSignerError {

    final case object UnauthenticatedMemberDoNotSign extends RequestSignerError

    /** Key unavailable can be a result during onboarding when the node has not yet seen its key */
    final case class KeyNotAvailable(at: CantonTimestamp) extends RequestSignerError
    final case class Unexpected(err: SyncCryptoError) extends RequestSignerError

    def toSendAsyncClientError(err: RequestSignerError): SendAsyncClientError = err match {
      case KeyNotAvailable(at) =>
        SendAsyncClientError.SigningKeyNotAvailable(at)
      case Unexpected(error) =>
        SendAsyncClientError.RequestRefused(SendAsyncError.RequestRefused(error.toString))
      case UnauthenticatedMemberDoNotSign =>
        SendAsyncClientError.RequestInvalid("Unauthenticated member should not sign")
    }

  }

  def apply(
      topologyClient: DomainSyncCryptoClient,
      protocolVersion: ProtocolVersion,
  ): RequestSigner = new RequestSigner {
    override def signRequest[A <: HasCryptographicEvidence](
        request: A,
        hashPurpose: HashPurpose,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, RequestSignerError, SignedContent[A]] = {
      val snapshot = topologyClient.currentSnapshotApproximation
      SignedContent
        .create(
          topologyClient.pureCrypto,
          snapshot,
          request,
          Some(snapshot.ipsSnapshot.timestamp),
          hashPurpose,
          protocolVersion,
        )
        .leftMap {
          case SyncCryptoError.KeyNotAvailable(_, _, ts, candidates) if candidates.isEmpty =>
            RequestSignerError.KeyNotAvailable(ts)
          case other => RequestSignerError.Unexpected(other)
        }
    }
  }

  /** Request signer for unauthenticated members: never signs anything */
  object UnauthenticatedRequestSigner extends RequestSigner {
    override def signRequest[A <: HasCryptographicEvidence](request: A, hashPurpose: HashPurpose)(
        implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, RequestSignerError, SignedContent[A]] =
      EitherT.leftT(UnauthenticatedMemberDoNotSign)
  }
}
