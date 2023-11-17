// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose}
import com.digitalasset.canton.sequencing.protocol.SignedContent
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
  ): EitherT[Future, String, SignedContent[A]]
}

object RequestSigner {
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
    ): EitherT[Future, String, SignedContent[A]] = {
      val snapshot = topologyClient.headSnapshot
      SignedContent
        .create(
          topologyClient.pureCrypto,
          snapshot,
          request,
          Some(snapshot.ipsSnapshot.timestamp),
          hashPurpose,
          protocolVersion,
        )
        .leftMap(_.toString)
    }
  }

  /** Request signer for unauthenticated members: never signs anything */
  object UnauthenticatedRequestSigner extends RequestSigner {
    override def signRequest[A <: HasCryptographicEvidence](request: A, hashPurpose: HashPurpose)(
        implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, SignedContent[A]] =
      EitherT.leftT("Unauthenticated members do not sign submission requests")
  }
}
