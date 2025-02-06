// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi, SynchronizerCryptoClient}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait RequestSigner {
  def signRequest[A <: HasCryptographicEvidence](
      request: A,
      hashPurpose: HashPurpose,
      snapshotO: Option[SyncCryptoApi] = None,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, SignedContent[A]]
}

object RequestSigner {
  def apply(
      topologyClient: SynchronizerCryptoClient,
      protocolVersion: ProtocolVersion,
      loggerFactoryP: NamedLoggerFactory,
  ): RequestSigner = new RequestSigner with NamedLogging {
    override val loggerFactory: NamedLoggerFactory = loggerFactoryP
    override def signRequest[A <: HasCryptographicEvidence](
        request: A,
        hashPurpose: HashPurpose,
        snapshotO: Option[SyncCryptoApi],
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] = {
      val snapshot = snapshotO.getOrElse(topologyClient.headSnapshot)
      logger.trace(s"Signing request with snapshot at ${snapshot.ipsSnapshot.timestamp}")
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
}
