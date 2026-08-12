// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Defines the default methods for protocol signing that use a topology snapshot for key lookup.
  * This approach uses the signing APIs registered in Canton at node startup.
  */
class SyncCryptoSignerWithLongTermKeys(
    member: Member,
    signPrivateApiWithLongTermKeys: SigningPrivateOps,
    override protected val cryptoPrivateStore: CryptoPrivateStore,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncCryptoSigner {

  /** Sign given hash with signing key for (member, domain, timestamp)
    */
  override def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      signingPublicKey <- findSigningKey(member, topologySnapshot, usage)
      signature <- signPrivateApiWithLongTermKeys
        .sign(hash, signingPublicKey.id, usage)
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
    } yield signature

  override def close(): Unit = ()

}
