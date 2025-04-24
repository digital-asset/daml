// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import cats.instances.order.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPrivateStoreError}
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyMapping.{Code, ReferencedAuthorizations}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, TopologyMapping}
import com.digitalasset.canton.topology.{Namespace, TopologyManagerError}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Component that determines the signing keys both relevant for the transaction and available on
  * the node.
  *
  * The selection rules are as follows:
  *
  * General objectives:
  *   - the selected keys must be in the node's private crypto store
  *   - if possible, select a key other than the root certificate key
  *
  * For '''namespaces''': select the key with the longest certificate chain from the root
  * certificate. This way we always favor keys that are not the root certificate key. We define
  * chainLength(ns, k) as number of namespace delegations required to construct a valid certificate
  * chain from the root certificate of namespace ns to the target key k.
  *
  * If there are multiple keys with the same chainLength, sort the keys lexicographically and take
  * the last one. While this decision is arbitrary (because there is no other criteria easily
  * available), it is deterministic.
  *
  * Example:
  *
  * Given:
  *   - NSD(ns1, target k1, signedBy = k1) // root certificate
  *   - NSD(ns1, target = k2, signedBy = k1)
  *   - NSD(ns1, target = k3, signedBy = k2)
  *
  * Then:
  *   - chainLength(ns1, k1) = 1
  *   - chainLength(ns1, k2) = 2
  *   - chainLength(ns1, k3) = 3
  *
  * For '''decentralized namespaces''': apply the mechanism used for determining keys for namespaces
  * separately for each of the decentralized namespace owners' namespace.
  *
  * If there are multiple keys with the same chainLength, sort the keys lexicographically and take
  * the last one. While this decision is arbitrary (because there is no other criteria easily
  * available), it is deterministic.
  */
class TopologyManagerSigningKeyDetection[+PureCrypto <: CryptoPureApi](
    @VisibleForTesting override protected[processing] val store: TopologyStore[TopologyStoreId],
    protected val pureCrypto: PureCrypto,
    protected val cryptoPrivateStore: CryptoPrivateStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends TransactionAuthorizationCache[PureCrypto]
    with NamedLogging {

  private def filterKnownKeysForNamespace(
      namespace: Namespace,
      mappingToAuthorize: TopologyMapping.Code,
      returnAllValidKeys: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]] =
    tryGetAuthorizationCheckForNamespace(namespace)
      .authorizedDelegations()
      // if `namespace` happens to be a decentralized namespace, we must check the "highest level"
      // for each namespace separately
      .values
      .toSeq
      .parFlatTraverse { delegations =>
        delegations
          .collect {
            case (delegation, level) if delegation.mapping.canSign(mappingToAuthorize) =>
              (delegation.mapping.target.fingerprint, level)
          }
          .parFilterA { case (fingerprint, _) =>
            cryptoPrivateStore.existsSigningKey(fingerprint)
          }
          .map { usableKeys =>
            val selectedKeys =
              if (returnAllValidKeys) usableKeys
              else
                // find the highest level, and within the level the lexicographically highest fingerprint
                usableKeys.maxByOption { case (fingerprint, level) => (level, fingerprint) }.toList

            selectedKeys.map { case (fingerprint, _) => fingerprint }
          }
      }

  /** @param asOfExclusive
    *   the timestamp used to query topology state
    * @param toSign
    *   the topology transaction to sign
    * @param inStore
    *   the latest fully authorized topology transaction with the same unique key as `toSign`
    * @param returnAllValidKeys
    *   if true, returns all keys that can be used to sign. if false, only returns the most specific
    *   keys per namespace/uid.
    * @return
    *   fingerprints of keys the node can use to sign the topology transaction `toSign`
    */
  def getValidSigningKeysForTransaction(
      asOfExclusive: CantonTimestamp,
      toSign: GenericTopologyTransaction,
      inStore: Option[GenericTopologyTransaction],
      returnAllValidKeys: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TopologyManagerError,
    (ReferencedAuthorizations, Seq[Fingerprint]),
  ] = {
    val result = for {
      _ <- EitherT
        .right(populateCaches(asOfExclusive, toSign, inStore))

      referencedAuth = toSign.mapping.requiredAuth(inStore).referenced

      knownNsKeys = referencedAuth.namespaces.toSeq
        .parFlatTraverse(namespace =>
          filterKnownKeysForNamespace(namespace, toSign.mapping.code, returnAllValidKeys)
            .map { keys =>
              if (keys.nonEmpty) logger.debug(s"Keys for $namespace: $keys")
              keys
            }
        )

      knownExtraKeys = referencedAuth.extraKeys.toSeq
        .parFilterA { key =>
          cryptoPrivateStore.existsSigningKey(key)
        }
        .map { keys =>
          if (keys.nonEmpty) logger.debug(s"Keys for extra keys: $keys")
          keys
        }
      selfSigned = EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
        toSign.mapping match {
          case nsd @ NamespaceDelegation(ns, target, _)
              if ns.fingerprint == target.fingerprint && nsd.canSign(Code.NamespaceDelegation) =>
            Seq(target.fingerprint)
          case _ => Seq.empty
        }
      )

      allKnownKeysEligibleForSigning <- Seq(
        knownNsKeys,
        knownExtraKeys,
        selfSigned,
      ).combineAll
    } yield (referencedAuth, allKnownKeysEligibleForSigning)

    result.leftMap(err =>
      TopologyManagerError.InvalidSignatureError
        .KeyStoreFailure(err): TopologyManagerError
    )
  }

}
