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
import com.digitalasset.canton.topology.transaction.NamespaceDelegation
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.{Namespace, TopologyManagerError, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Component that determines the signing keys both relevant for the transaction and available
  * on the node.
  * The selection rules are as follows:
  * General objectives:
  * <ul>
  *   <li>the selected keys must be in the node's private crypto store</li>
  *   <li>if possible, select a key other than the root certificate key</li>
  * </ul>
  *
  * For <strong>namespaces</strong>: select the key with the longest certificate chain
  * from the root certificate. This way we always favor keys that are not
  * the root certificate key. We define chainLength(ns, k) as number of namespace delegations
  * required to construct a valid certificate chain from the root certificate of namespace ns
  * to the target key k. The same mechanism holds true in case the authorization requires a root delegation,
  * with the additional restriction that only root delegations are taken into account.
  *
  * If there are multiple keys with the same chainLength, sort the keys lexicographically and take the last one.
  * While this decision is arbitrary (because there is no other criteria easily available), it is deterministic.
  *
  * Example:
  *
  * Given
  * <ul>
  *   <li>NSD(ns1, target = k1, signedBy = k1) // root certificate</li>
  *   <li>NSD(ns1, target = k2, signedBy = k1)</li>
  *   <li>NSD(ns1, target = k3, signedBy = k2)</li>
  * </ul>
  * Then
  * <ul>
  *   <li>chainLength(ns1, k1) = 1</li>
  *   <li>chainLength(ns1, k2) = 2</li>
  *   <li>chainLength(ns1, k3) = 3</li>
  * </ul>
  *
  * For <strong>decentralized namespaces</strong>: apply the mechanism used for determining keys for namespaces
  * separately for each of the decentralized namespace owners' namespace.
  *
  * For <strong>UIDs</strong>: select any identifier delegation that is well authorized. Since an identifier delegation
  * is issued for s specific UID, the chainLength property is not so relevant, because the target key of an identifier
  * delegation is supposed to be a different key than the root certificate key anyway.
  *
  * If there are multiple keys with the same chainLength, sort the keys lexicographically and take the last one.
  * While this decision is arbitrary (because there is no other criteria easily available), it is deterministic.
  *
  * If there is no identifier delegation, follow the rules for namespaces for the UID's namespace.
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
      requireRoot: Boolean,
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
            case (delegation, level) if delegation.mapping.isRootDelegation || !requireRoot =>
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

  private def filterKnownKeysForUID(uid: UniqueIdentifier, returnAllValidKeys: Boolean)(implicit
      traceContext: TraceContext
  ) = {
    // namespaceCheck to verify that the IDDs are still fully authorized by their respective signing keys
    val namespaceCheck = tryGetAuthorizationCheckForNamespace(uid.namespace)

    tryGetIdentifierDelegationsForUid(uid).toSeq
      .parTraverseFilter { idd =>
        val isIddAuthorized = namespaceCheck.existsAuthorizedKeyIn(
          idd.signingKeys,
          requireRoot = false,
        )
        if (isIddAuthorized) {
          cryptoPrivateStore
            .existsSigningKey(idd.mapping.target.fingerprint)
            .map(Option.when(_)(idd.mapping.target.fingerprint))
        } else {
          // if the IDD is not authorized, it's not a valid candidate for signing
          EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
            None: Option[Fingerprint]
          )
        }
      }
      .map { usableKeys =>
        if (returnAllValidKeys) usableKeys
        // pick the lexicographically highest key
        else usableKeys.maxOption.toList
      }
  }

  /** @param asOfExclusive the timestamp used to query topology state
    * @param toSign the topology transaction to sign
    * @param inStore the latest fully authorized topology transaction with the same unique key as `toSign`
    * @param returnAllValidKeys if true, returns all keys that can be used to sign.
    *                           if false, only returns the most specific keys per namespace/uid.
    * @return fingerprints of keys the node can use to sign the topology transaction `toSign`
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
          filterKnownKeysForNamespace(namespace, requireRoot = false, returnAllValidKeys)
            .map { keys =>
              if (keys.nonEmpty) logger.debug(s"Keys for $namespace: $keys")
              keys
            }
        )

      knownRootNsKeys = referencedAuth.namespacesWithRoot.toSeq
        .parFlatTraverse(namespace =>
          filterKnownKeysForNamespace(namespace, requireRoot = true, returnAllValidKeys)
            .map { keys =>
              if (keys.nonEmpty) logger.debug(s"Keys for root $namespace: $keys")
              keys
            }
        )

      knownUidKeys = referencedAuth.uids.toSeq.parFlatTraverse { uid =>
        filterKnownKeysForUID(uid, returnAllValidKeys)
          .flatMap { knownIddKeys =>
            if (knownIddKeys.nonEmpty) logger.debug(s"Keys for $uid: $knownIddKeys")

            if (knownIddKeys.isEmpty || returnAllValidKeys) {
              // if we don't know of any keys delegated to by IDDs, or we should return all valid keys,
              // we try to find keys that could sign on behalf of the uid's namespace.
              filterKnownKeysForNamespace(
                uid.namespace,
                requireRoot = false,
                returnAllValidKeys,
              ).map { namespaceKeys =>
                if (namespaceKeys.nonEmpty)
                  logger.debug(s"Keys for $uid's namespace: $namespaceKeys")
                // it's correct to join the two collections of keys here, because either
                // * knownIddKeys is empty, then it doesn't change the result
                // * knownIddKeys is non-empty, but we need to return all keys
                knownIddKeys ++ namespaceKeys
              }
            } else {
              EitherT.pure[FutureUnlessShutdown, CryptoPrivateStoreError](knownIddKeys)
            }
          }
      }
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
          case NamespaceDelegation(ns, target, true) if ns.fingerprint == target.fingerprint =>
            Seq(target.fingerprint)
          case _ => Seq.empty
        }
      )

      allKnownKeysEligibleForSigning <- Seq(
        knownNsKeys,
        knownRootNsKeys,
        knownUidKeys,
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
