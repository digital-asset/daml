// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.NamespaceDelegation
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Component that determines the signing keys both relevant for the transaction and available
  * on the node.
  * The selection rules are as follow:
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
  * If there is no identifier delegation, follow the rules for namespaces for the UID's namespace.
  */
class TopologyManagerSigningKeyDetection(
    @VisibleForTesting override protected[processing] val store: TopologyStore[TopologyStoreId],
    protected val crypto: Crypto,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends TransactionAuthorizationCache
    with NamedLogging {

  private def filterKnownKeysForNamespace(namespace: Namespace, requireRoot: Boolean)(implicit
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
            crypto.cryptoPrivateStore.existsSigningKey(fingerprint)
          }
          .map(
            _.toList
              .maxByOption { case (_, level) => level }
              .map { case (fingerprint, _) =>
                fingerprint
              }
              .toList
          )
      }

  def getValidSigningKeysForTransaction(
      asOf: CantonTimestamp,
      toSign: GenericTopologyTransaction,
      inStore: Option[GenericTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]] =
    for {
      _ <- EitherT.right(populateCaches(asOf, toSign, inStore)).mapK(FutureUnlessShutdown.outcomeK)

      referencedAuth = toSign.mapping.requiredAuth(inStore).referenced

      knownNsKeys = referencedAuth.namespaces.toSeq
        .parFlatTraverse(namespace =>
          filterKnownKeysForNamespace(namespace, requireRoot = false)
            .map { keys =>
              if (keys.nonEmpty) logger.info(s"Keys for $namespace: $keys")
              keys
            }
        )

      knownRootNsKeys = referencedAuth.namespacesWithRoot.toSeq
        .parFlatTraverse(namespace =>
          filterKnownKeysForNamespace(namespace, requireRoot = true)
            .map { keys =>
              if (keys.nonEmpty) logger.info(s"Keys for root $namespace: $keys")
              keys
            }
        )

      knownUidKeys = referencedAuth.uids.toSeq.parFlatTraverse { uid =>
        val namespaceCheck = tryGetAuthorizationCheckForNamespace(uid.namespace)
        tryGetIdentifierDelegationsForUid(uid).toSeq
          .parTraverseFilter { idd =>
            val isIddAuthorized = namespaceCheck.existsAuthorizedKeyIn(
              idd.signingKeys,
              requireRoot = false,
            )
            if (isIddAuthorized) {
              crypto.cryptoPrivateStore
                .existsSigningKey(idd.mapping.target.fingerprint)
                .map(Option.when(_)(idd.mapping.target.fingerprint))
            } else
              EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
                None: Option[Fingerprint]
              )
          }
          // take the first IDD available
          .map(_.headOption)
          .flatMap(knownIddKey =>
            if (knownIddKey.isEmpty) {
              // if we don't know of any keys delegated to by IDDs,
              // we try to find keys that could sign on behalf of the uid's namespace
              filterKnownKeysForNamespace(uid.namespace, requireRoot = false).map { keys =>
                if (keys.nonEmpty) logger.info(s"Keys for $uid's namespace: $keys")
                keys
              }
            } else {
              logger.info(s"Keys for $uid: $knownIddKey")
              EitherT
                .rightT[FutureUnlessShutdown, CryptoPrivateStoreError][Seq[Fingerprint]](
                  knownIddKey.toList
                )
            }
          )
      }
      knownExtraKeys = referencedAuth.extraKeys.toSeq
        .parFilterA { key =>
          crypto.cryptoPrivateStore.existsSigningKey(key)
        }
        .map { keys =>
          if (keys.nonEmpty) logger.info(s"Keys for extra keys: $keys")
          keys
        }

      selfSigned = EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
        toSign.mapping match {
          case NamespaceDelegation(ns, target, true) if (ns.fingerprint == target.fingerprint) =>
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
    } yield allKnownKeysEligibleForSigning

}
