// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.either.*
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.*
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** Data collection providing information with respect to what is affected by this update
  *
  * @param authChecks the set of Uids that is mentioned in the transaction such that we can load the certificates for the respective uids
  * @param cascadingNamespaces the set of namespaces where we had a namespace delegation change requiring a cascading update
  * @param cascadingUids the set of uids where we had a identifier delegation change requiring a cascading update
  */
private[processing] final case class UpdateAggregation(
    authChecks: Set[UniqueIdentifier] = Set(),
    cascadingNamespaces: Set[Namespace] = Set(),
    cascadingUids: Set[UniqueIdentifier] = Set(),
) {

  /** returns all cascading uids which are not already covered by the cascading namespaces */
  def filteredCascadingUids: Set[UniqueIdentifier] =
    cascadingUids.filterNot(x => cascadingNamespaces.contains(x.namespace))

  /** returns true if the given uid is affected by a cascading update */
  def isCascading(uid: UniqueIdentifier): Boolean =
    cascadingNamespaces.contains(uid.namespace) || cascadingUids.contains(uid)

  def add(mapping: TopologyMapping): UpdateAggregation = mapping match {
    case NamespaceDelegation(ns, _, _) =>
      // change in certificate requires full recompute for namespace (add could unlock existing certificates, remove could make anything obsolete)
      this.copy(cascadingNamespaces = cascadingNamespaces + ns)
    case IdentifierDelegation(uid, _) =>
      // change in identifier delegation requires full recompute for uid
      addAuthCheck(uid).copy(cascadingUids = cascadingUids + uid)
    case x =>
      addAuthCheck(x.requiredAuth.uids: _*)
  }

  private def addAuthCheck(uid: UniqueIdentifier*): UpdateAggregation =
    copy(authChecks = authChecks ++ uid)

  def nothingCascading: Boolean = cascadingNamespaces.isEmpty && cascadingUids.isEmpty

  def authNamespaces: Set[Namespace] = authChecks.map(_.namespace) ++ cascadingNamespaces

}

/** validate incoming topology transactions
  *
  * NOT THREAD SAFE. Note that this class is not thread safe
  *
  * we check three things:
  * (1) are the signatures valid
  * (2) are the signatures properly authorized
  *     a. load current set of authorized keys
  *     b. for each transaction, verify that the authorization key is valid. a key is a valid authorization if there
  *        is a certificate chain that originates from the root certificate at the time when the
  *        transaction is added (one by one).
  *     c. if the transaction is a namespace or identifier delegation, update its impact on the authorization set
  *        this means that if we add or remove a namespace delegation, then we need to perform a cascading
  *        update that activates or deactivates states that depend on this delegation.
  * (3) finally, what we compute as the "authorized graph" is then used to compute the derived table
  *     of "namespace delegations"
  */
class IncomingTopologyTransactionAuthorizationValidator(
    cryptoPureApi: CryptoPureApi,
    val store: TopologyStore[TopologyStoreId],
    domainId: Option[DomainId],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with TransactionAuthorizationValidator {

  def reset(): Unit = {
    namespaceCache.clear()
    identifierDelegationCache.clear()
  }

  /** determine whether one of the txs got already added earlier */
  private def findDuplicates(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Seq[Option[EffectiveTime]]] = {
    Future.sequence(
      transactions.map { tx =>
        // skip duplication check for non-adds
        if (tx.transaction.op != TopologyChangeOp.Add)
          Future.successful(None)
        else {
          // check that the transaction has not been added before (but allow it if it has a different version ...)
          store
            .findStored(tx)
            .map(
              _.filter(x =>
                x.validFrom.value < timestamp && x.transaction.protoVersion == tx.protoVersion && x.transaction == tx
              ).map(_.validFrom)
            )
        }
      }
    )
  }

  /** Validates the provided domain topology transactions and applies the certificates to the auth state
    *
    * When receiving topology transactions we have to evaluate them and continuously apply any
    * update to the namespace delegations or identifier delegations to the "head state".
    *
    * And we use that "head state" to verify if the transactions are authorized or not.
    */
  def validateAndUpdateHeadAuthState(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit
      traceContext: TraceContext
  ): Future[(UpdateAggregation, Seq[ValidatedTopologyTransaction])] = {

    val (updateAggregation, signaturesChecked) = validateSignaturesAndGrabAuthChecks(transactions)
    val validateDuplicatesF = findDuplicates(timestamp, transactions)

    val loadGraphsF = loadAuthorizationGraphs(timestamp, updateAggregation.authNamespaces)
    val loadUidsF =
      loadIdentifierDelegationsCascading(timestamp, updateAggregation, updateAggregation.authChecks)

    logger.debug(s"Update aggregation yielded ${updateAggregation}")

    for {
      _ <- loadGraphsF
      cascadingUidsFromNamespace <- loadUidsF
      validateDuplicates <- validateDuplicatesF
    } yield {
      val validated = signaturesChecked.zip(validateDuplicates).map {
        // two times None means the tx has a valid signature and hasn't been added before
        case (ValidatedTopologyTransaction(elem: SignedTopologyTransaction[_], None), None) =>
          val res = if (processTransaction(elem)) {
            None
          } else {
            Some(TopologyTransactionRejection.NotAuthorized)
          }
          ValidatedTopologyTransaction(elem, res)
        case (
              ValidatedTopologyTransaction(elem: SignedTopologyTransaction[_], None),
              Some(knownBefore),
            ) =>
          ValidatedTopologyTransaction(
            elem,
            Some(TopologyTransactionRejection.Duplicate(knownBefore.value)),
          )
        case (v, _) => v
      }
      // add any uid for which we have a valid identifier delegation to the cascading set (as a new namespace
      // certificate might activate an identifier delegation)
      (
        updateAggregation.copy(cascadingUids =
          updateAggregation.cascadingUids ++ cascadingUidsFromNamespace
        ),
        validated,
      )
    }
  }

  private def processTransaction(
      elem: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): Boolean =
    elem.transaction match {
      case TopologyStateUpdate(op, element) =>
        element.mapping match {
          case nd: NamespaceDelegation =>
            processNamespaceDelegation(
              op,
              AuthorizedTopologyTransaction(elem.uniquePath, nd, elem),
            )
          case id: IdentifierDelegation =>
            processIdentifierDelegation(
              op,
              AuthorizedTopologyTransaction(elem.uniquePath, id, elem),
            )
          case _ => isCurrentlyAuthorized(elem)
        }

      case _: DomainGovernanceTransaction => isCurrentlyAuthorized(elem)
    }

  def getValidSigningKeysForMapping(asOf: CantonTimestamp, mapping: TopologyMapping)(implicit
      traceContext: TraceContext
  ): Future[Seq[Fingerprint]] = {
    @nowarn("msg=match may not be exhaustive")
    def intersect(sets: Seq[Set[Fingerprint]]): Set[Fingerprint] = {
      sets match {
        case Seq() => Set()
        case Seq(one, rest @ _*) =>
          rest.foldLeft(one) { case (acc, elem) => acc.intersect(elem) }
      }
    }
    val loadGF = loadAuthorizationGraphs(asOf, mapping.requiredAuth.namespaces._1.toSet)
    val loadIDF = loadIdentifierDelegations(asOf, Seq(), mapping.requiredAuth.uids.toSet)
    for {
      _ <- loadGF
      _ <- loadIDF
    } yield {
      val (namespaces, requireRoot) = mapping.requiredAuth.namespaces
      val fromNs = namespaces
        .map { namespace =>
          getAuthorizationGraphForNamespace(namespace)
            .authorizedDelegations()
            .filter { auth =>
              auth.mapping.isRootDelegation || !requireRoot
            }
            .map(x => x.mapping.target.fingerprint)
            .toSet
        }
      val fromUids = mapping.requiredAuth.uids.map { uid =>
        identifierDelegationCache.get(uid).toList.flatMap { cache =>
          val graph = getAuthorizationGraphForNamespace(uid.namespace)
          cache
            .filter(aid => graph.isValidAuthorizationKey(aid.signingKey, requireRoot = false))
            .map(_.mapping.target.fingerprint)
        }
      }
      val selfSigned = mapping match {
        case NamespaceDelegation(_, target, true) => Set(target.fingerprint)
        case _ => Set()
      }
      (intersect(fromUids.map(_.toSet)) ++ intersect(fromNs) ++ selfSigned).toSeq
    }
  }

  /** loads all identifier delegations into the identifier delegation cache
    *
    * This function has two "modes". On a cascading update affecting namespaces, we have
    * to reload all identifier delegation certificates in order to figure out the affected
    * uids. The return Set then contains all the uids that were loaded as a result of the
    * namespace query.
    *
    * If there is no cascading namespace update, we just load the affected uids and return an empty set.
    */
  private def loadIdentifierDelegationsCascading(
      timestamp: CantonTimestamp,
      cascadingUpdate: UpdateAggregation,
      transactionUids: Set[UniqueIdentifier],
  )(implicit traceContext: TraceContext): Future[Set[UniqueIdentifier]] = {
    // we need to load the identifier delegations for all the uids that are mentioned by a transactions
    val loadUids =
      (transactionUids ++ cascadingUpdate.cascadingUids) -- identifierDelegationCache.keySet
    if (loadUids.isEmpty && cascadingUpdate.cascadingNamespaces.isEmpty) {
      Future.successful(Set.empty[UniqueIdentifier])
    } else loadIdentifierDelegations(timestamp, cascadingUpdate.cascadingNamespaces.toSeq, loadUids)
  }

  private def processIdentifierDelegation(
      op: AddRemoveChangeOp,
      elem: AuthorizedIdentifierDelegation,
  ): Boolean = {
    // check authorization
    val graph = getAuthorizationGraphForNamespace(elem.mapping.identifier.namespace)
    val auth = graph.isValidAuthorizationKey(elem.signingKey, requireRoot = false)
    // update identifier delegation cache if necessary
    if (auth) {
      val updateOp: Set[AuthorizedIdentifierDelegation] => Set[AuthorizedIdentifierDelegation] =
        op match {
          case TopologyChangeOp.Add =>
            x => x + elem
          case TopologyChangeOp.Remove =>
            x => // using a filter as the key that authorized the removal might be different that authorized the addition
              x.filter(cur => cur.mapping != elem.mapping)
        }
      updateIdentifierDelegationCache(elem.mapping.identifier, updateOp)
    }
    auth
  }

  private def processNamespaceDelegation(
      op: AddRemoveChangeOp,
      elem: AuthorizedNamespaceDelegation,
  )(implicit traceContext: TraceContext): Boolean = {
    val graph = getAuthorizationGraphForNamespace(elem.mapping.namespace)
    // add or remove including authorization check
    op match {
      case TopologyChangeOp.Add => graph.add(elem)
      case TopologyChangeOp.Remove => graph.remove(elem)
    }
  }

  private def validateSignaturesAndGrabAuthChecks(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  ): (UpdateAggregation, Seq[ValidatedTopologyTransaction]) = {

    def verifySignature(
        tx: SignedTopologyTransaction[TopologyChangeOp]
    ): Either[TopologyTransactionRejection, Unit] =
      tx.verifySignature(cryptoPureApi).leftMap(TopologyTransactionRejection.SignatureCheckFailed)

    def verifyDomain(
        tx: SignedTopologyTransaction[TopologyChangeOp]
    ): Either[TopologyTransactionRejection, Unit] =
      tx.restrictedToDomain match {
        case Some(txDomainId) =>
          Either.cond(
            domainId.forall(_ == txDomainId),
            (),
            TopologyTransactionRejection.WrongDomain(txDomainId),
          )
        case None => Right(())
      }

    // we need to figure out for which namespaces and uids we need to load the validation checks
    // and for which uids and namespaces we'll have to perform a cascading update
    transactions.foldLeft((UpdateAggregation(), Seq.empty[ValidatedTopologyTransaction])) {
      case ((cascadingUpdate, acc), x) =>
        val res = (for {
          _ <- verifySignature(x)
          _ <- verifyDomain(x)
        } yield ()) match {
          case Right(()) => None
          case Left(err) =>
            Some(err)
        }
        val cc = res.fold(cascadingUpdate.add(x.transaction.element.mapping))(_ => cascadingUpdate)
        (cc, acc :+ ValidatedTopologyTransaction(x, res))
    }
  }

  def authorizedNamespaceDelegationsForNamespaces(
      namespaces: Set[Namespace]
  ): Seq[AuthorizedNamespaceDelegation] =
    for {
      ns <- namespaces.toList
      gr <- namespaceCache.get(ns).toList
      item <- gr.authorizedDelegations()
    } yield item

  def authorizedIdentifierDelegationsForUid(
      uid: UniqueIdentifier
  ): Seq[AuthorizedIdentifierDelegation] = {
    val ret = for {
      graph <- namespaceCache.get(uid.namespace)
      items <- identifierDelegationCache.get(uid)
    } yield items
      .filter(x => graph.isValidAuthorizationKey(x.signingKey, requireRoot = false))
      .toSeq
    ret.getOrElse(Seq())
  }

}
