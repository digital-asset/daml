// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.Monoid
import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransactionX.{
  AuthorizedDecentralizedNamespaceDefinitionX,
  AuthorizedIdentifierDelegationX,
  AuthorizedNamespaceDelegationX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.GenericTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.{ExecutionContext, Future}

/** Data collection providing information with respect to what is affected by this update
  *
  * @param authChecks the set of Uids that is mentioned in the transaction such that we can load the certificates for the respective uids
  * @param cascadingNamespaces the set of namespaces where we had a namespace delegation change requiring a cascading update
  * @param cascadingUids the set of uids where we had a identifier delegation change requiring a cascading update
  */
private[processing] final case class UpdateAggregationX(
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

  def add(
      mapping: TopologyMappingX,
      currentTransaction: Option[GenericTopologyTransactionX],
  ): UpdateAggregationX = mapping match {
    case NamespaceDelegationX(ns, _, _) =>
      // change in certificate requires full recompute for namespace (add could unlock existing certificates, remove could make anything obsolete)
      this.copy(cascadingNamespaces = cascadingNamespaces + ns)
    case IdentifierDelegationX(uid, _) =>
      // change in identifier delegation requires full recompute for uid
      this.copy(cascadingUids = cascadingUids + uid, authChecks = authChecks + uid)
    case DecentralizedNamespaceDefinitionX(ns, _, owners) =>
      // change in decentralized namespace definition requires full recompute
      this.copy(cascadingNamespaces = cascadingNamespaces + ns ++ owners)
    case x =>
      this.copy(authChecks =
        authChecks ++ mapping.requiredAuth(currentTransaction).authorizations.uids
      )
  }

  def nothingCascading: Boolean = cascadingNamespaces.isEmpty && cascadingUids.isEmpty

  def authNamespaces: Set[Namespace] = authChecks.map(_.namespace) ++ cascadingNamespaces
}

object UpdateAggregationX {
  implicit val monoid: Monoid[UpdateAggregationX] = new Monoid[UpdateAggregationX] {
    override def empty: UpdateAggregationX = UpdateAggregationX()

    override def combine(x: UpdateAggregationX, y: UpdateAggregationX): UpdateAggregationX =
      UpdateAggregationX(
        authChecks = x.authChecks ++ y.authChecks,
        cascadingNamespaces = x.cascadingNamespaces ++ y.cascadingNamespaces,
        cascadingUids = x.cascadingUids ++ y.cascadingUids,
      )
  }
}

/** validate incoming topology transactions
  *
  * NOT THREAD SAFE. Note that this class is not thread safe
  *
  * we check three things:
  * (1) are the signatures valid
  * (2) are the signatures properly authorized
  *     a. load current set of authorized keys
  *     b. for each transaction, verify that the authorization keys are valid. a key is a valid authorization if there
  *        is a certificate chain that originates from the root certificate at the time when the
  *        transaction is added (one by one).
  *     c. if the transaction is a namespace or identifier delegation, update its impact on the authorization set
  *        this means that if we add or remove a namespace delegation, then we need to perform a cascading
  *        update that activates or deactivates states that depend on this delegation.
  * (3) finally, what we compute as the "authorized graph" is then used to compute the derived table
  *     of "namespace delegations"
  */
class IncomingTopologyTransactionAuthorizationValidatorX(
    val pureCrypto: CryptoPureApi,
    val store: TopologyStoreX[TopologyStoreId],
    domainId: Option[DomainId],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with TransactionAuthorizationValidatorX {

  def reset(): Unit = {
    namespaceCache.clear()
    identifierDelegationCache.clear()
    decentralizedNamespaceCache.clear()
  }

  /** Validates the provided topology transactions and applies the certificates to the auth state
    *
    * When receiving topology transactions we have to evaluate them and continuously apply any
    * update to the namespace delegations or identifier delegations to the "head state".
    *
    * And we use that "head state" to verify if the transactions are authorized or not.
    */
  def validateAndUpdateHeadAuthState(
      timestamp: CantonTimestamp,
      transactionsToValidate: Seq[GenericSignedTopologyTransactionX],
      transactionsInStore: Map[MappingHash, GenericSignedTopologyTransactionX],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[(UpdateAggregationX, Seq[GenericValidatedTopologyTransactionX])] = {
    for {
      authCheckResult <- determineRelevantUidsAndNamespaces(
        transactionsToValidate,
        transactionsInStore.view.mapValues(_.transaction).toMap,
      )
      (updateAggregation, targetDomainVerified) = authCheckResult
      loadGraphsF = loadAuthorizationGraphs(timestamp, updateAggregation.authNamespaces)
      loadUidsF = loadIdentifierDelegationsCascading(
        timestamp,
        updateAggregation,
        updateAggregation.authChecks,
      )
      _ <- loadGraphsF
      cascadingUidsFromNamespace <- loadUidsF
    } yield {

      logger.debug(s"Update aggregation yielded ${updateAggregation}")
      val validated = targetDomainVerified.map {
        case ValidatedTopologyTransactionX(tx, None, _) =>
          processTransaction(
            tx,
            transactionsInStore.get(tx.transaction.mapping.uniqueKey),
            expectFullAuthorization,
          )
        case v => v
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
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
      expectFullAuthorization: Boolean,
  )(implicit traceContext: TraceContext): GenericValidatedTopologyTransactionX = {
    val processedNs = toValidate.selectMapping[NamespaceDelegationX].forall { sigTx =>
      processNamespaceDelegation(
        toValidate.transaction.op,
        AuthorizedTopologyTransactionX(sigTx),
      )
    }

    val processedIdent = toValidate.selectMapping[IdentifierDelegationX].forall { sigTx =>
      processIdentifierDelegation(
        toValidate.transaction.op,
        AuthorizedTopologyTransactionX(sigTx),
      )
    }

    val resultDns = toValidate.selectMapping[DecentralizedNamespaceDefinitionX].map { sigTx =>
      processDecentralizedNamespaceDefinition(
        sigTx.transaction.op,
        AuthorizedTopologyTransactionX(sigTx),
      )
    }
    val processedDns = resultDns.forall(_._1)
    val mappingSpecificCheck = processedNs && processedIdent && processedDns

    val rejectionOrMissingAuthorizers = isCurrentlyAuthorized(toValidate, inStore)

    // The mappingSpecificCheck is a necessary condition for having sufficient authorizers.
    val hasSufficientAuthorizers =
      mappingSpecificCheck && rejectionOrMissingAuthorizers.exists(_.isEmpty)
    // Conversely a failing mappingSpecificCheck implies missing authorizers irrespective
    // of whether the generic auth check finds missing signatures (e.g. in a decentralizedNamespace
    // only the mappingSpecificCheck sees if the USD owner threshold is satisfied).
    val hasMissingAuthorizers =
      rejectionOrMissingAuthorizers.exists(!_.isEmpty || !mappingSpecificCheck)

    // the transaction is fully authorized if either
    // 1. it's a root certificate, or
    // 2. there is no authorization error and there are no missing authorizers
    // We need to check explicitly for the root certificate here, because a REMOVE operation
    // removes itself from the authorization graph, and therefore `isCurrentlyAuthorized` cannot validate it.
    val isFullyAuthorized =
      NamespaceDelegationX.isRootCertificate(toValidate) || hasSufficientAuthorizers

    // If a decentralizedNamespace transaction is fully authorized, reflect so in the decentralizedNamespace cache.
    // Note: It seems a bit unsafe to update the caches on the assumption that the update will also be eventually
    // persisted by the caller (a few levels up the call chain in TopologyStateProcessorX.validateAndApplyAuthorization
    // as the caller performs additional checks such as the numeric value of the serial number).
    // But at least this is safer than where the check was previously (inside processDecentralizedNamespaceDefinition before even
    // `isCurrentlyAuthorized` above had finished all checks).
    if (isFullyAuthorized) {
      resultDns.foreach { case (_, updateDecentralizedNamespaceCache) =>
        updateDecentralizedNamespaceCache()
      }
    }

    val acceptMissingAuthorizers = !expectFullAuthorization && hasMissingAuthorizers

    val finalTransaction = toValidate.copy(isProposal = !isFullyAuthorized)

    // Either the transaction is fully authorized or the request allows partial authorization
    if (isFullyAuthorized || acceptMissingAuthorizers) {
      ValidatedTopologyTransactionX(finalTransaction, None)
    } else {
      ValidatedTopologyTransactionX(
        toValidate,
        rejectionOrMissingAuthorizers match {
          case Left(rejection) => Some(rejection)
          case Right(missingAuthorizers) =>
            if (!missingAuthorizers.isEmpty) {
              logger.debug(s"Missing authorizers for $toValidate: $missingAuthorizers")
            }
            if (!mappingSpecificCheck) {
              logger.debug(s"Mapping specific check failed for $toValidate")
            }
            Some(TopologyTransactionRejection.NotAuthorized)
        },
      )
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
      cascadingUpdate: UpdateAggregationX,
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
      op: TopologyChangeOpX,
      tx: AuthorizedIdentifierDelegationX,
  ): Boolean = {
    // check authorization
    val check = getAuthorizationCheckForNamespace(tx.mapping.identifier.namespace)
    val keysAreValid = check.areValidAuthorizationKeys(tx.signingKeys, requireRoot = false)
    // update identifier delegation cache if necessary
    if (keysAreValid) {
      val updateOp: Set[AuthorizedIdentifierDelegationX] => Set[AuthorizedIdentifierDelegationX] =
        op match {
          case TopologyChangeOpX.Replace =>
            x => x + tx
          case TopologyChangeOpX.Remove =>
            x => // using a filter as the key that authorized the removal might be different that authorized the addition
              x.filter(cur => cur.mapping != tx.mapping)
        }
      updateIdentifierDelegationCache(tx.mapping.identifier, updateOp)
    }
    keysAreValid
  }

  private def processNamespaceDelegation(
      op: TopologyChangeOpX,
      tx: AuthorizedNamespaceDelegationX,
  )(implicit traceContext: TraceContext): Boolean = {
    val graph = getAuthorizationGraphForNamespace(tx.mapping.namespace)
    // add or remove including authorization check
    op match {
      case TopologyChangeOpX.Replace => graph.add(tx)
      case TopologyChangeOpX.Remove => graph.remove(tx)
    }
  }

  /** Process decentralized namespace definition
    *
    * return whether decentralized namespace definition mapping is authorizable along with a "cache-update function" to be invoked
    * by the caller once the mapping is to be committed.
    */
  private def processDecentralizedNamespaceDefinition(
      op: TopologyChangeOpX,
      tx: AuthorizedDecentralizedNamespaceDefinitionX,
  ): (Boolean, () => Unit) = {
    val decentralizedNamespace = tx.mapping.namespace
    val (auth, dnsGraph) = decentralizedNamespaceCache
      .get(decentralizedNamespace)
      .map { case (_, dnsGraph) =>
        val auth = dnsGraph.areValidAuthorizationKeys(tx.signingKeys, false)
        auth -> dnsGraph
      }
      .getOrElse {
        val directDecentralizedNamespaceGraph = namespaceCache.getOrElseUpdate(
          decentralizedNamespace,
          new AuthorizationGraphX(
            decentralizedNamespace,
            extraDebugInfo = false,
            loggerFactory,
          ),
        )
        val ownerGraphs = tx.mapping.owners.forgetNE.toSeq.map(getAuthorizationGraphForNamespace)
        val newDecentralizedNamespaceGraph = DecentralizedNamespaceAuthorizationGraphX(
          tx.mapping,
          directDecentralizedNamespaceGraph,
          ownerGraphs,
        )
        val auth = newDecentralizedNamespaceGraph.areValidAuthorizationKeys(tx.signingKeys, false)
        (auth, newDecentralizedNamespaceGraph)
      }

    (
      auth,
      () => {
        val ownerGraphs = tx.mapping.owners.forgetNE.toSeq.map(getAuthorizationGraphForNamespace)
        decentralizedNamespaceCache
          .put(
            decentralizedNamespace,
            (tx.mapping, dnsGraph.copy(dnd = tx.mapping, ownerGraphs = ownerGraphs)),
          )
          .discard
      },
    )
  }

  private def determineRelevantUidsAndNamespaces(
      transactionsToValidate: Seq[GenericSignedTopologyTransactionX],
      transactionsInStore: Map[MappingHash, GenericTopologyTransactionX],
  ): Future[(UpdateAggregationX, Seq[GenericValidatedTopologyTransactionX])] = {
    def verifyDomain(
        tx: GenericSignedTopologyTransactionX
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
    import UpdateAggregationX.monoid
    transactionsToValidate.parFoldMapA { toValidate =>
      EitherT
        .fromEither[Future](verifyDomain(toValidate))
        .fold(
          rejection =>
            (UpdateAggregationX(), Seq(ValidatedTopologyTransactionX(toValidate, Some(rejection)))),
          _ =>
            (
              UpdateAggregationX().add(
                toValidate.transaction.mapping,
                transactionsInStore.get(toValidate.transaction.mapping.uniqueKey),
              ),
              Seq(ValidatedTopologyTransactionX(toValidate, None)),
            ),
        )
    }
  }
}
