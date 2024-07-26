// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.{
  AuthorizedDecentralizedNamespaceDefinition,
  AuthorizedIdentifierDelegation,
  AuthorizedNamespaceDelegation,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

private[processing] final case class AuthorizationKeys(
    uids: Set[UniqueIdentifier],
    namespaces: Set[Namespace],
) {
  def ++(other: AuthorizationKeys): AuthorizationKeys = AuthorizationKeys(
    uids ++ other.uids,
    namespaces ++ other.namespaces,
  )
}

private object AuthorizationKeys {

  val empty: AuthorizationKeys = AuthorizationKeys(Set.empty, Set.empty)

  def required(
      toValidate: TopologyTransaction[TopologyChangeOp, TopologyMapping],
      inStore: Option[TopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): AuthorizationKeys = {
    val referencedAuthorizations = toValidate.mapping.requiredAuth(inStore).referenced
    requiredForProcessing(toValidate) ++
      requiredForCheckingAuthorization(referencedAuthorizations)
  }

  private def requiredForProcessing(
      toValidate: TopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): AuthorizationKeys =
    toValidate.mapping match {
      case NamespaceDelegation(namespace, _, _) => AuthorizationKeys(Set.empty, Set(namespace))
      case DecentralizedNamespaceDefinition(_, _, owners) if toValidate.operation == Replace =>
        // In case of Replace, we need to preload the owner graphs so that we can construct the decentralized graph.
        // In case of a Remove, we do not need to preload anything, as we'll simply set the cache value to None.
        AuthorizationKeys(Set.empty, owners.forgetNE)
      case IdentifierDelegation(identifier, _) => AuthorizationKeys(Set(identifier), Set.empty)
      case _: TopologyMapping => AuthorizationKeys.empty
    }

  private def requiredForCheckingAuthorization(
      requiredAuth: ReferencedAuthorizations
  ): AuthorizationKeys = {
    val ReferencedAuthorizations(namespacesWithRoot, namespaces, uids, _extraKeys) = requiredAuth
    AuthorizationKeys(
      uids,
      namespacesWithRoot ++ namespaces ++ uids.map(_.namespace),
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
class IncomingTopologyTransactionAuthorizationValidator(
    val pureCrypto: CryptoPureApi,
    val store: TopologyStore[TopologyStoreId],
    validationIsFinal: Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends NamedLogging
    with TransactionAuthorizationValidator {

  private val domainId =
    TopologyStoreId.select[TopologyStoreId.DomainStore](store).map(_.storeId.domainId)

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
      effectiveTime: CantonTimestamp,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[GenericValidatedTopologyTransaction] =
    verifyDomain(toValidate) match {
      case ValidatedTopologyTransaction(tx, None, _) =>
        val referencedKeys =
          AuthorizationKeys.required(toValidate.transaction, inStore.map(_.transaction))
        val loadGraphsF = loadNamespaceCaches(effectiveTime, referencedKeys.namespaces)
        val loadUidsF = loadIdentifierDelegationCaches(effectiveTime, referencedKeys.uids)
        for {
          _ <- loadGraphsF
          _ <- loadUidsF
        } yield processTransaction(
          tx,
          inStore,
          expectFullAuthorization,
        )

      case invalid @ ValidatedTopologyTransaction(_, Some(_rejectionReason), _) =>
        Future.successful(invalid)
    }

  /** Validates a topology transaction as follows:
    * <ol>
    *   <li>check that the transaction has valid signatures and is sufficiently authorized. if not, reject.</li>
    *   <li>if there are no missing authorizers, as is the case for proposals, we update internal caches for NSD, IDD, and DND</li>
    *   <li>if this validation is run to determine a final verdict, as is the case for processing topology transactions coming from the domain,
    *   automatically clear the proposal flag for transactions with sufficent authorizing signatures.</li>
    * </ol>
    */
  private def processTransaction(
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
  )(implicit traceContext: TraceContext): GenericValidatedTopologyTransaction = {
    // See validateRootCertificate why we need to check the removal of a root certificate explicitly here.
    val signatureCheckResult = validateRootCertificate(toValidate)
      .getOrElse(validateSignaturesAndDetermineMissingAuthorizers(toValidate, inStore))

    signatureCheckResult match {
      // propagate the rejection reason
      case Left(rejectionReason) => ValidatedTopologyTransaction(toValidate, Some(rejectionReason))

      // if a transaction wasn't outright rejected, run some additional checks
      case Right((validatedTx, missingAuthorizers)) =>
        handleSuccessfulSignatureChecks(
          validatedTx,
          missingAuthorizers,
          expectFullAuthorization,
        )
    }
  }

  private def handleSuccessfulSignatureChecks(
      toValidate: GenericSignedTopologyTransaction,
      missingAuthorizers: ReferencedAuthorizations,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): ValidatedTopologyTransaction[TopologyChangeOp, TopologyMapping] = {
    // if there are no missing authorizers, we can update the internal caches
    if (missingAuthorizers.isEmpty) {
      toValidate.selectMapping[NamespaceDelegation].foreach { sigTx =>
        processNamespaceDelegation(AuthorizedTopologyTransaction(sigTx))
      }

      toValidate.selectMapping[IdentifierDelegation].foreach { sigTx =>
        processIdentifierDelegation(AuthorizedTopologyTransaction(sigTx))
      }

      toValidate.selectMapping[DecentralizedNamespaceDefinition].foreach { sigTx =>
        processDecentralizedNamespaceDefinition(AuthorizedTopologyTransaction(sigTx))
      }
    }

    val acceptMissingAuthorizers = toValidate.isProposal && !expectFullAuthorization

    // if the result of this validation is final (when processing transactions for the authorized store
    // or sequenced transactions from the domain) we set the proposal flag according to whether the transaction
    // is fully authorized or not.
    // This must not be done when preliminarily validating transactions via the DomainTopologyManager, because
    // the validation outcome might change when validating the transaction again after it has been sequenced.
    val finalTransaction =
      if (validationIsFinal) toValidate.copy(isProposal = !missingAuthorizers.isEmpty)
      else toValidate

    // Either the transaction is fully authorized or the request allows partial authorization
    if (missingAuthorizers.isEmpty || acceptMissingAuthorizers) {
      ValidatedTopologyTransaction(finalTransaction, None)
    } else {
      if (!missingAuthorizers.isEmpty) {
        logger.debug(s"Missing authorizers: $missingAuthorizers")
      }
      ValidatedTopologyTransaction(
        toValidate,
        Some(TopologyTransactionRejection.NotAuthorized),
      )
    }
  }

  /**  Validates the signature of the removal of a root certificate.
    *  This check is done separately from the mechanism used for other topology transactions (ie isCurrentlyAuthorized),
    *  because removing a root certificate removes it from the authorization graph and therefore
    *  isCurrentlyAuthorized would not find the key to validate it.
    */
  private def validateRootCertificate(
      toValidate: GenericSignedTopologyTransaction
  ): Option[Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransaction, ReferencedAuthorizations),
  ]] = {
    toValidate
      .selectMapping[NamespaceDelegation]
      .filter(NamespaceDelegation.isRootCertificate)
      .map { rootCert =>
        val result = rootCert.signatures.toSeq.forgetNE
          .traverse_(
            pureCrypto
              .verifySignature(
                rootCert.hash.hash,
                rootCert.mapping.target,
                _,
              )
          )
          .bimap(
            TopologyTransactionRejection.SignatureCheckFailed,
            _ => (toValidate, ReferencedAuthorizations.empty /* no missing authorizers */ ),
          )
        result
      }

  }

  private def processIdentifierDelegation(
      tx: AuthorizedIdentifierDelegation
  )(implicit traceContext: TraceContext): Unit = {
    val uid = tx.mapping.identifier
    // This will succeed, because loading of the uid is requested in AuthorizationKeys.requiredForProcessing
    val oldIDDs = tryGetIdentifierDelegationsForUid(uid)
    val withTxRemoved = oldIDDs.filter(_.mapping.uniqueKey != tx.mapping.uniqueKey)
    val newIDDs = tx.operation match {
      case TopologyChangeOp.Replace =>
        // We also need to remove the old mapping so that the new mapping actually *replaces* the old one.
        withTxRemoved + tx
      case TopologyChangeOp.Remove => withTxRemoved
    }
    identifierDelegationCache.put(uid, newIDDs).discard
  }

  private def processNamespaceDelegation(
      tx: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    // This will succeed, because loading of the graph is requested in AuthorizationKeys.requiredForProcessing
    val graph = tryGetAuthorizationGraphForNamespace(tx.mapping.namespace)

    // add or remove including authorization check
    tx.operation match {
      case TopologyChangeOp.Replace => graph.replace(tx)
      case TopologyChangeOp.Remove => graph.remove(tx)
    }
  }

  /** Process decentralized namespace definition
    *
    * return whether decentralized namespace definition mapping is authorizable along with a "cache-update function" to be invoked
    * by the caller once the mapping is to be committed.
    */
  private def processDecentralizedNamespaceDefinition(
      tx: AuthorizedDecentralizedNamespaceDefinition
  )(implicit traceContext: TraceContext): Unit = {
    val decentralizedNamespace = tx.mapping.namespace

    tx.operation match {
      case TopologyChangeOp.Remove =>
        decentralizedNamespaceCache.put(decentralizedNamespace, None).discard

      case TopologyChangeOp.Replace =>
        val ownerGraphs = tx.mapping.owners.forgetNE.toSeq.map(
          // This will succeed, because loading of owner graphs is requested in AuthorizationKeys.requiredForProcessing
          tryGetAuthorizationGraphForNamespace
        )
        val decentralizedGraph = DecentralizedNamespaceAuthorizationGraph(tx.mapping, ownerGraphs)
        decentralizedNamespaceCache.put(decentralizedNamespace, Some(decentralizedGraph)).discard
    }
  }

  private def verifyDomain(
      toValidate: GenericSignedTopologyTransaction
  ): GenericValidatedTopologyTransaction =
    toValidate.restrictedToDomain.zip(domainId) match {
      case Some((txDomainId, underlyingDomainId)) if txDomainId != underlyingDomainId =>
        ValidatedTopologyTransaction(
          toValidate,
          Some(TopologyTransactionRejection.InvalidDomain(txDomainId)),
        )
      case _ => ValidatedTopologyTransaction(toValidate, None)
    }
}
