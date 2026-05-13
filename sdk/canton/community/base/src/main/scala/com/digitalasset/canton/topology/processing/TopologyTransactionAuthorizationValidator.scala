// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint, Hash, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.{
  AuthorizedDecentralizedNamespaceDefinition,
  AuthorizedNamespaceDelegation,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.Authorization as AuthorizationRejections
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherUtil, ErrorUtil}

import scala.concurrent.ExecutionContext

private[processing] final case class AuthorizationKeys(
    namespaces: Set[Namespace]
) {
  def ++(other: AuthorizationKeys): AuthorizationKeys = AuthorizationKeys(
    namespaces ++ other.namespaces
  )
}

private object AuthorizationKeys {

  val empty: AuthorizationKeys = AuthorizationKeys(Set.empty)
}

/** validate topology transactions
  *
  * NOT THREAD SAFE. Note that this class is not thread safe
  *
  * we check three things:
  *   1. are the signatures valid
  *   1. are the signatures properly authorized
  *      a. load current set of authorized keys
  *      a. for each transaction, verify that the authorization keys are valid. a key is a valid
  *         authorization if there is a certificate chain that originates from the root certificate
  *         at the time when the transaction is added (one by one).
  *      a. if the transaction is a namespace, update its impact on the authorization set. This
  *         means that if we add or remove a namespace delegation, then we need to perform a
  *         cascading update that activates or deactivates states that depend on this delegation.
  *   1. finally, what we compute as the "authorized graph" is then used to compute the derived
  *      table of "namespace delegations"
  *
  * @param validationIsFinal
  *   if false, then a new topology transaction will be marked as a proposal in the case where the
  *   provided signatures are valid, but not sufficient.
  */
class TopologyTransactionAuthorizationValidator[+PureCrypto <: CryptoPureApi](
    val pureCrypto: PureCrypto,
    val store: TopologyStore[TopologyStoreId],
    validationIsFinal: Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends NamedLogging
    with TransactionAuthorizationCache[PureCrypto] {

  /** Validates the provided topology transactions and applies the certificates to the auth state
    *
    * When receiving topology transactions we have to evaluate them and continuously apply any
    * update to the namespace delegations to the "head state".
    *
    * And we use that "head state" to verify if the transactions are authorized or not.
    *
    * @param relaxChecksForBackwardsCompatibility
    *   If set to true, the validation of the transaction relaxes certain checks for backwards
    *   compatibility:
    *   - participant permission upgrades in PartyToParticipant mapping do not require the
    *     authorization of the participant
    *   - it does not consider missing signatures for extra keys (e.g. new signing keys for
    *     OwnerToKeyMapping) to be required for the transaction to become fully authorized. This
    *     flag allows importing legacy topology snapshots that contain topology transactions that
    *     did not require signatures for new signing keys.
    */
  def validateAndUpdateHeadAuthState(
      effectiveTime: CantonTimestamp,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
      // TODO(#29057): check whether the relaxations can be removed
      relaxChecksForBackwardsCompatibility: Boolean,
      storeIsEmpty: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericValidatedTopologyTransaction] =
    verifySynchronizer(toValidate) match {
      case ValidatedTopologyTransaction(tx, None, _) =>
        populateCaches(
          effectiveTime,
          toValidate.transaction,
          inStore.map(_.transaction),
          relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
          storeIsEmpty,
        ).map(_ =>
          processTransaction(
            tx,
            inStore,
            expectFullAuthorization = expectFullAuthorization,
            relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
          )
        )
      case invalid @ ValidatedTopologyTransaction(_, Some(_rejectionReason), _) =>
        FutureUnlessShutdown.pure(invalid)
    }

  /** Validates a topology transaction as follows:
    *   - check that the transaction has valid signatures and is sufficiently authorized. if not,
    *     reject.
    *   - if there are no missing authorizers, as is the case for proposals, we update internal
    *     caches for NSD, IDD, and DND
    *   - if this validation is run to determine a final verdict, as is the case for processing
    *     topology transactions coming from the synchronizer, automatically clear the proposal flag
    *     for transactions with sufficent authorizing signatures.
    */
  private def processTransaction(
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit traceContext: TraceContext): GenericValidatedTopologyTransaction = {
    // See validateRootCertificate why we need to check the removal of a root certificate explicitly here.
    val signatureCheckResult = validateRootCertificate(toValidate)
      .getOrElse(
        validateSignaturesAndDetermineMissingAuthorizers(
          toValidate,
          inStore,
          relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
        )
      )

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

  private def validateSignaturesAndDetermineMissingAuthorizers(
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransaction, ReferencedAuthorizations),
  ] = {
    // check if this is a transaction that just appends a new signature to the existing, already approved one
    val mergingNewSignatureIntoActiveStateTx =
      inStore.exists(tx => !tx.isProposal && tx.transaction == toValidate.transaction)
    // first determine all possible namespaces that need to sign the transaction
    val requiredAuth = requiredAuthFor(
      toValidate.transaction,
      inStore.map(_.transaction),
      relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
    )
    // the following signatures are allowed
    val permissibleSig = {
      val tmp = requiredAuthFor(
        toValidate.transaction,
        None,
        relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
      ).referenced
      TopologyMapping.ReferencedAuthorizations(
        namespaces = tmp.namespaces ++ requiredAuth.referenced.namespaces,
        extraKeys = tmp.extraKeys ++ requiredAuth.referenced.extraKeys,
      )
    }

    val (wronglyCoveredHashes, unvalidatedSigningKeysCoveringHash) =
      toValidate.signatures.toSet.partitionMap(sig =>
        Either.cond(sig.coversHash(toValidate.hash), sig.authorizingLongTermKey, sig.hashesCovered)
      )
    val wronglyCoveredHashesNE = NonEmpty.from(wronglyCoveredHashes.flatten)
    // let's determine which namespaces actually delegated to any of the keys
    val namespaceAuthorizations = permissibleSig.namespaces.map { ns =>
      // This succeeds because loading of uid is requested in AuthorizationKeys.requiredForCheckingAuthorization
      val check = tryGetAuthorizationCheckForNamespace(ns)
      val keysUsedFromAuthorizationGraph: Set[SigningPublicKey] = check.keysSupportingAuthorization(
        unvalidatedSigningKeysCoveringHash,
        toValidate.mapping.code,
      )

      // Mappings may define a key that authorizes the mappings primary namespace (ie. mapping.namespace)
      val selfAuthorizingKeysUsed = toValidate.selectMapping[KeyMapping].flatMap {
        _.transaction.mapping.namespaceKeyForSelfAuthorization.filter(key =>
          // consider the key ONLY if a signature with that key has been provided.
          // the validity check of that signature will be done later.
          unvalidatedSigningKeysCoveringHash.contains(key.fingerprint) &&
            // AND its fingerprint matches the namespace's fingerprint (it authorizes THAT namespace)
            key.fingerprint == ns.fingerprint
        )
      }

      val selfAuthorizingKeyAuthorizesNamespace = selfAuthorizingKeysUsed.nonEmpty
      val allKeysUsed =
        keysUsedFromAuthorizationGraph ++ selfAuthorizingKeysUsed.map(Set(_)).getOrElse(Set.empty)
      val keysFromAuthGraphAuthorizeNamespace = check.existsAuthorizedKeyIn(
        unvalidatedSigningKeysCoveringHash,
        toValidate.mapping.code,
      )
      val keysAuthorizeNamespace =
        keysFromAuthGraphAuthorizeNamespace || selfAuthorizingKeyAuthorizesNamespace
      ns -> (keysAuthorizeNamespace, allKeysUsed)
    }.toMap

    // TODO(i28555): Simplify/refactor this block of code and below
    val extraKeyAuthorizations =
      // assume extra keys are not found
      permissibleSig.extraKeys
        .map(k => k -> (false, Set.empty[SigningPublicKey]))
        .toMap ++
        // and replace with those that were actually found
        // we have to dive into owner to key mappings and paryt to key mappings directly here, because we don't
        // need to keep them around (like we do for namespace delegations) and OTK / PTK are the
        // only places that hold SigningPublicKeys.
        {
          val otk = toValidate
            .select[TopologyChangeOp.Replace, OwnerToKeyMapping]
            .toList
            .flatMap(_.mapping.keys)
          val ptk = toValidate
            .select[TopologyChangeOp.Replace, PartyToKeyMapping]
            .toList
            .flatMap(_.mapping.signingKeys)
          val p2p = toValidate
            .select[TopologyChangeOp.Replace, PartyToParticipant]
            .toList
            .flatMap(_.mapping.partySigningKeys)
          (otk ++ ptk ++ p2p).collect {
            case k: SigningPublicKey
                // only consider the public key as "found" if:
                // * it's required and
                // * actually used to sign the transaction
                if permissibleSig
                  .extraKeys(k.fingerprint) && unvalidatedSigningKeysCoveringHash(k.fingerprint) =>
              k.fingerprint -> (true, Set(k))
          }.toMap
        }

    val namespaceAuthorizationKeys = namespaceAuthorizations.values.flatMap { case (_, keys) =>
      keys.map(k => k.id -> k)
    }.toMap
    val extraKeyAuthorizationKeys = extraKeyAuthorizations.values.flatMap { case (_, keys) =>
      keys.map(k => k.id -> k)
    }.toMap

    val allKeysUsedForAuthorization = namespaceAuthorizationKeys ++ extraKeyAuthorizationKeys
    val superfluousKeys =
      toValidate.signatures.map(_.authorizingLongTermKey) -- allKeysUsedForAuthorization.keys

    if (logger.underlying.isDebugEnabled()) {
      logger.debug(
        s"Authorization details for ${toValidate.mapping.code}=${toValidate.transaction.hash}\n" +
          s"  Required: $requiredAuth\n" +
          s"  Provided for namespaces:" + renderAuthorizations(
            namespaceAuthorizations,
            requiredAuth.referenced.namespaces.map(_.fingerprint),
          ) + "\n" +
          s"  Provided for extraKeys: " + renderAuthorizations(
            extraKeyAuthorizations,
            requiredAuth.referenced.extraKeys,
          ) + "\n" +
          s"  All keys used for authorization: ${allKeysUsedForAuthorization.keySet}" + (
            if (superfluousKeys.nonEmpty) s"\n  Superfluous keys: ${superfluousKeys.mkString(", ")}"
            else ""
          )
      )
    }

    for {
      // consistency check
      _ <- EitherUtil.condUnit(
        inStore.forall(previous =>
          previous.mapping.uniqueKey == toValidate.mapping.uniqueKey && (previous.serial < toValidate.serial || previous.transaction == toValidate.transaction)
        ),
        TopologyTransactionRejection.Processor.InternalError(
          s"In-store and toValidate are inconsistent. In-store:\n  $inStore\ntoValidate $toValidate"
        ),
      )
      // check that all signatures cover the transaction hash
      _ <- wronglyCoveredHashesNE
        .map(AuthorizationRejections.MultiTransactionHashMismatch(toValidate.hash, _))
        .toLeft(())
      _ <- Either.cond[TopologyTransactionRejection, Unit](
        // there must be at least 1 key used for the signatures for one of the delegation mechanisms
        (unvalidatedSigningKeysCoveringHash -- superfluousKeys).nonEmpty,
        (), {
          logger.info(
            s"The keys ${superfluousKeys.mkString(", ")} have no delegation to authorize the transaction $toValidate"
          )
          AuthorizationRejections.NoDelegationFoundForKeys(superfluousKeys)
        },
      )

      txWithSignaturesToVerify <- toValidate
        .removeSignatures(superfluousKeys)
        .toRight({
          logger.info(
            "Removing all superfluous keys results in a transaction without any signatures at all."
          )
          AuthorizationRejections.NoDelegationFoundForKeys(superfluousKeys)
        })

      _ <- Either.cond(
        namespaceAuthorizationKeys.nonEmpty,
        (),
        AuthorizationRejections.NotAuthorizedByNamespaceKey,
      )

      _ <- validateSignatures(
        txWithSignaturesToVerify,
        namespaceAuthorizationKeys = namespaceAuthorizationKeys,
        extraKeyAuthorizationKeys = extraKeyAuthorizationKeys,
      )
    } yield
    // At this point, only new signatures where added to an existing fully authorized transactions. All signatures
    // where also validated.
    // This cannot have invalidated the authorization and therefore we can state that there are no missing authorizations.
    if (mergingNewSignatureIntoActiveStateTx)
      (txWithSignaturesToVerify, ReferencedAuthorizations.empty)
    else {
      // Only returns the keys of the map, for which the first tuple element is true.
      def onlyFullyAuthorized[A](map: Map[A, (Boolean, ?)]): Set[A] = map.collect {
        case (a, (true, _)) => a
      }.toSet

      // Finally we can check whether the authorizations granted by the keys actually satisfy
      // the authorization requirements
      val actual = ReferencedAuthorizations(
        namespaces = onlyFullyAuthorized(namespaceAuthorizations),
        extraKeys = onlyFullyAuthorized(extraKeyAuthorizations),
      )
      (
        txWithSignaturesToVerify,
        requiredAuth
          .satisfiedByActualAuthorizers(actual)
          .fold(
            // If:
            // 1. transaction as a candidate for missing signing key signatures
            // 2. the transaction is an OwnerToKeyMapping
            // Then:
            //   such missing signatures should be ignored and not reported as authorization failure.
            missing => {
              val ret =
                if (
                  relaxChecksForBackwardsCompatibility && toValidate.mapping.code == OwnerToKeyMapping.code
                )
                  missing.copy(extraKeys = Set.empty)
                else missing
              logger.debug(s"Missing authorizations ${ret.namespaces} / ${ret.extraKeys}")
              ret
            },
            _ => ReferencedAuthorizations.empty,
          ),
      )
    }
  }

  /** Validates the signatures of a SignedTopologyTransactions. First validates the signatures for
    * namespace authorizations, and only afterward validates the signatures from extra keys. This
    * mitigates an "unauthenticated" attack, where anybody could submit an OTK for any other member
    * with a lot of signing keys and corresponding signatures, that are expensive to validate.
    */
  private def validateSignatures(
      txWithSignaturesToVerify: SignedTopologyTransaction[TopologyChangeOp, TopologyMapping],
      namespaceAuthorizationKeys: Map[Fingerprint, SigningPublicKey],
      extraKeyAuthorizationKeys: Map[Fingerprint, SigningPublicKey],
  )(implicit traceContext: TraceContext): Either[TopologyTransactionRejection, Unit] =
    NonEmpty // a transaction must have at least one signature, so this should pass
      .from(namespaceAuthorizationKeys.keySet ++ extraKeyAuthorizationKeys.keySet)
      .toRight(
        TopologyTransactionRejection.Authorization.NoSignatureProvided
      )
      .flatMap { signingKeysNE =>
        val validSignaturesToVerify = txWithSignaturesToVerify.signaturesWithHash(pureCrypto)

        val keyIdsWithUsage = TopologyManager.assignExpectedUsageToKeys(
          txWithSignaturesToVerify.mapping,
          signingKeysNE,
          forSigning = false,
        )
        // partition the signatures into two groups:
        // 1. namespace signatures
        // 2. extraKey signatures.
        // This allows us to first validate the signatures namespace authorizations, and
        // only validate extra key signatures, if there is at least 1 valid namespace signature.
        val (namespaceKeysWithSignature, extraKeysWithSignature) =
          validSignaturesToVerify.toSeq.partitionEither { case (hash, signature) =>
            val namespaceKeyWithSignature = namespaceAuthorizationKeys
              .get(signature.authorizingLongTermKey)
              .map(pubKey => Left((hash, signature, pubKey)))

            lazy val extraKeyWithSignature = extraKeyAuthorizationKeys
              .get(signature.authorizingLongTermKey)
              .map(pubKey => Right((hash, signature, pubKey)))

            namespaceKeyWithSignature
              .orElse(extraKeyWithSignature)
              .getOrElse(
                ErrorUtil.invalidState(
                  s"Key ${signature.authorizingLongTermKey} was delegated to, but no actual key was identified. This should not happen."
                )
              )
          }

        // validate the hash of an individual signature
        def validateSignature(
            hash: Hash,
            signature: TopologyTransactionSignature,
            publicKey: SigningPublicKey,
        ): Either[TopologyTransactionRejection, Unit] = for {
          _ <- Either.cond(
            signature.coversHash(txWithSignaturesToVerify.hash),
            (),
            AuthorizationRejections
              .MultiTransactionHashMismatch(
                txWithSignaturesToVerify.hash,
                signature.hashesCovered,
              ),
          )
          _ <- pureCrypto
            .verifySignature(
              hash,
              publicKey,
              signature.signature,
              keyIdsWithUsage.forgetNE(publicKey.id),
            )
            .leftMap(AuthorizationRejections.SignatureCheckFailed.apply)
        } yield ()

        for {
          _ <- namespaceKeysWithSignature.traverse_((validateSignature _).tupled)
          _ <- extraKeysWithSignature.traverse_((validateSignature _).tupled)
        } yield ()

      }

  private def renderAuthorizations[A](
      auths: Map[A, (Boolean, Set[SigningPublicKey])],
      required: Set[Fingerprint],
  ): String = {
    val authorizingKeys = auths
      .collect {
        case (auth, (fullyAuthorized, keys)) if keys.nonEmpty => (auth, fullyAuthorized, keys)
      }
    if (authorizingKeys.nonEmpty) {
      val report = authorizingKeys
        .map { case (auth, fullyAuthorized, keys) =>
          val status = if (fullyAuthorized) "fully" else "partially"
          val (requiredSig, permissibleSig) = keys.partition(c => required.contains(c.fingerprint))
          val fst = s"$auth $status authorized by keys ${requiredSig.map(c => c.id).mkString(", ")}"
          if (permissibleSig.nonEmpty)
            fst + s" (and permissible keys ${permissibleSig.map(c => c.id).mkString(", ")})"
          else
            fst
        }
        .mkString("\n    ")
      "\n    " + report
    } else " No authorizing keys"
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

      toValidate.selectMapping[DecentralizedNamespaceDefinition].foreach { sigTx =>
        processDecentralizedNamespaceDefinition(AuthorizedTopologyTransaction(sigTx))
      }
    }

    val acceptMissingAuthorizers = toValidate.isProposal && !expectFullAuthorization

    // if the result of this validation is final (when processing transactions for the authorized store
    // or sequenced transactions from the synchronizer) we set the proposal flag according to whether the transaction
    // is fully authorized or not.
    // This must not be done when preliminarily validating transactions via the SynchronizerTopologyManager, because
    // the validation outcome might change when validating the transaction again after it has been sequenced.
    val finalTransaction =
      if (validationIsFinal) toValidate.updateIsProposal(isProposal = !missingAuthorizers.isEmpty)
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
        Some(AuthorizationRejections.NotFullyAuthorized(missingAuthorizers)),
      )
    }
  }

  /** Validates the signature of the removal of a root certificate. This check is done separately
    * from the mechanism used for other topology transactions (ie isCurrentlyAuthorized), because
    * removing a root certificate removes it from the authorization graph and therefore
    * isCurrentlyAuthorized would not find the key to validate it.
    */
  private def validateRootCertificate(
      toValidate: GenericSignedTopologyTransaction
  )(implicit traceContext: TraceContext): Option[Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransaction, ReferencedAuthorizations),
  ]] =
    toValidate
      .selectMapping[NamespaceDelegation]
      .filter(NamespaceDelegation.isRootCertificate)
      .map { rootCert =>
        val result = {
          validateSignatures(
            rootCert,
            Map(rootCert.mapping.target.fingerprint -> rootCert.mapping.target),
            Map.empty,
          )

        }.map(_ => (toValidate, ReferencedAuthorizations.empty /* no missing authorizers */ ))
        result
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
    * return whether decentralized namespace definition mapping is authorizable along with a
    * "cache-update function" to be invoked by the caller once the mapping is to be committed.
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

  private def verifySynchronizer(
      toValidate: GenericSignedTopologyTransaction
  ): GenericValidatedTopologyTransaction =
    toValidate.restrictedToSynchronizer.zip(synchronizerId) match {
      case Some((txSynchronizerId, underlyingSynchronizerId))
          if txSynchronizerId != underlyingSynchronizerId.logical =>
        ValidatedTopologyTransaction(
          toValidate,
          Some(AuthorizationRejections.InvalidSynchronizer(txSynchronizerId)),
        )
      case _ => ValidatedTopologyTransaction(toValidate, None)
    }
}
