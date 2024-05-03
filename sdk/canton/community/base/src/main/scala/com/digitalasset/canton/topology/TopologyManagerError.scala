// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*

sealed trait TopologyManagerError extends CantonError

object TopologyManagerError extends TopologyManagerErrorGroup {

  @Explanation(
    """This error indicates that there was an internal error within the topology manager."""
  )
  @Resolution("Inspect error message for details.")
  object InternalError
      extends ErrorCode(
        id = "TOPOLOGY_MANAGER_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class ImplementMe(msg: String = "")(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "TODO(#14048) implement me" + (if (msg.nonEmpty) s": $msg" else "")
        )
        with TopologyManagerError

    final case class Other(s: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"TODO(#14048) other failure: ${s}"
        )
        with TopologyManagerError

    final case class CryptoPublicError(error: CryptoPublicStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the public crypto store failed"
        )
        with TopologyManagerError

    final case class CryptoPrivateError(error: CryptoPrivateStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the secret crypto store failed"
        )
        with TopologyManagerError

    final case class IncompatibleOpMapping(op: TopologyChangeOp, mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The operation is incompatible with the mapping"
        )
        with TopologyManagerError

    final case class TopologySigningError(error: SigningError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Creating a signed transaction failed due to a crypto error"
        )
        with TopologyManagerError

    final case class ReplaceExistingFailed(invalid: GenericValidatedTopologyTransaction)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Replacing existing transaction failed upon removal"
        )
        with TopologyManagerError

  }

  @Explanation("""The topology manager has received a malformed message from another node.""")
  @Resolution("Inspect the error message for details.")
  object TopologyManagerAlarm extends AlarmErrorCode(id = "TOPOLOGY_MANAGER_ALARM") {
    final case class Warn(override val cause: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause)
        with TopologyManagerError {
      override def logOnCreation: Boolean = false
    }
  }

  @Explanation("This error indicates that a topology transaction could not be found.")
  @Resolution(
    "The topology transaction either has been rejected, is not valid anymore, is not yet valid, or does not yet exist."
  )
  object TopologyTransactionNotFound
      extends ErrorCode(
        id = "TOPOLOGY_TRANSACTION_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(txHash: TxHash, effective: EffectiveTime)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Topology transaction with hash ${txHash} does not exist or is not active or is not an active proposal at $effective"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the secret key with the respective fingerprint can not be found."""
  )
  @Resolution(
    "Ensure you only use fingerprints of secret keys stored in your secret key store."
  )
  object SecretKeyNotInStore
      extends ErrorCode(
        id = "SECRET_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Secret key with given fingerprint could not be found"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a command contained a fingerprint referring to a public key not being present in the public key store."""
  )
  @Resolution(
    "Upload the public key to the public key store using $node.keys.public.load(.) before retrying."
  )
  object PublicKeyNotInStore
      extends ErrorCode(
        id = "PUBLIC_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Public key with given fingerprint is missing in the public key store"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the uploaded signed transaction contained an invalid signature."""
  )
  @Resolution(
    "Ensure that the transaction is valid and uses a crypto version understood by this participant."
  )
  object InvalidSignatureError extends AlarmErrorCode(id = "INVALID_TOPOLOGY_TX_SIGNATURE_ERROR") {

    final case class Failure(error: SignatureCheckError)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = "Transaction signature verification failed")
        with TopologyManagerError
  }

  object SerialMismatch
      extends ErrorCode(id = "SERIAL_MISMATCH", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    final case class Failure(expected: PositiveInt, actual: PositiveInt)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The given serial $actual did not match the expected serial $expected."
        )
        with TopologyManagerError
  }

  object WrongDomain
      extends ErrorCode(id = "INVALID_DOMAIN", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Failure(wrong: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Wrong domain $wrong"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a transaction has already been added previously."""
  )
  @Resolution(
    """Nothing to do as the transaction is already registered. Note however that a revocation is " +
    final. If you want to re-enable a statement, you need to re-issue an new transaction."""
  )
  object DuplicateTransaction
      extends ErrorCode(
        id = "DUPLICATE_TOPOLOGY_TRANSACTION",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(
        transaction: GenericTopologyTransaction,
        authKey: Fingerprint,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The given topology transaction already exists."
        )
        with TopologyManagerError
    final case class ExistsAt(ts: CantonTimestamp)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The given topology transaction already exists at ${ts}."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction would create a state that already exists and has been authorized with the same key."""
  )
  @Resolution("""Your intended change is already in effect.""")
  object MappingAlreadyExists
      extends ErrorCode(
        id = "TOPOLOGY_MAPPING_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(existing: TopologyMapping, keys: NonEmpty[Set[Fingerprint]])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "A matching topology mapping x authorized with the same keys already exists in this state"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error results if the topology manager did not find a secret key in its store to authorize a certain topology transaction."""
  )
  @Resolution("""Inspect your topology transaction and your secret key store and check that you have the
      appropriate certificates and keys to issue the desired topology transaction. If the list of candidates is empty,
      then you are missing the certificates.""")
  object NoAppropriateSigningKeyInStore
      extends ErrorCode(
        id = "NO_APPROPRIATE_SIGNING_KEY_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(candidates: Seq[Fingerprint])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Could not find an appropriate signing key to issue the topology transaction"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a transaction was rejected, as the signing key is not authorized within the current state."""
  )
  @Resolution(
    """Inspect the topology state and ensure that valid namespace or identifier delegations of the signing key exist or upload them before adding this transaction."""
  )
  object UnauthorizedTransaction extends AlarmErrorCode(id = "UNAUTHORIZED_TOPOLOGY_TRANSACTION") {

    final case class Failure(reason: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = s"Topology transaction is not properly authorized: $reason")
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a removal transaction was rejected, as the mapping affecting the removal did not exist."""
  )
  @Resolution(
    """Inspect the topology state and ensure the mapping of the active transaction you are trying to revoke matches your revocation arguments."""
  )
  object NoCorrespondingActiveTxToRevoke
      extends ErrorCode(
        id = "NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Mapping(mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the mapping of the revocation request"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would remove the last valid key of the given entity, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingLastKeyMustBeForced
      extends ErrorCode(
        id = "REMOVING_LAST_KEY_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Topology transaction would remove the last key of the given entity"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would create dangling topology transactions, making the node unusable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingKeyWithDanglingTransactionsMustBeForced
      extends ErrorCode(
        id = "REMOVING_KEY_DANGLING_TRANSACTIONS_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Topology transaction would remove a key that creates conflicts and dangling transactions"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that it has been attempted to increase the ``ledgerTimeRecordTimeTolerance`` domain parameter in an insecure manner.
      |Increasing this parameter may disable security checks and can therefore be a security risk.
      |"""
  )
  @Resolution(
    """Make sure that the new value of ``ledgerTimeRecordTimeTolerance`` is at most half of the ``mediatorDeduplicationTimeout`` domain parameter.
      |
      |Use ``myDomain.service.set_ledger_time_record_time_tolerance`` for securely increasing ledgerTimeRecordTimeTolerance.
      |
      |Alternatively, add the ``force = true`` flag to your command, if security is not a concern for you.
      |The security checks will be effective again after twice the new value of ``ledgerTimeRecordTimeTolerance``.
      |Using ``force = true`` is safe upon domain bootstrapping.
      |"""
  )
  object IncreaseOfLedgerTimeRecordTimeTolerance
      extends ErrorCode(
        id = "INCREASE_OF_LEDGER_TIME_RECORD_TIME_TOLERANCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class TemporarilyInsecure(
        oldValue: NonNegativeFiniteDuration,
        newValue: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The parameter ledgerTimeRecordTimeTolerance can currently not be increased to $newValue."
        )
        with TopologyManagerError

    final case class PermanentlyInsecure(
        newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
        mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Unable to increase ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance, because it must not be more than half of mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the attempted update of the extra traffic limits for a particular member failed because the new limit is lower than the current limit."
  )
  @Resolution(
    """Extra traffic limits can only be increased. Submit the topology transaction with a higher limit.
      |The metadata details of this error contain the expected minimum value in the field ``expectedMinimum``."""
  )
  object InvalidTrafficLimit
      extends ErrorCode(
        id = "INVALID_TRAFFIC_LIMIT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class TrafficLimitTooLow(
        member: Member,
        actual: PositiveLong,
        expectedMinimum: PositiveLong,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The extra traffic limit for $member should be at least $expectedMinimum, but was $actual."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that a threshold in the submitted transaction was higher than the number of members that would have to satisfy that threshold."
  )
  @Resolution(
    """Submit the topology transaction with a lower threshold.
      |The metadata details of this error contain the expected maximum in the field ``expectedMaximum``."""
  )
  object InvalidThreshold
      extends ErrorCode(id = "INVALID_THRESHOLD", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class ThresholdTooHigh(actual: Int, expectedMaximum: Int)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Threshold must not be higher than $expectedMaximum, but was $actual."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that members referenced in a topology transaction have not declared at least one signing key or at least 1 encryption key or both."
  )
  @Resolution(
    """Ensure that all members referenced in the topology transaction have declared at least one signing key and at least one encryption key, then resubmit the failed transaction.
      |The metadata details of this error contain the members with the missing keys in the field ``members``."""
  )
  object InsufficientKeys
      extends ErrorCode(
        id = "INSUFFICIENT_KEYS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Members ${members.sorted.mkString(", ")} are missing a signing key or an encryption key or both."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the topology transaction references members that are currently unknown."
  )
  @Resolution(
    """Wait for the onboarding of the members to be become active or remove the unknown members from the topology transaction.
      |The metadata details of this error contain the unknown member in the field ``members``."""
  )
  object UnknownMembers
      extends ErrorCode(
        id = "UNKNOWN_MEMBERS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Members ${members.sorted.mkString(", ")} are unknown."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a participant is trying to rescind their domain trust certificate
      |while still being hosting parties."""
  )
  @Resolution(
    """The participant should work with the owners of the parties mentioned in the ``parties`` field in the
      |error details metadata to get itself removed from the list of hosting participants of those parties."""
  )
  object IllegalRemovalOfDomainTrustCertificate
      extends ErrorCode(
        id = "ILLEGAL_REMOVAL_OF_DOMAIN_TRUST_CERTIFICATE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class ParticipantStillHostsParties(
        participantId: ParticipantId,
        parties: Seq[PartyId],
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Cannot remove domain trust certificate for $participantId because it still hosts parties ${parties.sorted
                .mkString(",")}"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a participant was not able to onboard to a domain because onboarding restrictions are in place."""
  )
  @Resolution(
    """Verify the onboarding restrictions of the domain. If the domain is not locked, then the participant needs first to be put on the allow list by issuing a ParticipantDomainPermission transaction."""
  )
  object ParticipantOnboardingRefused
      extends ErrorCode(
        id = "PARTICIPANT_ONBOARDING_REFUSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        participantId: ParticipantId,
        restriction: OnboardingRestriction,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The $participantId can not join the domain because onboarding restrictions are in place"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a participant is trying to rescind their domain trust certificate
      |while still hosting parties."""
  )
  @Resolution(
    """The participant should work with the owners of the parties mentioned in the ``parties`` field in the
      |error details metadata to get itself removed from the list of hosting participants of those parties."""
  )
  object InvalidTopologyMapping
      extends ErrorCode(
        id = "INVALID_TOPOLOGY_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        description: String
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The topology transaction was rejected due to an invalid mapping: $description"
        )
        with TopologyManagerError
  }

  abstract class DomainErrorGroup extends ErrorGroup()
  abstract class ParticipantErrorGroup extends ErrorGroup()

}
