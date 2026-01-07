// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.ErrorCategory.{
  InvalidGivenCurrentSystemStateOther,
  InvalidGivenCurrentSystemStateResourceExists,
  InvalidIndependentOfSystemState,
}
import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.{OnboardingRestriction, ReassignmentId}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  PositiveTopologyTransaction,
  TxHash,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Util

sealed trait TopologyManagerError extends ContextualizedCantonError

object TopologyManagerError extends TopologyManagerErrorGroup {

  @Explanation(
    "This error indicates that currently, too many topology transactions are pending for this node."
  )
  @Resolution(
    """Change the maximum queue size of the synchronizer outbox or retry."""
  )
  object TooManyPendingTopologyTransactions
      extends ErrorCode(
        "TOPOLOGY_TOO_MANY_PENDING_TOPOLOGY_TRANSACTIONS",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Backpressure()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Too many pending topology transactions on this node."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that there was an internal error within the topology manager."""
  )
  @Resolution("Inspect error message for details.")
  object InternalError
      extends ErrorCode(
        id = "TOPOLOGY_MANAGER_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class Unhandled(description: String, throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Unhandled error: $description",
          throwableO = Some(throwable),
        )
        with TopologyManagerError

    final case class Unexpected(description: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Unhandled error: $description"
        )
        with TopologyManagerError

    final case class TopologySigningError(error: SigningError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Creating a signed transaction failed due to a crypto error"
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
            s"Topology transaction with hash $txHash does not exist or is not active or is not an active proposal at $effective"
        )
        with TopologyManagerError

    final case class EmptyStore()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = "The topology store is empty.")
        with TopologyManagerError
  }

  @Explanation("This error indicates that the expected topology store was not found.")
  @Resolution("Check that the provided topology store name is correct before retrying.")
  object TopologyStoreUnknown
      extends ErrorCode(
        id = "TOPOLOGY_STORE_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(storeId: TopologyStoreId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Topology store '$storeId' is not known."
        )
        with TopologyManagerError

    final case class NotFoundForSynchronizer(synchronizerId: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Topology store for synchronizer $synchronizerId is not known."
        )
        with TopologyManagerError

    final case class NoSynchronizerStoreAvailable()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "No synchronizer store available."
        )
  }

  @Explanation(
    "This error indicates that more than one topology transaction with the same transaction hash was found."
  )
  @Resolution(
    """Inspect the topology state for consistency and take corrective measures before retrying.
      |The metadata details of this error contains the transactions in the field ``transactions``."""
  )
  object TooManyTransactionsWithHash
      extends ErrorCode(
        "TOPOLOGY_TOO_MANY_TRANSACTION_WITH_HASH",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        txHash: TxHash,
        effectiveTime: EffectiveTime,
        transactions: Seq[GenericSignedTopologyTransaction],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Found more than one topology transaction with hash $txHash at $effectiveTime: $transactions"
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
        id = "TOPOLOGY_SECRET_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Secret key with given fingerprint could not be found"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the uploaded signed transaction contained an invalid signature."""
  )
  @Resolution(
    "Ensure that the transaction is valid and uses a crypto version understood by this participant."
  )
  object InvalidSignatureError extends AlarmErrorCode(id = "TOPOLOGY_INVALID_TX_SIGNATURE") {

    final case class Failure(error: SignatureCheckError)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = "Transaction signature verification failed")
        with TopologyManagerError

    final case class KeyStoreFailure(error: CryptoPrivateStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Creating a signed transaction failed due to a key store error: $error"
        )
        with TopologyManagerError
  }

  object SerialMismatch
      extends ErrorCode(
        id = "TOPOLOGY_SERIAL_MISMATCH",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(actual: Option[PositiveInt], expected: Option[PositiveInt])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The provided serial $actual did not match the expected serial $expected."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error is returned when a signature does not cover the expected transaction hash
      |of the transaction."""
  )
  @Resolution(
    """Either add a signature for the hash of this specific transaction only, or a signature
      |that covers the this transaction as part of a multi transaction hash."""
  )
  object MultiTransactionHashMismatch
      extends ErrorCode(
        id = "TOPOLOGY_MULTI_TRANSACTION_HASH_MISMATCH",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(expected: TxHash, actual: NonEmpty[Set[TxHash]])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The given transaction hash set $actual did not contain the expected hash $expected of the transaction."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction was submitted that is restricted to another synchronizer."""
  )
  @Resolution(
    """Recreate the content of the transaction with a correct synchronizer identifier."""
  )
  object InvalidSynchronizer
      extends ErrorCode(
        id = "TOPOLOGY_INVALID_SYNCHRONIZER",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Failure(invalid: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Invalid synchronizer $invalid"
        )
        with TopologyManagerError

    final case class MultipleSynchronizerStoresFound(storeIds: Seq[TopologyStoreId])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Multiple synchronizer stores found for the provided storeId: ${storeIds.mkString(", ")}."
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
            "A matching topology mapping authorized with the same keys already exists in this state"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error results if the topology manager did not find a secret key in its store to authorize a certain topology transaction."""
  )
  @Resolution("""Inspect your topology transaction and your secret key store and check that you have the
      appropriate certificates and keys to issue the desired topology transaction.
      If you explicitly requested signing with specific keys, then the unusable keys are listed. Otherwise,
      if the list is empty, then you are missing the certificates.""")
  object NoAppropriateSigningKeyInStore
      extends ErrorCode(
        id = "TOPOLOGY_NO_APPROPRIATE_SIGNING_KEY_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(required: ReferencedAuthorizations, unusable: Seq[Fingerprint])(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Could not find an appropriate signing key to issue the topology transaction"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a transaction was rejected, as the signing key is not authorized within the current state."""
  )
  @Resolution(
    """Inspect the topology state and ensure that a valid namespace delegations of the signing key exists or upload one before adding this transaction."""
  )
  object UnauthorizedTransaction extends AlarmErrorCode(id = "TOPOLOGY_UNAUTHORIZED_TRANSACTION") {

    final case class NoNamespaceAuth()(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(cause = s"Topology transaction is not properly authorized by any namespace key")
        with TopologyManagerError

    final case class Missing(referenced: ReferencedAuthorizations)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            s"Topology transaction is missing authorizations by namespaces=${referenced.namespaces} and keys=${referenced.extraKeys}"
        )
        with TopologyManagerError

    final case class NoDelegation(keys: Set[Fingerprint])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            s"Topology transaction authorization cannot be verified due to missing namespace delegations for keys ${keys
                .mkString(", ")}"
        )
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
        id = "TOPOLOGY_NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE",
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
    """This error indicates that a dangerous command that could break a node was rejected.
      |This is the case if a dangerous command is run on a node to issue transactions for another node.
      |Such commands must be run with force, as they are very dangerous and could easily break
      |the node.
      |As an example, if we assign an encryption key to a participant that the participant does not
      |have, then the participant will be unable to process an incoming transaction. Therefore we must
      |be very careful to not create such situations.
      | """
  )
  @Resolution("Set the ForceFlag.AlienMember if you really know what you are doing.")
  object DangerousCommandRequiresForce
      extends ErrorCode(
        id = "TOPOLOGY_DANGEROUS_COMMAND_REQUIRES_FORCE_ALIEN_MEMBER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class AlienMember(member: Member, topologyMapping: TopologyMapping.Code)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Issuing $topologyMapping for alien members requires ForceFlag.AlienMember"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that it has been attempted to increase the ``preparationTimeRecordTimeTolerance`` synchronizer parameter in an insecure manner.
      |Increasing this parameter may disable security checks and can therefore be a security risk.
      |"""
  )
  @Resolution(
    """Make sure that the new value of ``preparationTimeRecordTimeTolerance`` is at most half of the ``mediatorDeduplicationTimeout`` synchronizer parameter.
      |
      |Use ``mySynchronizer.service.set_preparation_time_record_time_tolerance`` for securely increasing preparationTimeRecordTimeTolerance.
      |
      |Alternatively, add the flag ``ForceFlag.PreparationTimeRecordTimeToleranceIncrease`` to your command, if security is not a concern for you.
      |The security checks will be effective again after twice the new value of ``preparationTimeRecordTimeTolerance``.
      |Using ``ForceFlag.PreparationTimeRecordTimeToleranceIncrease`` is safe upon synchronizer bootstrapping.
      |"""
  )
  object IncreaseOfPreparationTimeRecordTimeTolerance
      extends ErrorCode(
        id = "TOPOLOGY_INCREASE_OF_PREPARATION_TIME_TOLERANCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class TemporarilyInsecure(
        oldValue: NonNegativeFiniteDuration,
        newValue: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The parameter preparationTimeRecordTimeTolerance can currently not be increased to $newValue."
        )
        with TopologyManagerError

    final case class PermanentlyInsecure(
        newPreparationTimeRecordTimeTolerance: NonNegativeFiniteDuration,
        mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Unable to increase preparationTimeRecordTimeTolerance to $newPreparationTimeRecordTimeTolerance, because it must not be more than half of mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error occurs when the new parameter value is outside the defined lower and upper bounds."""
  )
  @Resolution(
    """Choose a value that is within the allowed lower and upper limits.
      |
      |Alternatively, add the flag ``ForceFlag.AllowOutOfBoundsValue`` to force the value change.
      |Caution: Forcing a value change may result in adverse system behaviour. Proceed only if you understand the risks.
      |"""
  )
  object ValueOutOfBounds
      extends ErrorCode(
        id = "TOPOLOGY_VALUE_OUT_OF_BOUNDS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        value: NonNegativeFiniteDuration,
        name: String,
        min: NonNegativeFiniteDuration,
        max: NonNegativeFiniteDuration,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Parameter `$name` needs to be between $min and $max; found: $value"
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
        id = "TOPOLOGY_INSUFFICIENT_KEYS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Members ${members.sorted.mkString(", ")} are missing a valid owner to key mapping."
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
        id = "TOPOLOGY_UNKNOWN_MEMBERS",
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
    """This error indicates that a participant is trying to rescind actively used topology transactions
      |while still being hosting parties."""
  )
  @Resolution(
    """The participant must remove itself from the party to participant mappings that still refer to it."""
  )
  object IllegalRemovalOfActiveTopologyTransactions
      extends ErrorCode(
        id = "TOPOLOGY_ILLEGAL_REMOVAL_OF_ACTIVE_TRANSACTIONS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    private val maxDisplayed = 10

    final case class ParticipantStillHostsParties(
        participantId: ParticipantId,
        parties: Seq[PartyId],
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Cannot remove synchronizer trust certificate or owner to key mapping for $participantId because it still hosts parties ${parties.sorted
                .take(maxDisplayed)
                .mkString(",")} ${if (parties.sizeIs >= maxDisplayed)
                s" (only showing first $maxDisplayed of ${parties.size})"
              else ""}"
        )
        with TopologyManagerError

  }

  @Explanation(
    """This error indicates that a participant was not able to onboard to a synchronizer because onboarding restrictions are in place."""
  )
  @Resolution(
    """Verify the onboarding restrictions of the synchronizer. If the synchronizer is not locked, then the participant needs first to be put on the allow list by issuing a ParticipantSynchronizerPermission transaction."""
  )
  object ParticipantOnboardingRefused
      extends ErrorCode(
        id = "TOPOLOGY_PARTICIPANT_ONBOARDING_REFUSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        participantId: ParticipantId,
        restriction: OnboardingRestriction,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The $participantId can not join the synchronizer because onboarding restrictions are in place"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates the owner to key mapping is still being used by another transaction."""
  )
  @Resolution(
    """Every synchronizer member needs keys before it can be registered. Consequently, the member needs to be removed first before the keys can be removed."""
  )
  object InvalidOwnerToKeyMappingRemoval
      extends ErrorCode(
        id = "TOPOLOGY_INVALID_REMOVAL_OF_OWNER_TO_KEY_MAPPING",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(
        member: Member,
        inUseBy: PositiveTopologyTransaction,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The owner to key mapping of $member cannot be removed as it is referenced by $inUseBy"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the provided owner to key mapping does not confirm to the requirements of the synchronizer."""
  )
  @Resolution(
    """Each synchronizer defines the valid set of key specs supported. Any member must provide at least one signing key with the valid specs. Participants must provide also an encryption key."""
  )
  object InvalidOwnerToKeyMapping
      extends ErrorCode(
        id = "TOPOLOGY_INVALID_OWNER_TO_KEY_MAPPING",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(
        member: Member,
        keyType: String,
        provided: Seq[PublicKey],
        supported: Seq[String],
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            if (provided.isEmpty)
              s"The owner to key mapping for $member was rejected, as no $keyType key was provided. Supported specs are: $supported"
            else
              s"The owner to key mapping for $member was rejected, as none of the ${provided.size} $keyType keys supports the valid specs ($supported): $provided"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the submitted topology mapping was invalid."""
  )
  @Resolution(
    """The participant should work with the owners of the parties mentioned in the ``parties`` field in the
      |error details metadata to get itself removed from the list of hosting participants of those parties."""
  )
  object InvalidTopologyMapping
      extends ErrorCode(
        id = "TOPOLOGY_INVALID_MAPPING",
        ErrorCategory.InvalidIndependentOfSystemState,
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

  @Explanation("This error indicates that a mapping cannot be removed.")
  @Resolution("Use the REPLACE operation to change the existing mapping.")
  object CannotRemoveMapping
      extends ErrorCode(
        id = "TOPOLOGY_CANNOT_REMOVE_MAPPING",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(mappingCode: TopologyMapping.Code)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Removal of $mappingCode is not supported. Use Replace instead."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the removal of a topology mapping also changed the content compared
      |to the mapping with the previous serial."""
  )
  @Resolution(
    "Submit the transaction with the same topology mapping as the previous serial, but with the operation REMOVE."
  )
  object RemoveMustNotChangeMapping
      extends ErrorCode(
        id = "TOPOLOGY_REMOVE_MUST_NOT_CHANGE_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(actual: TopologyMapping, expected: TopologyMapping)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"""REMOVE must not change the topology mapping:
         |actual: $actual
         |expected: $expected""".stripMargin)
        with TopologyManagerError {}
  }

  @Explanation(
    "This error indicates that the submitted topology snapshot was internally inconsistent."
  )
  @Resolution(
    """Inspect the transactions mentioned in the ``transactions`` field in the error details metadata for inconsistencies."""
  )
  object InconsistentTopologySnapshot
      extends ErrorCode(
        id = "TOPOLOGY_INCONSISTENT_SNAPSHOT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class MultipleEffectiveMappingsPerUniqueKey(
        transactions: Seq[(String, Seq[GenericStoredTopologyTransaction])]
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The topology snapshot was rejected because it was inconsistent."
        )
        with TopologyManagerError
  }

  object MissingTopologyMapping
      extends ErrorCode(
        id = "TOPOLOGY_MISSING_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Reject(
        missingMappings: Map[Member, Seq[TopologyMapping.Code]]
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = {
            val mappingString = missingMappings.toSeq
              .sortBy(_._1)
              .map { case (member, mappings) =>
                s"$member: ${mappings.mkString(", ")}"
              }
              .mkString("; ")
            s"The following members are missing certain topology mappings: $mappingString"
          }
        )
        with TopologyManagerError

    final case class MissingSynchronizerParameters(effectiveTime: EffectiveTime)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Missing synchronizer parameters at $effectiveTime"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction attempts to add mediators to multiple mediator groups."""
  )
  @Resolution(
    "Either first remove the mediators from their current groups or choose other mediators to add."
  )
  object MediatorsAlreadyInOtherGroups
      extends ErrorCode(
        id = "TOPOLOGY_MEDIATORS_ALREADY_IN_OTHER_GROUPS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(
        group: NonNegativeInt,
        mediators: Map[MediatorId, NonNegativeInt],
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Tried to add mediators to group $group, but they are already assigned to other groups: ${mediators.toSeq
                .sortBy(_._1.toProtoPrimitive)
                .mkString(", ")}"
        )
        with TopologyManagerError
  }

  object MemberCannotRejoinSynchronizer
      extends ErrorCode(
        id = "TOPOLOGY_MEMBER_CANNOT_REJOIN_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(members: Seq[Member])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Members ${members.sorted} tried to rejoin a synchronizer which they had previously left."
        )
        with TopologyManagerError
    final case class RejectNewKeys(member: Member)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Member $member tried to re-register its keys which they had previously removed."
        )
        with TopologyManagerError

  }

  @Explanation(
    """This error indicates that the namespace is already used by another entity."""
  )
  @Resolution(
    """Change the namespace used in the submitted topology transaction."""
  )
  object NamespaceAlreadyInUse
      extends ErrorCode(
        id = "TOPOLOGY_NAMESPACE_ALREADY_IN_USE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(
        namespace: Namespace
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The namespace $namespace is already in use by another entity."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the namespace has been revoked."""
  )
  @Resolution(
    """Use a different namespace."""
  )
  object NamespaceHasBeenRevoked
      extends ErrorCode(
        id = "TOPOLOGY_NAMESPACE_HAS_BEEN_REVOKED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(
        namespace: Namespace
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The namespace $namespace has been revoked."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the confirming threshold is higher than the number of hosting nodes, which will result in a non functional party."
  )
  @Resolution("Decrease the threshold or increase the number of hosting nodes.")
  object ConfirmingThresholdCannotBeReached
      extends ErrorCode(
        id = "TOPOLOGY_CONFIRMING_THRESHOLD_CANNOT_BE_REACHED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(threshold: PositiveInt, numberOfHostingNodes: Int)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to set a confirming threshold (${threshold.value}) above the number of hosting nodes ($numberOfHostingNodes)."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the signing threshold is higher than the number of signing keys, which will result in a non functional party."
  )
  @Resolution("Decrease the threshold or increase the number of signing keys.")
  object SigningThresholdCannotBeReached
      extends ErrorCode(
        id = "TOPOLOGY_SIGNING_THRESHOLD_CANNOT_BE_REACHED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(threshold: PositiveInt, numberOfSigningKeys: Int)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to set a signing threshold (${threshold.value}) above the number of signing keys ($numberOfSigningKeys)."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that the partyId to allocate is the same as an already existing admin party."
  )
  @Resolution("Submit the topology transaction with a changed partyId.")
  object PartyIdConflictWithAdminParty
      extends ErrorCode(
        id = "TOPOLOGY_PARTY_ID_CONFLICT_WITH_ADMIN_PARTY",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(partyId: PartyId)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to allocate party $partyId with the same UID as an already existing admin party."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that a participant failed to be onboarded, because it has the same UID as an already existing party."
  )
  @Resolution(
    "Change the identity of the participant by either changing the namespace or the participant's UID and try to onboard to the synchronizer again."
  )
  object ParticipantIdConflictWithPartyId
      extends ErrorCode(
        id = "TOPOLOGY_PARTICIPANT_ID_CONFLICT_WITH_PARTY",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(participantId: ParticipantId, partyId: PartyId)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Tried to onboard participant $participantId while party $partyId with the same UID already exists."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that there already exists a temporary topology store with the desired identifier."
  )
  @Resolution(
    "Either first the existing temporary topology store before resubmitting the request or use the store as it is."
  )
  object TemporaryTopologyStoreAlreadyExists
      extends ErrorCode(
        id = "TOPOLOGY_TEMPORARY_STORE_ALREADY_EXISTS",
        InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Reject(storeId: TopologyStoreId.TemporaryStore)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Cannot create topology store with id $storeId, because it already exists."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that a synchronizer upgrade is ongoing and only mappings related to synchronizer upgrade are permitted."
  )
  @Resolution(
    "Contact the owners of the synchronizer about the ongoing synchronizer upgrade."
  )
  object OngoingSynchronizerUpgrade
      extends ErrorCode(
        id = "TOPOLOGY_ONGOING_SYNCHRONIZER_UPGRADE",
        InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(synchronizerId: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The topology state of synchronizer $synchronizerId is frozen due to an ongoing synchronizer upgrade and no more topology changes are allowed."
        )
        with TopologyManagerError
  }

  @Explanation(
    "This error indicates that a synchronizer upgrade is not ongoing, which prevents some upgrade operations from being performed."
  )
  @Resolution(
    "Contact the owners of the synchronizer about the ongoing synchronizer upgrade."
  )
  object NoOngoingSynchronizerUpgrade
      extends ErrorCode(
        id = "TOPOLOGY_NO_ONGOING_SYNCHRONIZER_UPGRADE",
        InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"The operation cannot be performed because no upgrade is ongoing"
        )
        with TopologyManagerError
  }

  @Explanation("This error indicates that the successor synchronizer id is not valid.")
  @Resolution("""Change the physical synchronizer id of the successor so that it satisfies:
      |- it is greater than the current physical synchronizer id
      |- it is greater than all previous synchronizer announcements
      |""")
  object InvalidSynchronizerSuccessor
      extends ErrorCode(id = "TOPOLOGY_INVALID_SUCCESSOR", InvalidIndependentOfSystemState) {
    final case class Reject(
        successorSynchronizerId: PhysicalSynchronizerId,
        details: String,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"The declared successor $successorSynchronizerId of synchronizer is not valid: $details"
        )
        with TopologyManagerError

    object Reject {
      def conflictWithCurrentPSId(
          currentSynchronizerId: PhysicalSynchronizerId,
          successorSynchronizerId: PhysicalSynchronizerId,
      )(implicit loggingContext: ErrorLoggingContext): Reject =
        Reject(
          successorSynchronizerId,
          s"successor id is not greater than current synchronizer id $currentSynchronizerId",
        )

      def conflictWithPreviousAnnouncement(
          successorSynchronizerId: PhysicalSynchronizerId,
          previouslyAnnouncedSuccessor: PhysicalSynchronizerId,
      )(implicit loggingContext: ErrorLoggingContext): Reject =
        Reject(
          successorSynchronizerId = successorSynchronizerId,
          details =
            s"conflicts with previous announcement with successor $previouslyAnnouncedSuccessor",
        )
    }
  }

  @Explanation(
    "This error indicates that the synchronizer upgrade announcement specified an invalid upgrade time."
  )
  @Resolution(
    "Resubmit the synchronizer announcement with an upgrade time sufficiently in the future."
  )
  object InvalidUpgradeTime
      extends ErrorCode(id = "TOPOLOGY_INVALID_UPGRADE_TIME", InvalidGivenCurrentSystemStateOther) {
    final case class Reject(
        synchronizerId: SynchronizerId,
        effective: EffectiveTime,
        upgradeTime: CantonTimestamp,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"The upgrade time $upgradeTime must be after the effective ${effective.value} of the synchronizer upgrade announcement for synchronizer $synchronizerId."
        )
        with TopologyManagerError

  }
  abstract class SynchronizerErrorGroup extends ErrorGroup()

  abstract class ParticipantErrorGroup extends ErrorGroup()

  object ParticipantTopologyManagerError extends ParticipantErrorGroup {
    @Explanation(
      """This error indicates a vetting request failed due to dependencies not being vetted.
        |On every vetting request, the set supplied packages is analysed for dependencies. The
        |system requires that not only the main packages are vetted explicitly but also all dependencies.
        |This is necessary as not all participants are required to have the same packages installed and therefore
        |not every participant can resolve the dependencies implicitly."""
    )
    @Resolution("Vet the dependencies first and then repeat your attempt.")
    object DependenciesNotVetted
        extends ErrorCode(
          id = "TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(unvetted: Set[Ref.PackageId])(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Package vetting failed due to dependencies not being vetted"
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates that a package vetting command failed due to packages not existing locally.
        |This can be due to either the packages not being present or their dependencies being missing.
        |When vetting a package, the package must exist on the participant, as otherwise the participant
        |will not be able to process a transaction relying on a particular package."""
    )
    @Resolution(
      "Upload the package locally first before issuing such a transaction."
    )
    object CannotVetDueToMissingPackages
        extends ErrorCode(
          id = "TOPOLOGY_CANNOT_VET_DUE_TO_MISSING_PACKAGES",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Missing(packages: Set[Ref.PackageId])(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Package vetting failed due to packages not existing on the local node"
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates that a dangerous PartyToParticipant mapping deletion was rejected.
        |If the command is run and there are active contracts where the party is a stakeholder, these contracts
        |will become will never get pruned, resulting in storage that cannot be reclaimed.
        | """
    )
    @Resolution("Set the ForceFlag.PackageVettingRevocation if you really know what you are doing.")
    object DisablePartyWithActiveContractsRequiresForce
        extends ErrorCode(
          id = "TOPOLOGY_DISABLE_PARTY_WITH_ACTIVE_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(partyId: PartyId, synchronizerId: SynchronizerId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Disable party $partyId failed because there are active contracts on synchronizer $synchronizerId, on which the party is a stakeholder. " +
                s"It may also have other contracts on other synchronizers. " +
                s"Set the ForceFlag.DisablePartyWithActiveContracts if you really know what you are doing."
          )
          with TopologyManagerError
    }

    @Explanation(
      """This error indicates that a dangerous PartyToParticipant mapping was rejected.
        |If the command is run, there will no longer be enough signatory-assigning participants
        |(i.e., reassigning participants with confirmation permissions for assignments) to complete the ongoing reassignments, these reassignments
        |will remain stuck.
        | """
    )
    @Resolution(
      "Set the ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty if you really know what you are doing."
    )
    object InsufficientSignatoryAssigningParticipantsForParty
        extends ErrorCode(
          id = "TOPOLOGY_INSUFFICIENT_SIGNATORY_ASSIGNING_PARTICIPANTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class RejectRemovingParty(
          partyId: PartyId,
          synchronizerId: SynchronizerId,
          reassignmentId: ReassignmentId,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Disable party $partyId failed because there are incomplete reassignments, such as $reassignmentId, on synchronizer $synchronizerId involving the party. " +
                s"Set the ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty if you really know what you are doing."
          )
          with TopologyManagerError

      final case class RejectThresholdIncrease(
          partyId: PartyId,
          synchronizerId: SynchronizerId,
          reassignmentId: ReassignmentId,
          nextThreshold: PositiveInt,
          signatoryAssigningParticipants: Set[ParticipantId],
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Increasing the threshold to $nextThreshold for the party $partyId would result in insufficient signatory-assigning participants for reassignment $reassignmentId " +
                s"on synchronizer $synchronizerId. The signatory assigning participants for this reassignment are: $signatoryAssigningParticipants. " +
                s"Set the ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty if you really know what you are doing."
          )
          with TopologyManagerError

      final case class RejectNotEnoughSignatoryAssigningParticipants(
          partyId: PartyId,
          synchronizerId: SynchronizerId,
          reassignmentId: ReassignmentId,
          threshold: PositiveInt,
          signatoryAssigningParticipants: Set[ParticipantId],
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Changing the party to participant mapping for party $partyId would result in insufficient signatory-assigning participants for reassignment $reassignmentId " +
                s"on synchronizer $synchronizerId. Completing the assignment requires $threshold signatory-assigning participants, but only $signatoryAssigningParticipants would be available. " +
                s"Set the ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty if you are certain about proceeding with this change."
          )
          with TopologyManagerError

    }

    @Explanation(
      """This error indicates that a request to change a participant permission to observer was rejected.
        |If the command is run and the party is still a signatory on active contracts,
        |then this transition prevents it from using the contracts.
        |"""
    )
    @Resolution(
      "Set the ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty if you really know what you are doing."
    )
    object InsufficientParticipantPermissionForSignatoryParty
        extends ErrorCode(
          id = "TOPOLOGY_INSUFFICIENT_PERMISSION_FOR_SIGNATORY_PARTY",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(partyId: PartyId, synchronizerId: SynchronizerId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Changing participant permission of $partyId to observer failed because it is still a signatory on active contracts on synchronizer $synchronizerId. " +
                s"It may also be a signatory on contracts across other synchronizers. " +
                s"Set the ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty if you really know what you are doing."
          )
          with TopologyManagerError
    }

    @Explanation(
      "This error indicates that the package to be vetted is invalid because it doesn't upgrade the vetted packages it claims to upgrade."
    )
    @Resolution(
      "Contact the supplier of the DAR or ensure the vetting state change does not lead to simultaneously-vetted upgrade-incompatible packages."
    )
    object Upgradeability
        extends ErrorCode(
          id = "NOT_VALID_UPGRADE_PACKAGE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
          oldPackage: Util.PkgIdWithNameAndVersion,
          newPackage: Util.PkgIdWithNameAndVersion,
          upgradeError: String,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            s"Upgrade checks indicate that $newPackage cannot be an upgrade of $oldPackage. Reason: $upgradeError"
          )
          with TopologyManagerError
    }

    @Explanation(
      "This error indicates that a package with name `daml-prim` or `daml-std-lib` that isn't a utility package was being vetted. All `daml-prim` and `daml-std-lib` packages should be utility packages."
    )
    @Resolution("Contact the supplier of the Dar.")
    object UpgradeDamlPrimIsNotAUtilityPackage
        extends ErrorCode(
          id = "DAML_PRIM_NOT_UTILITY_PACKAGE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
          packageToBeVetted: Util.PkgIdWithNameAndVersion
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Tried to vet a package $packageToBeVetted, but this package is not a utility package. All packages named `daml-prim` or `daml-std-lib` must be a utility package."
          )
          with TopologyManagerError
    }

    @Explanation(
      "This error indicates that the upgrade checks failed on a package because another package with the same name and version has been previously vetted."
    )
    @Resolution("Inspect the error message and contact support.")
    object UpgradeVersion
        extends ErrorCode(
          id = "KNOWN_PACKAGE_VERSION",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
      final case class Error(
          firstPackage: Util.PkgIdWithNameAndVersion,
          secondPackage: Util.PkgIdWithNameAndVersion,
      )(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Tried to vet two packages with the same name and version: $firstPackage and $secondPackage."
          )
          with TopologyManagerError
    }
  }

  @Explanation(
    "This error indicates that preview features need to be enabled."
  )
  @Resolution("Set flag `enablePreviewFeatures` to true and retry.")
  object PreviewFeature
      extends ErrorCode(
        id = "PREVIEW_FEATURE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        operation: String
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(cause = s"Operation $operation is a preview feature.")
        with TopologyManagerError
  }
}
