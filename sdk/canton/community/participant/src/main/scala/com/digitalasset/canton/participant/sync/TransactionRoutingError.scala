// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.*
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.RoutingErrorGroup
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionSubmissionError
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId

sealed trait TransactionRoutingError extends TransactionError with Product with Serializable
sealed trait TransactionRoutingErrorWithSynchronizer extends TransactionRoutingError {
  def synchronizerId: SynchronizerId
}

/** All routing errors happen before in-flight submission checking and are therefore never definite answers. */
object TransactionRoutingError extends RoutingErrorGroup {

  final case class SubmissionError(
      synchronizerId: SynchronizerId,
      parent: TransactionSubmissionError,
  ) extends TransactionParentError[TransactionSubmissionError]
      with TransactionRoutingError {

    override def mixinContext: Map[String, String] = Map(
      "synchronizerId" -> synchronizerId.toString
    )

  }

  object ConfigurationErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that a transaction has been submitted that requires multi-synchronizer support.
        Multi-synchronizers support is a preview feature that needs to be enabled explicitly by configuration."""
    )
    @Resolution("Set canton.features.enable-preview-commands = yes")
    object MultiSynchronizerSupportNotEnabled
        extends ErrorCode(
          id = "MULTI_SYNCHRONIZER_SUPPORT_NOT_ENABLED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(synchronizers: Set[SynchronizerId])
          extends TransactionErrorImpl(
            cause =
              s"""This transaction requires multi-synchronizer support which is turned off on this participant.
                | Used contracts reside on synchronizers $synchronizers. """.stripMargin
          )
          with TransactionRoutingError

    }

    @Explanation(
      """This error indicates that the transaction should be submitted to a synchronizer which is not connected or not configured."""
    )
    @Resolution(
      """Ensure that the synchronizer specified in the workflowId is correctly connected."""
    )
    object SubmissionSynchronizerNotReady
        extends ErrorCode(
          id = "SUBMISSION_SYNCHRONIZER_NOT_READY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Error(synchronizerId: SynchronizerId)
          extends TransactionErrorImpl(
            cause = "Trying to submit to a disconnected or not configured synchronizer."
          )
          with TransactionRoutingError
          with TransactionSubmissionError
    }

    object InvalidPrescribedSynchronizerId
        extends ErrorCode(
          id = "INVALID_PRESCRIBED_SYNCHRONIZER_ID",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class InputContractsNotOnSynchronizer(
          synchronizerId: SynchronizerId,
          inputContractSynchronizerId: SynchronizerId,
      ) extends TransactionErrorImpl(
            cause =
              s"The needed input contracts are not on $synchronizerId, but on $inputContractSynchronizerId"
          )
          with TransactionRoutingErrorWithSynchronizer

      final case class NotAllInformeeAreOnSynchronizer(
          synchronizerId: SynchronizerId,
          synchronizersOfAllInformee: NonEmpty[Set[SynchronizerId]],
      ) extends TransactionErrorImpl(
            cause =
              s"Not all informee are on the specified synchronizer: $synchronizerId, but on $synchronizersOfAllInformee"
          )
          with TransactionRoutingErrorWithSynchronizer

      final case class Generic(synchronizerId: SynchronizerId, reason: String)
          extends TransactionErrorImpl(
            cause =
              s"Cannot submit transaction to prescribed synchronizer `$synchronizerId` because: $reason"
          )
          with TransactionRoutingErrorWithSynchronizer
    }
  }

  object MalformedInputErrors extends ErrorGroup() {

    @Explanation(
      """The party defined as a reader (actAs or readAs) can not be parsed into a valid Canton party."""
    )
    @Resolution(
      """Check that you only use correctly setup party names in your application."""
    )
    object InvalidReader
        extends ErrorCode(
          id = "INVALID_READER",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Error(submitter: String)
          extends TransactionErrorImpl(
            cause = "Unable to parse reader (actAs or readAs)."
          )
          with TransactionRoutingError
    }

    @Explanation(
      """The given party identifier is not a valid Canton party identifier."""
    )
    @Resolution(
      """Ensure that your commands only refer to correct and valid Canton party identifiers of parties
        |that are properly enabled on the system"""
    )
    object InvalidPartyIdentifier
        extends ErrorCode(
          id = "INVALID_PARTY_IDENTIFIER",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Error(err: String)
          extends TransactionErrorImpl(
            cause = s"The given party is not a valid Canton party identifier: $err"
          )
          with TransactionRoutingError
    }

    @Explanation(
      """A provided disclosed contract could not be processed."""
    )
    @Resolution(
      """Ensure that disclosed contracts provided with command submission have an authenticated contract id
        |(i.e. have been created in participant nodes running Canton protocol version 4 or higher) and
        |match the original contract creation format and content as sourced from the Ledger API.
        |If the problem persists, contact the participant operator."""
    )
    object InvalidDisclosedContract
        extends ErrorCode(
          id = "INVALID_DISCLOSED_CONTRACT",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Error(err: String)
          extends TransactionErrorImpl(
            cause = s"Invalid disclosed contract: $err"
          )
          with TransactionRoutingError
    }

    @Explanation(
      """A provided disclosed contract could not be authenticated against the provided contract id."""
    )
    @Resolution(
      """Ensure that disclosed contracts provided with command submission match the original contract creation
        |content as sourced from the Ledger API.
        |If the problem persists, contact the participant operator."""
    )
    object DisclosedContractAuthenticationFailed
        extends ErrorCode(
          id = "DISCLOSED_CONTRACT_AUTHENTICATION_FAILED",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Error(err: String)
          extends TransactionErrorImpl(
            cause = s"Disclosed contract authentication failed: $err"
          )
          with TransactionRoutingError
    }
  }

  object TopologyErrors extends ErrorGroup() {

    @Explanation(
      """This error indicates that a transaction has been sent where the system can not find any active " +
          "synchronizer on which this participant can submit in the name of the given set of submitters."""
    )
    @Resolution(
      """Ensure that you are connected to a synchronizer where this participant has submission rights.
        Check that you are actually connected to the synchronizers you expect to be connected and check that
        your participant node has the submission permission for each submitting party."""
    )
    object NoSynchronizerOnWhichAllSubmittersCanSubmit
        extends ErrorCode(
          id = "NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      final case class NotAllowed(unknownSubmitter: LfPartyId)
          extends TransactionErrorImpl(
            cause =
              "This participant cannot submit as the given submitter on any connected synchronizer"
          )
          with TransactionRoutingError

      final case class NoSuitableSynchronizer(unknownSubmitters: Seq[LfPartyId])
          extends TransactionErrorImpl(
            cause =
              "Not connected to a synchronizer on which this participant can submit for all submitters"
          )
          with TransactionRoutingError

    }

    @Explanation(
      """This error indicates that the transaction is referring to some submitters that are not known on any connected synchronizer."""
    )
    @Resolution(
      """Check the list of provided submitters and check if your participant is connected to the synchronizers you are expecting it to be."""
    )
    object UnknownSubmitters
        extends ErrorCode(
          id = "UNKNOWN_SUBMITTERS",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      final case class Error(unknownSubmitters: Set[LfPartyId])
          extends TransactionErrorImpl(
            cause =
              "The participant is not connected to any synchronizer where the given submitters are known."
          )
          with TransactionRoutingError

    }

    @Explanation(
      """This error indicates that the transaction is referring to some informees that are not known on any connected synchronizer."""
    )
    @Resolution(
      """Check the list of submitted informees and check if your participant is connected to the synchronizers you are expecting it to be."""
    )
    object UnknownInformees
        extends ErrorCode(
          id = "UNKNOWN_INFORMEES",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      final case class Error(unknownInformees: Set[LfPartyId])
          extends TransactionErrorImpl(
            cause =
              "The participant is not connected to any synchronizer where the given informees are known."
          )
          with TransactionRoutingError

    }

    @Explanation(
      """This error indicates that the submitters are known, but there is no connected synchronizer on which all the submitters are hosted."""
    )
    @Resolution(
      "Ensure that there is such a synchronizer, as Canton requires a synchronizer where all submitters are present."
    )
    object SubmittersNotActive
        extends ErrorCode(
          id = "SUBMITTERS_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(synchronizers: Set[SynchronizerId], informees: Set[LfPartyId])
          extends TransactionErrorImpl(
            cause = "There is no common synchronizer where all submitters are active"
          )
          with TransactionRoutingError
    }
    @Explanation(
      """This error indicates that the informees are known, but there is no connected synchronizer on which all the informees are hosted."""
    )
    @Resolution(
      "Ensure that there is such a synchronizer, as Canton requires a synchronizer where all informees are present."
    )
    object InformeesNotActive
        extends ErrorCode(
          id = "INFORMEES_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(synchronizers: Set[SynchronizerId], informees: Set[LfPartyId])
          extends TransactionErrorImpl(
            cause = "There is no common synchronizer where all informees are active"
          )
          with TransactionRoutingError
    }

    @Explanation(
      """This error indicates that there is no common synchronizer to which all submitters can submit and all informees are connected."""
    )
    @Resolution(
      "Check that your participant node is connected to all synchronizers you expect and check that the parties are hosted on these synchronizers as you expect them to be."
    )
    object NoCommonSynchronizer
        extends ErrorCode(
          id = "NO_COMMON_SYNCHRONIZER",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(submitters: Set[LfPartyId], informees: Set[LfPartyId])
          extends TransactionErrorImpl(
            cause =
              "There is no common synchronizer to which all submitters can submit and all informees are connected."
          )
          with TransactionRoutingError
    }

    @Explanation(
      """This error indicates that the transaction requires contract reassignments for which the submitter must be a stakeholder."""
    )
    @Resolution(
      "Check that your participant node is connected to all synchronizers you expect and check that the parties are hosted on these synchronizers as you expect them to be."
    )
    object SubmitterAlwaysStakeholder
        extends ErrorCode(
          id = "SUBMITTER_ALWAYS_STAKEHOLDER",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(cids: Seq[LfContractId])
          extends TransactionErrorImpl(
            cause = "The given contracts cannot be reassigned as no submitter is a stakeholder."
          )
          with TransactionRoutingError
    }

    @Explanation(
      """This error indicates that no valid synchronizer was found for submission."""
    )
    @Resolution(
      "Check the status of your synchronizer connections, that packages are vetted and that you are connected to synchronizers running recent protocol versions."
    )
    object NoSynchronizerForSubmission
        extends ErrorCode(
          id = "NO_SYNCHRONIZER_FOR_SUBMISSION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      /** @param synchronizersNotUsed The reason why each synchronizer cannot be used for submission.
        */
      final case class Error(synchronizersNotUsed: Map[SynchronizerId, String])
          extends TransactionErrorImpl(
            cause = "No valid synchronizer for submission found."
          )
          with TransactionRoutingError {}
    }

    @Explanation(
      """This error indicates that the transaction is referring to contracts on synchronizers to which this participant is currently not connected."""
    )
    @Resolution("Check the status of your synchronizer connections.")
    object NotConnectedToAllContractSynchronizers
        extends ErrorCode(
          id = "NOT_CONNECTED_TO_ALL_CONTRACT_SYNCHRONIZERS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Error(contractIds: Map[String, SynchronizerId])
          extends TransactionErrorImpl(
            cause =
              s"The given contracts ${contractIds.keySet} reside on synchronizers ${contractIds.values} to which this participant is currently not connected."
          )
          with TransactionRoutingError
    }

    @Explanation(
      """This error indicates that the transaction is referring to contracts whose synchronizer is not currently known."""
    )
    @Resolution(
      "Ensure all reassignment operations on contracts used by the transaction have completed and check connectivity to synchronizers."
    )
    object UnknownContractSynchronizers
        extends ErrorCode(
          id = "UNKNOWN_CONTRACT_SYNCHRONIZERS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Error(contractIds: List[String])
          extends TransactionErrorImpl(
            cause =
              s"The synchronizers for the contracts $contractIds are currently unknown due to ongoing contract reassignments or disconnected synchronizers"
          )
          with TransactionRoutingError {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.ContractId, contractIds.mkString("[", ", ", "]"))
        )
      }
    }
  }

  @Explanation(
    """This error indicates that the automated reassignment could not succeed, as the current topology does not
      allow the reassignment to complete, mostly due to lack of confirmation permissions of the involved parties."""
  )
  @Resolution(
    """Inspect the message and your topology and ensure appropriate permissions exist."""
  )
  object AutomaticReassignmentForTransactionFailure
      extends ErrorCode(
        id = "AUTOMATIC_REASSIGNMENT_FOR_TRANSACTION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failed(reason: String)
        extends TransactionErrorImpl(
          cause = "Automatically reassigning contracts to a common synchronizer failed."
        )
        with TransactionRoutingError
  }

  @Explanation(
    """This error indicates that topology information could not be queried."""
  )
  @Resolution(
    """Check that the participant is connected to the synchronizer."""
  )
  object UnableToQueryTopologySnapshot
      extends ErrorCode(
        id = "UNABLE_TO_GET_TOPOLOGY_SNAPSHOT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failed(synchronizerId: SynchronizerId)
        extends TransactionErrorImpl(
          cause = s"Participant is not connected to synchronizer `$synchronizerId`."
        )
        with TransactionRoutingErrorWithSynchronizer
  }

  @Explanation(
    """This error indicates an internal error in the Canton synchronizer router."""
  )
  @Resolution(
    """Please contact support."""
  )
  object RoutingInternalError
      extends ErrorCode(
        id = "ROUTING_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class IllformedTransaction(reason: String)
        extends TransactionErrorImpl(
          cause = "Illformed transaction received."
        )
        with TransactionRoutingError

    // should not happen as this is caught earlier
    final case class InputContractsOnDifferentSynchronizers(
        synchronizerIds: Iterable[SynchronizerId]
    ) extends TransactionErrorImpl(
          cause = "Input contracts are on different synchronizers"
        )
        with TransactionRoutingError
  }
}
