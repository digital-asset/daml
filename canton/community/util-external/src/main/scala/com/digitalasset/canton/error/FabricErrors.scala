// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCategory.{BackgroundProcessDegradationWarning, TransientServerFailure}
import com.daml.error.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.error.EnterpriseSequencerErrorGroups.FabricErrorGroup

object FabricErrors extends FabricErrorGroup {
  object ConfigurationErrors extends ErrorGroup {

    @Explanation(
      """This error is logged when the sequencer is currently processing blocks that are very far behind
        |the head of the blockchain of the connected Fabric network. The Fabric sequencer won't observe new transactions
        |in the blockchain until it has caught up to the head. This may take a long time depending on the blockchain length
        |and number of Canton transaction in the blocks. Empirically, we have observed that the Canton sequencer
        |processes roughly 500 empty blocks/second. This may vary strongly for non-empty blocks.
        |"""
    )
    @Resolution(
      """Change the configuration of `startBlockHeight` for the Fabric sequencer when working
        |with an existing (not fresh) Fabric network. Alternatively, wait until the sequencer has caught up to the head
        | of the blockchain. """
    )
    object ManyBlocksBehindHead
        extends ErrorCode("FABRIC_MANY_BLOCKS_BEHIND_HEAD", BackgroundProcessDegradationWarning) {
      final case class Warn(currBlock: Long, blockchainHead: Long)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"The Fabric sequencer is ${blockchainHead - currBlock} blocks behind the head of the blockchain. This might take a while. Consider updating `start-block-height`."
          )
    }

    @Explanation(
      """This warning is logged on startup if the sequencer is configured to only start reading from a block
      |that wasn't ordered yet by the blockchain (e.g. sequencer is supposed to start reading from block 500, but
      |the latest block is only 100). This is likely due to a misconfiguration.
      |"""
    )
    @Resolution(
      """This issue frequently occurs when the blockchain is reset but the sequencer database configuration is
      |  not updated or the sequencer database (which persists the last block that was read by the sequencer) is not reset.
      |  Validate these settings and ensure that the sequencer is still reading from the same blockchain. """
    )
    object AheadOfHead
        extends ErrorCode("FABRIC_AHEAD_OF_HEAD", BackgroundProcessDegradationWarning) {
      final case class Warn(firstBlockToRead: Long, blockchainHead: Long)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"The Fabric sequencer attempted to start reading from block $firstBlockToRead but the last block that was ordered is only block $blockchainHead. The Fabric sequencer might be reading from the wrong channel."
          )
    }
  }

  object TransactionErrors extends ErrorGroup {
    @Explanation("""
                   |An error happened with the Fabric transaction proposal submissions possibly due to
                   |some of the peers being down or due to network issues.
                   |Thus won't stop the transaction workflow, because there might still be enough successful responses
                   |to satisfy the endorsement policy.
                   |Therefore the transaction might still go through successfully despite this being logged.
                   |""")
    @Resolution("""Generally, Canton should recover automatically from this error.
                  | If you continue to see this error, investigate possible root causes such as poor network connections,
                  | if the Fabric sequencer is properly configured with enough peers and if they are running.
                  |""")
    object TransactionProposalSubmissionFailed
        extends ErrorCode(
          "FABRIC_TRANSACTION_PROPOSAL_SUBMISSION_FAILED",
          TransientServerFailure,
        ) {
      final case class Warn(
          fcn: String,
          args: Seq[Array[Byte]],
          transientMap: Map[String, Array[Byte]],
          failedResponses: NonEmpty[List[ProposalResponseSummary]],
      )(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"Transaction proposal error for $fcn. ${failedResponses.size} endorsers failed: ${failedResponses.head1.message}. Was verified: ${failedResponses.head1.verified}"
          )
    }

    final case class ProposalResponseSummary(message: String, verified: Boolean)

    @Explanation("""
                   |This error is logged when the Sequencer Fabric application receives an error during any of the
                   | transaction flow steps that prevents the submission of a transaction over the Fabric client.
                   | Common causes for this are network errors, peers that are down or that there aren't enough configured endorsers.
                   |""")
    @Resolution("""Generally, Canton should recover automatically from this error.
                  | If you continue to see this error, investigate possible root causes such as poor network connections,
                  | if the Fabric sequencer is properly configured with enough peers and if they are running.
                  |""")
    object SubmissionFailed
        extends ErrorCode(
          "FABRIC_TRANSACTION_SUBMISSION_FAILED",
          TransientServerFailure,
        ) {
      final case class Warn(
          fcn: String,
          args: Seq[Array[Byte]],
          transientMap: Map[String, Array[Byte]],
          throwable: Throwable,
      )(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"Fabric transaction submission failed: $throwable ",
            throwableO = Some(throwable),
          )
    }

    @Explanation("""
                   |This error happens when the Sequencer Fabric application reads a transaction from the blockchain
                   | which is malformed (e.g, missing arguments, arguments aren't parseable or too large).
                   |This could happen if a malicious or faulty Fabric Sequencer node is placing faulty data on the
                   | blockchain.
                   |""")
    @Resolution("""Generally, Canton should recover automatically from this error.
                  | The faulty transactions are simply skipped by all non-malicious/non-faulty sequencers in a deterministic way,
                  | so the integrity of the event stream across sequencer nodes should be maintained.
                  | If you continue to see this error, investigate whether some of the sequencer nodes in the network are misbehaving.
                  |""")
    object InvalidTransaction extends AlarmErrorCode("FABRIC_TRANSACTION_INVALID") {

      final case class Warn(fcn: String, _msg: String, blockHeight: Long)
          extends Alarm(
            s"At block $blockHeight found invalid $fcn transaction. That indicates malicious or faulty behavior, so skipping it. Error: ${_msg}"
          )
    }
  }
}
