// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCategory.{
  BackgroundProcessDegradationWarning,
  InvalidGivenCurrentSystemStateOther,
  InvalidIndependentOfSystemState,
}
import com.daml.error.*
import com.digitalasset.canton.error.EnterpriseSequencerErrorGroups.EthereumErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.EthereumContractVersion
import org.slf4j.event.Level

import java.math.BigInteger

object EthereumErrors extends EthereumErrorGroup {

  object ConfigurationErrors extends ErrorGroup {
    @Explanation(
      """This error is logged when the sequencer detects that the version of the Besu client
        |it connects to is not what is expected / supported.
        |"""
    )
    @Resolution(
      """Either deploy the documented required version or set
        |canton.parameters.non-standard-config = true."""
    )
    object BesuVersionMismatch
        extends ErrorCode("BESU_VERSION_MISMATCH", InvalidIndependentOfSystemState) {
      final case class Error(actual: String, expected: String)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"The sequencer is configured to interact with a Besu client of version $expected but" +
              s" found $actual"
          )
    }

    @Explanation(
      """This error is logged when during setup the sequencer detects that it isn't
        |connected to a free-gas network. This usually leads to transactions silently being dropped by Ethereum nodes.
        |You should only use a non-free-gas network, if you have configured an Ethereum wallet for the sequencer to use
        |and have given it gas.
        |"""
    )
    @Resolution("""Change the configuration of the Ethereum network to a free-gas network. """)
    object NotFreeGasNetwork
        extends ErrorCode("NOT_FREE_GAS_NETWORK", InvalidIndependentOfSystemState) {
      final case class Error(gasCost: Long)(implicit val logger: ContextualizedErrorLogger)
          extends SequencerBaseError.Impl(
            s"Gas cost of Ethereum network is $gasCost Gwei and not zero."
          )
    }

    @Explanation(
      """This error is logged when the sequencer is currently processing blocks that are very far behind
        |the head of the blockchain of the connected Ethereum network. The Ethereum sequencer won't observe new transactions
        |in the blockchain until it has caught up to the head. This may take a long time depending on the blockchain length
        |and number of Canton transaction in the blocks. Empirically, we have observed that the Canton sequencer
        | processes roughly 500 empty blocks/second. This may vary strongly for non-empty blocks.
        | The sequencer logs once it has caught up to within `blocksBehindBlockchainHead` blocks behind the blockchain head.
        |"""
    )
    @Resolution(
      """Wait until the sequencer has caught up to the head
        | of the blockchain. Alternatively, consider changing the configuration of `block-to-read-from` of the Ethereum
        | sequencer when initializing it against an Ethereum network that already mined a lot of blocks. """
    )
    object ManyBlocksBehindHead
        extends ErrorCode("MANY_BLOCKS_BEHIND_HEAD", BackgroundProcessDegradationWarning) {
      final case class Warn(currBlock: Long, blockchainHead: Long)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends SequencerBaseError.Impl(
            s"The Ethereum sequencer is ${blockchainHead - currBlock} blocks behind the head of the blockchain. " +
              "Catching up might take a while. Consider updating `block-to-read-from`."
          ) {
        override implicit def logger: ContextualizedErrorLogger = loggingContext
      }
    }

    @Explanation(
      """This warning is logged on startup if the sequencer is configured to only start reading from a block
        |that wasn't mined yet by the blockchain (e.g. sequencer is supposed to start reading from block 500, but
        |the latest block is only 100). This is likely due to a misconfiguration.
        |"""
    )
    @Resolution(
      """This issue frequently occurs when the blockchain is reset but the sequencer database configuration is
        |  not updated or the sequencer database (which persists the last block that was read by the sequencer) is not reset.
        |  Validate these settings and ensure that the sequencer is still reading from the same blockchain. """
    )
    object AheadOfHead extends ErrorCode("AHEAD_OF_HEAD", BackgroundProcessDegradationWarning) {
      final case class Warn(firstBlockToRead: Long, blockchainHead: Long)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"The Ethereum sequencer attempted to start reading from block $firstBlockToRead but the last block that was mined is only block $blockchainHead. The Ethereum sequencer might be reading from the wrong blockchain."
          )
    }

    @Explanation(
      """The sequencer smart contract has detected that a value that is immutable after being set for the first time
        | (either the signing tolerance or the topology manager ID) was attempted to be changed.
        |   Most frequently this error occurs during testing when a Canton Ethereum sequencer process without persistence
        | is restarted while pointing to the same smart sequencer contract. An Ethereum sequencer attempts to set the
        | topology manager ID during initialization, however, without persistence the topology manager ID is randomly
        | regenerated on the restart which leads to the sequencer attempting to change the topology manager ID in the
        | sequencer smart contract.
        |"""
    )
    @Resolution(
      """Deploy a new instance of the sequencer contract and configure the Ethereum sequencer to use that instance.
        | If the errors occur because an Ethereum sequencer process is restarted without persistence, deploy a fresh
        | instance of the sequencer contract and configure persistence for restarts.
        |"""
    )
    object AttemptToChangeImmutableValue
        extends ErrorCode(
          "ATTEMPT_TO_CHANGE_IMMUTABLE_VALUE",
          InvalidGivenCurrentSystemStateOther,
        ) {
      final case class TopologyManagerId(message: String)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(message)

    }

    @Explanation(
      """Canton validates on startup that the configured address on the blockchain contains the EVM bytecode of the
        | sequencer smart contract in the latest block. This error indicates that no bytecode or the wrong bytecode was found.
        | This is a serious error and means that the sequencer can't sequence events.
        |"""
    )
    @Resolution("""This frequently error occurs when updating the Canton system without updating the sequencer
                  | contract deployed on the blockchain. Validate that the sequencer contract corresponding to the current Canton release
                  | is deployed in the latest blockchain blocks on the configured address.
                  | Another common reason for this error is that the wrong contract address was configured.
                  |""")
    object WrongEVMBytecode
        extends ErrorCode("WRONG_EVM_BYTECODE", InvalidGivenCurrentSystemStateOther) {
      final case class Error(
          foundBytecode: String,
          detectedEthereumContractVersion: EthereumContractVersion,
      )(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            "The sequencer contract address did not contain the expected Ethereum contract EVM bytecode in the latest block."
          ) {}

      override def logLevel: Level = Level.WARN
    }

    @Explanation("""
                   |As one of the first steps when initializing a Besu sequencer, Canton attempts to query the version (attribute)
                   |of the Sequencer.sol contract.
                   |""")
    @Resolution("""Usually, the root cause of this is a deployment or configuration problem.
                  | Ensure that a Sequencer.sol contract is deployed on the configured address on the latest block when attempting
                  |  to initialize the Canton Besu sequencer node. If this error persists, a malicious user may
                  |  be attempting to interfere with the Ethereum network.
                  |""")
    object UnableToQueryVersion
        extends ErrorCode("ETHEREUM_CANT_QUERY_VERSION", InvalidGivenCurrentSystemStateOther) {
      final case class Error(throwable: Throwable)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            "Received an error when attempting to query the version of the Sequencer.sol contract in the latest block.",
            Some(throwable),
          )

      override def logLevel: Level = Level.WARN
    }
  }

  object TransactionErrors extends ErrorGroup {
    @Explanation("""
                   |This error is logged when the Sequencer Ethereum application receives an error when attempting to submit a
                   | transaction to the transaction pool of the Ethereum client. Common causes for this are network errors,
                   | or when the Ethereum account of the Sequencer Ethereum application is used by another application.
                   | Less commonly, this error might also indirectly be caused if the transaction pool of the Ethereum client is
                   | full or flushed.
                   |""")
    @Resolution("""Generally, Canton should recover automatically from this error.
                  | If you continue to see this error, investigate possible root causes such as poor network connections,
                  | if the Ethereum wallet of the Ethereum Sequencer application is being reused by another application, and the
                  | health of the Ethereum client.
                  |""")
    object SubmissionFailed
        extends ErrorCode(
          "ETHEREUM_TRANSACTION_SUBMISSION_FAILED",
          BackgroundProcessDegradationWarning,
        ) {
      final case class Warn(payload: String, throwable: Throwable)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"Ethereum transaction submission failed: $throwable ",
            throwableO = Some(throwable),
          )
    }

    @Explanation("""
                   |This error happens when the Sequencer Ethereum application reads a transaction from the blockchain
                   | which is malformed (e.g, invalid member, arguments aren't parseable or too large).
                   |This could happen if a malicious or faulty Ethereum Sequencer node is placing faulty data on the
                   | blockchain.
                   |""")
    @Resolution("""Generally, Canton should recover automatically from this error.
                  | The faulty transactions are simply skipped by all non-malicious/non-faulty sequencers in a deterministic way,
                  | so the integrity of the event stream across sequencer nodes should be maintained.
                  | If you continue to see this error, investigate whether some of the sequencer nodes in the network are misbehaving.
                  |""")
    object InvalidTransaction extends AlarmErrorCode("ETHEREUM_TRANSACTION_INVALID") {

      final case class Warn[EventResponse](
          msg: String,
          blockHeight: BigInteger,
          eventResponse: EventResponse,
      ) extends Alarm(
            s"At block $blockHeight found invalid transaction. That indicates malicious or faulty behavior, so skipping it. Error: $msg"
          )
    }

    @Explanation("""This error occurs when the Ethereum sequencer attempts to fetch the transaction receipt for a
                   |previously submitted transaction but receives an exception. Usually, this is caused by network errors,
                   |the Ethereum client node being overloaded or the Ethereum sequencer reaching its `transactionReceiptPollingAttempts`
                   |for a given transaction.
                   |The fetching of transaction receipts of submitted transactions is separate from the Ethereum sequencer's
                   |read-stream used to ingest new transactions. Thus, in this sense, this error is purely informative and can be
                   |caused by transient issues (such as a transient network outage). Note, that the Canton nonce manager
                   |refreshes his cache whenever this error occurs which may unblock stuck transactions with a too-high nonce.
                   |""")
    @Resolution("""Usually, this error should resolve by itself. If you frequently see this error, ensure that
                  |the Ethereum account of the Ethereum sequencer is used by no one else and that the Ethereum client doesn't
                  |drop submitted transactions through being overloaded or reaching a full txpool. If this errors keeps occurring,
                  |please contact support.
                  |""")
    object ReceiptFetchingFailed
        extends ErrorCode(
          "ETHEREUM_TRANSACTION_RECEIPT_FETCHING_FAILED",
          BackgroundProcessDegradationWarning,
        ) {
      final case class Warn(e: Throwable)(implicit
          val logger: ContextualizedErrorLogger
      ) extends SequencerBaseError.Impl(
            s"Fetching the transaction receipt of an Ethereum transaction generated an exception: ${e.getMessage}",
            throwableO = Some(e),
          )
    }
  }
}
