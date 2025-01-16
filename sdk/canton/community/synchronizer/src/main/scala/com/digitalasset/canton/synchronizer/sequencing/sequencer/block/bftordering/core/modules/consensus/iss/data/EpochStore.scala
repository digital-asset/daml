// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.db.DbEpochStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.InMemoryEpochStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNetworkMessage,
  PbftViewChangeMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

import EpochStore.{Block, Epoch, EpochInProgress}

trait EpochStore[E <: Env[E]] extends AutoCloseable {

  def startEpoch(epoch: EpochInfo)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  protected def startEpochActionName(epoch: EpochInfo): String =
    s"start epoch ${epoch.number}"

  def completeEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  protected def completeEpochActionName(epochNumber: EpochNumber): String =
    s"complete epoch $epochNumber"

  def latestEpoch(includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Epoch]
  protected def latestCompletedEpochActionName: String = "fetch latest completed epoch"

  def addPrePrepare(prePrepare: SignedMessage[PrePrepare])(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  protected def addPrePrepareActionName(prePrepare: SignedMessage[PrePrepare]): String =
    s"add PrePrepare ${prePrepare.message.blockMetadata.blockNumber} epoch: ${prePrepare.message.blockMetadata.epochNumber}"

  def addPrepares(prepares: Seq[SignedMessage[Prepare]])(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected def addPreparesActionName: String = "add Prepares"

  /** Storing view-change and new-view messages for in-progress segments is important in order to properly rehydrate
    * the segment states after a crash and restart.
    *
    * We store all new-view messages, in order to indicate we've definitely moved to the indicated views.
    * The pre-prepares in the new-view messages, together with prepares stored separately during that view
    * will allow the node to either make progress in that view or build prepare certificates to change into another view.
    *
    * We only store locally-created view-change messages, in order to indicate that this node started a view change,
    * either because of a timeout or because of gathering a weak quorum of view-change messages from other nodes.
    * Once a correct node starts a view change to v+1, and sends the view-change message to other peers, it should not
    * go back to working on view v after a restart.
    */
  def addViewChangeMessage[M <: PbftViewChangeMessage](viewChangeMessage: SignedMessage[M])(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected def addViewChangeMessageActionName[M <: PbftViewChangeMessage](
      viewChangeMessage: SignedMessage[M]
  ): String = viewChangeMessage.message match {
    case newView: NewView =>
      s"add NewView for view ${newView.viewNumber}, segment ${newView.segmentIndex} and leader ${newView.from}"
    case viewChange: ViewChange =>
      s"add ViewChange for ${viewChange.viewNumber} and segment ${viewChange.segmentIndex}"
  }

  def addOrderedBlock(
      prePrepare: SignedMessage[PrePrepare],
      commitMessages: Seq[SignedMessage[Commit]],
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  def addOrderedBlockActionName(epochNumber: EpochNumber, blockNumber: BlockNumber): String =
    s"Add OrderedBlock $blockNumber epoch: $epochNumber"

  def loadEpochProgress(activeEpochInfo: EpochInfo)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[EpochInProgress]
  protected def loadEpochProgressActionName(activeEpochInfo: EpochInfo): String =
    s"load epoch progress ${activeEpochInfo.number}"

  def loadCompleteBlocks(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[Block]]
  protected def loadPrePreparesActionName(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  ): String =
    s"load complete blocks from $startEpochNumberInclusive to $endEpochNumberInclusive"
}

object EpochStore {
  // Can we remove these classes and merge with Epoch/Blocks from Data?
  final case class Epoch(
      info: EpochInfo,
      lastBlockCommits: Seq[SignedMessage[Commit]],
  )

  final case class Block(
      epochNumber: EpochNumber,
      blockNumber: BlockNumber,
      commitCertificate: CommitCertificate,
  )

  final case class EpochInProgress(
      completedBlocks: Seq[Block] = Seq.empty, // Expected to be sorted by height
      pbftMessagesForIncompleteBlocks: Seq[SignedMessage[PbftNetworkMessage]] = Seq.empty,
  )

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): EpochStore[PekkoEnv] & OrderedBlocksReader[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryEpochStore()
      case dbStorage: DbStorage =>
        new DbEpochStore(dbStorage, timeouts, loggerFactory)(ec)
    }
}
