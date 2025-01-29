// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

import OutputMetadataStore.{OutputBlockMetadata, OutputEpochMetadata}

abstract class GenericInMemoryOutputMetadataStore[E <: Env[E]] extends OutputMetadataStore[E] {

  private val blocks: TrieMap[BlockNumber, OutputBlockMetadata] = TrieMap.empty

  private val epochs: TrieMap[EpochNumber, OutputEpochMetadata] = TrieMap.empty

  protected def createFuture[T](action: String)(value: () => Try[T]): E#FutureUnlessShutdownT[T]

  override def insertBlockIfMissing(
      metadata: OutputBlockMetadata
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(insertBlockIfMissingActionName(metadata)) { () =>
      val key = metadata.blockNumber
      blocks.putIfAbsent(key, metadata) match {
        case None => Success(())
        case Some(value) =>
          if (value == metadata) Success(())
          else
            Failure(
              new RuntimeException(
                s"Updating existing entry in block metadata store is illegal: key: $key, oldValue: $value, newValue: $metadata"
              )
            )
      }
    }

  override def insertEpochIfMissing(
      metadata: OutputEpochMetadata
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(insertEpochIfMissingActionName(metadata)) { () =>
      val key = metadata.epochNumber
      epochs.putIfAbsent(key, metadata) match {
        case None => Success(())
        case Some(value) =>
          if (value == metadata) Success(())
          else
            Failure(
              new RuntimeException(
                s"Updating existing entry in epoch metadata store is illegal: key: $key, oldValue: $value, newValue: $metadata"
              )
            )
      }
    }

  override def getEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Option[OutputEpochMetadata]] =
    createFuture(getEpochMetadataActionName(epochNumber)) { () =>
      Success(epochs.get(epochNumber))
    }

  override def getBlockFromInclusive(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Seq[OutputBlockMetadata]] =
    createFuture(getFromInclusiveActionName(initialBlockNumber)) { () =>
      Success(
        blocks
          .collect {
            case (blockNumber, block) if blockNumber >= initialBlockNumber => block
          }
          .toSeq
          .sortBy(_.blockNumber)
          // because we may insert blocks out of order, we need to
          // make sure to never return a sequence of blocks with a gap
          .zipWithIndex
          .takeWhile { case (block, index) =>
            index + initialBlockNumber == block.blockNumber
          }
          .map(_._1)
      )
    }

  override def getLatestBlockAtOrBefore(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]] =
    createFuture(getLatestAtOrBeforeActionName(timestamp)) { () =>
      Success(
        blocks
          .collect {
            case (_, block) if block.blockBftTime <= timestamp => block
          }
          .maxByOption(_.blockNumber)
      )
    }

  override def getFirstBlockInEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]] =
    createFuture(getFirstInEpochActionName(epochNumber)) { () =>
      Success(sortedBlocksForEpoch(epochNumber).headOption)
    }

  override def getLastBlockInEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]] =
    createFuture(getFirstInEpochActionName(epochNumber)) { () =>
      Success(sortedBlocksForEpoch(epochNumber).lastOption)
    }

  private def sortedBlocksForEpoch(epochNumber: EpochNumber) =
    blocks
      .collect {
        case (_, block) if block.epochNumber == epochNumber => block
      }
      .toSeq
      .sortBy(_.blockNumber)

  override def getLastConsecutiveBlock(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]] =
    createFuture(lastConsecutiveActionName)(() =>
      Success(
        blocks.keySet.toSeq.sorted.zipWithIndex
          .takeWhile { case (blockNumber, index) => blockNumber == index }
          .map { case (blockNumber, _) => blockNumber }
          .maxOption
          .map(blocks)
      )
    )
}

class InMemoryOutputMetadataStore extends GenericInMemoryOutputMetadataStore[PekkoEnv] {
  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): PekkoFutureUnlessShutdown[T] =
    PekkoFutureUnlessShutdown(action, FutureUnlessShutdown.fromTry(value()))
  override def close(): Unit = ()
}
