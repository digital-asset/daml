// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.protocol.party.PartyReplicationAcsReader.*
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.chaining.scalaUtilChainingOps

/** The ACS reader encapsulates the ledger API ACS pekko source and a size-bound in-memory queue
  * with associated flow-control to ensure that the SP does not read too far ahead of the contracts
  * that the TP has requested.
  *
  * It also provides a helper method to read and dequeue contracts from the queue in a safe manner.
  */
private[party] final class PartyReplicationAcsReader(
    extractLedgerApiACS: TraceContext => Source[ActiveContract, NotUsed],
    spStore: SourceParticipantStore,
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
)(implicit
    ec: ExecutionContext,
    traceContext: TraceContext,
    actorSystem: ActorSystem,
) extends NamedLogging
    with FlagCloseable {

  // Queue needs to be synchronized for thread-safety
  private val queue = mutable.Queue.empty[ActiveContract]

  // Completed flag set only once/if the ACS reader stream has completed successfully.
  private val hasAcsReaderCompletedSuccessfully = new AtomicBoolean(false)

  private val (killSwitch, doneF) =
    extractLedgerApiACS(traceContext)
      .viaMat(KillSwitches.single)(Keep.right)
      .zipWithIndex
      // Use mapAsync(parallelism=1) for flow-control rather than map
      // to ensure that flow-control runs in the execution context instead
      // of blocking activity on the entire pekko stream.
      .mapAsync(parallelism = 1) { case (activeContract, ordinal) =>
        @tailrec
        def processActiveContract(): Unit = {
          val canProceed =
            if (isClosing) {
              true // if closing, skip processing further active contracts to allow completing the flow
            } else if (spStore.initialContractOrdinalInclusiveO.isEmpty) {
              false // flow-control until initialized source participant processor is initialized
            } else
              blocking {
                synchronized {
                  if (queue.sizeIs >= maxQueueSize.unwrap) {
                    false // flow-control until queue has space
                  } else if (ordinal < spStore.contractOrdinalToSendUpToExclusive.unwrap.toLong) {
                    // Skip any active contracts before the initial contract ordinal (if any).
                    if (
                      spStore.initialContractOrdinalInclusiveO.exists(ordinal >= _.unwrap.toLong)
                    ) {
                      logger.debug(
                        s"Queue.appending active contract with ordinal $ordinal: ${activeContract.contract.getCreatedEvent.contractId}"
                      )
                      queue.append(activeContract)
                    }
                    true // proceed to next active contract
                  } else {
                    false // flow-control until TP requests more contracts
                  }
                }
              }

          if (!canProceed) {
            blocking(Threading.sleep(flowControlBackoffMillis))
            processActiveContract()
          }
        }

        Future.successful(processActiveContract())
      }
      .watchTermination()(Keep.left)
      .toMat(Sink.ignore)(Keep.both)
      .run()
      .tap { case (_, df) =>
        logger.info(s"Started ACS reader flow")
        df.onComplete { maybeDoneT =>
          logger.info(s"ACS source stream completed with: $maybeDoneT")
          // For the reader to have completed successfully, the stream must complete successfully
          // and not as a result of closing.
          if (maybeDoneT.isSuccess && !isClosing) {
            hasAcsReaderCompletedSuccessfully.set(true)
          }
        }
      }

  /** Helper to read up to `numContractsToRead` contracts from the ACS queue and dequeue them.
    * @return
    *   A tuple of true iff we have read and sent the entire ACS and a potentially empty batch of
    *   contracts.
    */
  def readContracts(
      numContractsToRead: PositiveInt
  ): (Boolean, Seq[ActiveContract]) = blocking {
    synchronized {
      // Return contract batch if numContractsToRead are in the queue or if the ACS replication has
      // completed successfully indicating that there might be fewer or no entries in the queue.
      val isAcsReaderFinished = hasAcsReaderCompletedSuccessfully.get()
      val canReturnContractBatch =
        queue.sizeIs >= numContractsToRead.unwrap || isAcsReaderFinished
      val batch = if (canReturnContractBatch) {
        (0 until numContractsToRead.unwrap).flatMap(_ => queue.dequeueFirst(_ => true))
      } else Seq.empty
      val isDone = isAcsReaderFinished && queue.isEmpty
      (isDone, batch)
    }
  }

  override protected def onClosed(): Unit = {
    logger.info("Shutting down ACS source stream kill switch")(traceContext)
    killSwitch.shutdown()
    timeouts.closing.await_("Completing ACS source stream shutdown")(doneF)
  }
}

private object PartyReplicationAcsReader {
  // TODO(#22251): Make this configurable. The maxQueueSize cannot be below the TP request-batch-size
  //  PartyReplicationTargetParticipantProcessor.contractsToRequestEachTime to avoid a deadlock.
  lazy val maxQueueSize: PositiveInt = PositiveInt.tryCreate(1000)
  lazy val flowControlBackoffMillis = 1000L
}
