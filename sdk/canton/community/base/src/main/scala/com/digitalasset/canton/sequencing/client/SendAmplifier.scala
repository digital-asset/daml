// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either.*
import com.digitalasset.canton.config
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{PerformUnlessClosing, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{MessageId, SignedContent, SubmissionRequest}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil

import scala.concurrent.{ExecutionContext, blocking}

/** Sends a single signed submission request to possibly multiple sequencers concurrently.
  *
  * @param signedRequest The signed submission request to send. Should have an aggregation rule in place if it shall be amplified; otherwise it may get delivered multiple times.
  * @param puc The [[com.digitalasset.canton.lifecycle.PerformUnlessClosing]] that should be used to decide when to stop retrying.
  * @param peekAtSendResult Resends are stopped when this function returns [[scala.Some$]]
  */
class SendAmplifier(
    clock: Clock,
    signedRequest: SignedContent[SubmissionRequest],
    sequencersTransportState: SequencersTransportState,
    override protected val loggerFactory: NamedLoggerFactory,
    peekAtSendResult: () => Option[UnlessShutdown[SendResult]],
    puc: PerformUnlessClosing,
    timeout: scala.concurrent.duration.Duration,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import SendAmplifier.*

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var state: State = Initial

  /** Guards access to [[state]] */
  private val lock: AnyRef = new Object

  /** Sends the signed submission request to once sequencer chosen by the [[com.digitalasset.canton.sequencing.client.SequencersTransportState]]. */
  def sendOnce()(implicit traceContext: TraceContext): Unit =
    pickSequencerAndSend(amplify = false, _ => true)

  /** Sends the signed submission request to possibly multiple sequencers, as configured in the [[com.digitalasset.canton.sequencing.client.SequencersTransportState]]'s
    * [[com.digitalasset.canton.sequencing.SubmissionRequestAmplification]].
    * If the configuration changes in between retries, the new configuration will be used for picking the next sequencer
    * and for the subsequent retries. But the configuration change does not affect whether and when the next retry will happen.
    *
    * Properties:
    * - Waits for at most [[com.digitalasset.canton.sequencing.SubmissionRequestAmplification.patience]] (measured on the local [[com.digitalasset.canton.time.Clock]])
    *   before retrying
    * - If a sequencer synchronously responds with an error, retries immediately.
    * - Avoids previously attempted sequencers as long as there are enough other healthy ones.
    * - Stops retrying when the submission request was observed on the client or the client is closed.
    */
  def sendAmplified()(implicit traceContext: TraceContext): Unit =
    pickSequencerAndSend(amplify = true, _ => true)

  private def pickSequencerAndSend(
      amplify: Boolean,
      condition: Token => Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    val messageId = signedRequest.content.messageId

    // Using a lock instead of a CAS loop because nextAmplifiedTransport is a blocking operation
    // and we should not call a blocking operation inside a CAS loop.
    blocking(lock.synchronized {
      for {
        previousSequencers <- state match {
          case Initial => Right(Seq.empty)
          case SendAttempt(attemptedSequencers, token) =>
            Either.cond(condition(token), attemptedSequencers, ())
          case DeliveredOrAborted => Left(())
        }
        _ <- peekAtSendResult().toLeft(()).leftMap { result =>
          state = DeliveredOrAborted
          reportDeliverResult(messageId, result)
        }
      } yield {
        val (sequencerAlias, sequencerId, transport, patienceO) =
          sequencersTransportState.nextAmplifiedTransport(previousSequencers)
        val token = new Object
        state = SendAttempt(sequencerId +: previousSequencers, token)
        (sequencerAlias, sequencerId, transport, patienceO, token)
      }
    }).foreach { case (sequencerAlias, sequencerId, transport, patienceO, token) =>
      val sendF =
        puc.performUnlessClosingUSF(s"sending message $messageId to sequencer $sequencerId") {
          logger.info(
            s"Sending message ID $messageId to sequencer $sequencerId (alias $sequencerAlias) with max sequencing time ${signedRequest.content.maxSequencingTime}"
          )
          transport.sendAsyncSigned(signedRequest, timeout).value.map { outcome =>
            noTracingLogger.whenDebugEnabled {
              outcome match {
                case Right(()) =>
                  logger.debug(
                    s"Sending message ID $messageId to sequencer $sequencerId (alias $sequencerAlias) succeeded"
                  )
                case Left(error) =>
                  logger.info(
                    s"Sending message ID $messageId to sequencer $sequencerId (alias $sequencerAlias) failed with $error"
                  )
              }
            }

            if (patienceO.isEmpty || !amplify) {
              blocking(lock.synchronized {
                state = DeliveredOrAborted
              })
              logger.info(s"No (further) amplification for message ID $messageId")
            } else if (outcome.isLeft) {
              // Immediately resend upon errors
              logger.debug(s"Immediately resending message ID $messageId")
              pickSequencerAndSend(amplify, tok => tok eq token)
            }
          }
        }

      FutureUtil.doNotAwaitUnlessShutdown(
        sendF,
        s"Sending message $messageId to sequencer $sequencerId",
      )

      if (amplify) {
        patienceO match {
          case Some(patience) =>
            scheduleAmplification(patience, messageId, token)
          case None =>
            logger.info(s"Not scheduling amplification for message ID $messageId")
        }
      }
    }
  }

  private def reportDeliverResult(messageId: MessageId, result: UnlessShutdown[SendResult])(implicit
      traceContext: TraceContext
  ): Unit =
    noTracingLogger.whenDebugEnabled {
      result match {
        case Outcome(SendResult.Success(deliver)) =>
          logger.debug(
            s"Aborting amplification for message ID $messageId because it has been sequenced at ${deliver.timestamp}"
          )
        case Outcome(SendResult.Error(error)) =>
          // The BFT sequencer group has decided that the submission request shall not be sequenced.
          // We accept this decision here because amplification is about overcoming individual faulty sequencer nodes,
          // not about dealing with concurrent changes that might render the request valid later on (e.g., traffic top-ups)
          logger.info(
            s"Aborting amplification for message ID $messageId because it has been rejected at ${error.timestamp}"
          )
        case Outcome(SendResult.Timeout(timestamp)) =>
          logger.info(
            s"Aborting amplification for message ID $messageId because the max sequencing time ${signedRequest.content.maxSequencingTime} has elapsed: observed time $timestamp"
          )
        case AbortedDueToShutdown =>
          logger.info(
            s"Aborting amplification for message ID $messageId due to shutdown"
          )
      }
    }

  private def scheduleAmplification(
      patience: config.NonNegativeFiniteDuration,
      messageId: MessageId,
      token: Token,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"Scheduling amplification for message ID $messageId after $patience"
    )
    FutureUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAfter(
        _ => pickSequencerAndSend(amplify = true, tok => tok eq token),
        patience.asJava,
      ),
      s"Submission request amplification failed for message ID $messageId",
    )
  }

}

object SendAmplifier {

  /** Internal state of the retry loop for send amplification */
  private sealed trait State extends Product with Serializable

  /** Nothing has been sent so far */
  private case object Initial extends State

  /** THe submission request has been sent to the sequencers in [[attemptedSequencers]].
    * @param token A token to identify what piece of logic last updated the state. Each state update must insert a fresh token.
    *              Scheduled retries after the patience race with immediate retries upon synchronous sequencer rejections.
    *              Both perform a retry only if the token matches the one that was used to update their state.
    *              This ensures that there is a linear sequence of retries, even though the retries themselves may overlap.
    */
  private final case class SendAttempt(attemptedSequencers: Seq[SequencerId], token: Token)
      extends State

  /** The submission request was observed on the stream of sequenced events or retries have been aborted
    * (e.g., due to shutdown or hitting the amplification factor
    */
  private case object DeliveredOrAborted extends State

  private type Token = AnyRef
}
