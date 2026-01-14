// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction.Transaction.toJavaProto
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  TransactionWrapper,
  UpdateWrapper,
}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.{HashOps, SyncCryptoApi, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers.{
  MessageTransform,
  SignedMessageTransform,
}
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.integration.{HasCycleUtils, TestConsoleEnvironment}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.LockedContracts
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.{LocalRejectError, RequestId}
import com.digitalasset.canton.sequencing.protocol.{SignedContent, SubmissionRequest}
import com.digitalasset.canton.synchronizer.mediator.MediatorVerdict
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.{
  isConfirmationResponse,
  isConfirmationResult,
}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.{Member, ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.{BaseTest, LfPartyId}
import io.grpc.Status.Code
import io.grpc.stub.StreamObserver
import monocle.macros.GenLens
import monocle.syntax.all.*
import org.scalatest.Assertion

import java.time.Duration as JDuration
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.Ordering.Implicits.*
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.Try
import scala.util.control.NonFatal

/** A collection of utility methods for intercepting messages at the sequencer and for manipulating
  * messages.
  *
  * Limitation: the utility methods are tailored to transaction processing. Further tweaking is
  * required to apply them to transfer processing.
  */
trait SecurityTestHelpers extends SecurityTestLensUtils {
  self: BaseTest & HasProgrammableSequencer & HasCycleUtils =>

  // participant verdicts
  lazy val localApprove: LocalApprove = LocalApprove(testedProtocolVersion)
  lazy val localReject: LockedContracts.Reject =
    LocalRejectError.ConsistencyRejections.LockedContracts.Reject(Seq.empty)

  // mediator verdicts
  lazy val mediatorApprove: Verdict.Approve = Verdict.Approve(testedProtocolVersion)
  def participantReject(participantId: ParticipantId): Verdict.ParticipantReject =
    Verdict.ParticipantReject(
      NonEmpty(
        List,
        (Set.empty[LfPartyId], participantId, localReject.toLocalReject(testedProtocolVersion)),
      ),
      testedProtocolVersion,
    )
  lazy val mediatorReject: MediatorReject =
    MediatorVerdict
      .MediatorReject(MediatorError.MalformedMessage.Reject("malformed message test cause"))
      .toVerdict(testedProtocolVersion)

  /** Runs `body` while replacing confirmation responses at the sequencer. As a reminder,
    * confirmation responses are messages sent by the participants to the mediator, through the
    * sequencer.
    *
    * The method will apply every `transform` in `messageTransforms` to an incoming confirmation
    * response `message` yielding a sequence `newMessages` of confirmation responses. It will
    * replace the original response `message` by `newMessages`.
    *
    * If `newMessages` is empty, the method will advance the sim clock (if any) by the participant
    * confirmation response timeout.
    *
    * The method will fail, if the sequencer does not receive a confirmation response for the
    * mediator at `sequencerRef`. The method will also fail, if the sequencer receives a
    * confirmation response for the mediator at `sequencerRef` from a sender other than `sender`.
    *
    * @param senderRef
    *   only confirmation responses sent by `sender` will be replaced
    * @param sequencerRef
    *   only confirmation responses submitted to this sequencer will be replaced
    */
  def replacingConfirmationResponses[A](
      senderRef: LocalParticipantReference,
      sequencerRef: LocalSequencerReference,
      synchronizerId: PhysicalSynchronizerId,
      messageTransforms: SignedMessageTransform[ConfirmationResponses]*
  )(body: => A)(implicit env: TestConsoleEnvironment): A = {

    val receivedResponseP = Promise[Unit]()

    val confirmationResponseTimeout =
      sequencerRef.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .confirmationResponseTimeout
        .asJava
    val simClockO = env.environment.simClock

    val result = getProgrammableSequencer(sequencerRef.name).withSendPolicy_(
      s"Replacing confirmation responses",
      message =>
        if (!isConfirmationResponse(message)) SendDecision.Process
        else if (message.sender == senderRef.id) {
          receivedResponseP.trySuccess(())

          val newMessages = messageTransforms.map(_.apply(senderRef, message, synchronizerId))

          // Unchecked needed because of: https://github.com/scala/bug/issues/12252
          (newMessages: @unchecked) match {
            case head +: tail => SendDecision.Replace(head, tail*)

            case Seq() =>
              // Advance the clock to trigger a timeout at the mediator.
              simClockO.foreach(_.advance(confirmationResponseTimeout))
              // This extra second is needed, because the clock may be slightly behind of the request time.
              simClockO.foreach(_.advance(JDuration.ofSeconds(1)))

              SendDecision.Drop
          }
        } else {
          receivedResponseP.tryComplete(
            Try(fail(s"Received unexpected verdict from ${message.sender}"))
          )
          SendDecision.Process
        },
    )(body)

    receivedResponseP.future.futureValue

    result
  }

  /** Collects encrypted view messages that are part of confirmation requests while executing
    * `body`, then applies `checkMessages` to the collected messages. Only messages sent by the
    * specified sender are included.
    */
  def checkingEncryptedViewMessages[A](
      hashOps: HashOps,
      sender: ParticipantReference,
      sequencerRef: SequencerReference,
  )(
      body: => A
  )(
      checkMessages: Seq[EncryptedViewMessage[ViewType]] => Assertion
  ): A = {

    val requestsB = Seq.newBuilder[EncryptedViewMessage[ViewType]]
    val result = getProgrammableSequencer(sequencerRef.name).withSendPolicy_(
      s"Collecting encrypted view messages",
      { message =>
        if (
          message.isConfirmationRequest &&
          sender.id == message.sender
        ) {
          blocking {
            requestsB.synchronized {
              val allProtocolMessages = message.batch.envelopes.map(
                _.openEnvelope(hashOps, testedProtocolVersion)
                  .valueOrFail("open envelopes")
                  .protocolMessage
              )
              requestsB ++= allProtocolMessages.collect {
                case encryptedView: EncryptedViewMessage[ViewType] => encryptedView
              }
            }
          }
        }
        SendDecision.Process
      },
    )(body)

    val requests = blocking {
      requestsB.synchronized {
        requestsB.result()
      }
    }

    checkMessages(requests)

    result
  }

  /** Collects confirmation responses while running `body` and applies `checkResponses` to the
    * collected confirmation responses.
    *
    * It will only collect responses from `participantRefs` to any mediator group. It will check the
    * `requestId` of the collected responses and only collect responses with the earliest requestId;
    * as a result, all collected responses will refer to the first request.
    */
  def checkingConfirmationResponses[A](
      participantRefs: Seq[ParticipantReference],
      sequencerRef: SequencerReference,
  )(
      body: => A
  )(
      checkResponses: Map[ParticipantId, Seq[ConfirmationResponse]] => Assertion
  )(implicit env: TestConsoleEnvironment): A = {
    import env.*

    // Access needs to be synchronized through itself
    val responsesB = Seq.newBuilder[ConfirmationResponses]

    // Earliest request id seen so far
    val requestIdO = new AtomicReference[Option[RequestId]](None)

    val result = getProgrammableSequencer(sequencerRef.name).withSendPolicy_(
      s"Collecting confirmation responses",
      { message =>
        blocking {
          responsesB.synchronized {
            if (
              isConfirmationResponse(message) &&
              participantRefs.exists(_.id == message.sender)
            ) {
              val messages = traverseMessages[ConfirmationResponses](_ => None).getAll(message)

              val requestId =
                requestIdO
                  .updateAndGet(
                    OptionUtil.mergeWith(_, messages.headOption.map(_.requestId)) {
                      case (oldRequestId, newRequestId) =>
                        if (newRequestId >= oldRequestId) oldRequestId
                        else {
                          // The messages refer to an earlier requestId.
                          // Discard collected responses, because they do not refer to the earliest requestId.
                          responsesB.clear()
                          newRequestId
                        }
                    }
                  )
                  .value

              responsesB ++= messages.filter(_.requestId == requestId)
            }
          }
        }

        SendDecision.Process
      },
    )(body)

    val responses = blocking {
      responsesB.synchronized {
        responsesB.result()
      }
    }

    val responsesGroupedBySender =
      responses.groupMap(_.sender)(_.responses).view.mapValues(_.flatten).toMap

    checkResponses(responsesGroupedBySender)

    result
  }

  /** Runs `body` while replacing confirmation results at the sequencer. As a reminder, confirmation
    * results are messages sent by the mediator to the participants, through the sequencer.
    *
    * The method will apply every `transform` in `messageTransforms` to an incoming confirmation
    * result `message` yielding a sequence `newMessages` of confirmation results. It will replace
    * the original result `message` by `newMessages`.
    *
    * If `newMessages` is empty, the method will advance the sim clock (if any) by the participant
    * confirmation response timeout plus the mediator reaction timeout.
    *
    * The method will fail, if the sequencer does not receive a confirmation response for the
    * mediator at `synchronizerRef`.
    *
    * Only confirmation results submitted to `sequencerRef` of `synchronizerId` and originating from
    * `mediatorRef` will be replaced.
    *
    * @return
    *   The result of body as well as intercepted messages, by sender. It can be used to check that
    *   honest participants rejected a transaction.
    */
  def replacingConfirmationResult[A](
      synchronizerId: PhysicalSynchronizerId,
      sequencerRef: LocalSequencerReference,
      mediatorRef: LocalMediatorReference,
      messageTransforms: SignedMessageTransform[ConfirmationResultMessage]*
  )(body: => A)(implicit env: TestConsoleEnvironment): (A, Map[Member, Seq[SubmissionRequest]]) = {

    val receivedResultP = Promise[Unit]()

    val synchronizerParameters =
      sequencerRef.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(
        synchronizerId
      )
    val simClockO = env.environment.simClock

    val interceptedMessages = TrieMap[Member, Seq[SubmissionRequest]]()

    val result = getProgrammableSequencer(sequencerRef.name).withSendPolicy_(
      "Replacing confirmation results",
      message => {
        interceptedMessages.updateWith(message.sender) {
          case Some(messages) =>
            // Number of messages is sufficiently small that appending is fine
            Some(messages :+ message)
          case None => Some(Seq(message))
        }

        if (isConfirmationResult(message, mediatorRef.id)) {
          receivedResultP.trySuccess(())

          val newMessages = messageTransforms.map(_.apply(mediatorRef, message, synchronizerId))

          // Unchecked needed because of: https://github.com/scala/bug/issues/12252
          (newMessages: @unchecked) match {
            case head +: tail =>
              SendDecision.Replace(head, tail*)

            case Seq() =>
              // Advance the clock to trigger a timeout at the participants.
              simClockO.foreach(
                _.advance(synchronizerParameters.confirmationResponseTimeout.asJava)
              )
              // This extra second is needed, because the clock may be slightly behind of the request time.
              simClockO.foreach(_.advance(JDuration.ofSeconds(1)))
              simClockO.foreach(_.advance(synchronizerParameters.mediatorReactionTimeout.asJava))
              // Additionally, advance the sim clock by the mediator's observation timeout
              // so that it observes the timeout. Otherwise, the warning about the timeout
              // may spill over into the next test.
              simClockO.foreach { simClock =>
                val minObservationDuration =
                  mediatorRef.underlying.value.replicaManager.mediatorRuntime.value.mediator.timeTracker.config.minObservationDuration
                simClock.advance(minObservationDuration.asJava.plusSeconds(1))
              }
              SendDecision.Drop
          }
        } else SendDecision.Process
      },
    )(body)

    receivedResultP.future.futureValue

    (result, interceptedMessages.toMap)
  }

  /** Creates a [[SignedMessageTransform]] to replace the verdict in a confirmation response. The
    * transform will automatically update the signature of the confirmation response, as well as
    * sign and send the resulting submission request as the original.
    */
  def withLocalVerdict(
      verdict: LocalVerdict
  )(implicit executionContext: ExecutionContext): SignedMessageTransform[ConfirmationResponses] =
    signedTransformOf(
      traverseMessages[ConfirmationResponses](_)
        .modify(cr =>
          ConfirmationResponses.tryCreate(
            cr.requestId,
            cr.rootHash,
            cr.psid,
            cr.sender,
            cr.responses.map(cr => ConfirmationResponse.localVerdictUnsafe.replace(verdict)(cr)),
            testedProtocolVersion,
          )
        )(_)
    )

  def withLocalVerdict(
      verdict: LocalRejectError
  )(implicit executionContext: ExecutionContext): SignedMessageTransform[ConfirmationResponses] =
    withLocalVerdict(verdict.toLocalReject(testedProtocolVersion))

  /** Creates a [[SignedMessageTransform]] to replace the verdict in a confirmation response.
    *
    * @param senderRef
    *   member reference used as the submission request sender and to sign the confirmation response
    *   and submission request.
    */
  def withLocalVerdict(
      verdict: LocalVerdict,
      senderRef: LocalInstanceReference,
  )(implicit executionContext: ExecutionContext): SignedMessageTransform[ConfirmationResponses] =
    signedTransformWithSender(
      traverseMessages[ConfirmationResponses](_)
        .modify(cr =>
          ConfirmationResponses.tryCreate(
            cr.requestId,
            cr.rootHash,
            cr.psid,
            cr.sender,
            cr.responses.map(cr => ConfirmationResponse.localVerdictUnsafe.replace(verdict)(cr)),
            testedProtocolVersion,
          )
        )(_),
      senderRef,
    )

  /** Creates a [[MessageTransform]] to replace the verdict in a transaction result message. The
    * transform will automatically update the signature of the confirmation result, as well as sign
    * and send the resulting submission request as the original.
    */
  def withMediatorVerdict(
      verdict: Verdict
  )(implicit
      executionContext: ExecutionContext
  ): SignedMessageTransform[ConfirmationResultMessage] =
    signedTransformOf(
      traverseMessages[ConfirmationResultMessage](_)
        .andThen(GenLens[ConfirmationResultMessage](_.verdict))
        .replace(verdict)(_)
    )

  /** Creates a [[MessageTransform]] to replace the verdict in a transaction result message.
    *
    * @param senderRef
    *   member reference used as the submission request sender and to sign the verdict and
    *   submission request.
    */
  def withMediatorVerdict(
      verdict: Verdict,
      senderRef: LocalInstanceReference,
  )(implicit
      executionContext: ExecutionContext
  ): SignedMessageTransform[ConfirmationResultMessage] =
    signedTransformWithSender(
      traverseMessages[ConfirmationResultMessage](_)
        .andThen(GenLens[ConfirmationResultMessage](_.verdict))
        .replace(verdict)(_),
      senderRef,
    )

  /** Convenience method to create a [[SignedMessageTransform]] from a [[MessageTransform]], signing
    * and sending the submission request as the original.
    *
    * @param useCurrentSnapshot
    *   use the current snapshot when signing then message content
    */
  def signedTransformOf[A <: SignedProtocolMessageContent](
      transform: MessageTransform[A],
      useCurrentSnapshot: Boolean = false,
      approximateTimestampOverride: Option[CantonTimestamp] = None,
  )(implicit
      executionContext: ExecutionContext
  ): SignedMessageTransform[A] =
    (senderRef, submissionRequest, synchronizerId) => {
      val (sender, syncCrypto) = senderAndCryptoFromRef(senderRef, synchronizerId)

      (transform(updateSignatureUsing(syncCrypto, useCurrentSnapshot), _))
        .andThen(_.focus(_.sender).replace(sender))
        // TODO(i16512): See if we should pass `useCurrentSnapshot` to sign the submission request
        .andThen(signModifiedSubmissionRequest(_, syncCrypto, approximateTimestampOverride))(
          submissionRequest
        )
    }

  /** Convenience method to create a [[SignedMessageTransform]] from a [[MessageTransform]] signing
    * and sending the submission request as `senderRef`.
    *
    * @param useCurrentSnapshot
    *   use the current snapshot when signing then message content
    */
  def signedTransformWithSender[A <: SignedProtocolMessageContent](
      transform: MessageTransform[A],
      senderRef: LocalInstanceReference,
      useCurrentSnapshot: Boolean = false,
      approximateTimestampOverride: Option[CantonTimestamp] = None,
  )(implicit
      executionContext: ExecutionContext
  ): SignedMessageTransform[A] =
    (_, submissionRequest, synchronizerId) => {
      val (sender, syncCrypto) = senderAndCryptoFromRef(senderRef, synchronizerId)

      (transform(updateSignatureUsing(syncCrypto, useCurrentSnapshot), _))
        .andThen(_.focus(_.sender).replace(sender))
        // TODO(i16512): See if we should pass `useCurrentSnapshot` to sign the submission request
        .andThen(signModifiedSubmissionRequest(_, syncCrypto, approximateTimestampOverride))(
          submissionRequest
        )
    }

  private def senderAndCryptoFromRef(
      ref: LocalInstanceReference,
      synchronizerId: PhysicalSynchronizerId,
  ): (Member, SynchronizerCryptoClient) = ref match {
    case p: LocalParticipantReference =>
      (
        p.id: Member,
        p.underlying.value.sync.syncCrypto
          .tryForSynchronizer(synchronizerId, defaultStaticSynchronizerParameters),
      )
    case m: LocalMediatorReference =>
      (m.id, m.underlying.value.replicaManager.mediatorRuntime.value.mediator.syncCrypto)
    case _ => fail(s"Unexpected reference: $ref")
  }

  private def updateSignatureUsing[A <: SignedProtocolMessageContent](
      syncCrypto: SynchronizerCryptoClient,
      useCurrentSnapshot: Boolean,
  ): A => Option[SyncCryptoApi] =
    message => {
      if (useCurrentSnapshot) Some(syncCrypto.currentSnapshotApproximation.futureValueUS)
      else
        Some(
          message.signingTimestamp
            .map(syncCrypto.awaitSnapshot(_).futureValueUS)
            .getOrElse(syncCrypto.headSnapshot)
        )
    }

  /** Tracks completions and transaction while executing a piece of code `body`.
    *
    * After `body` has terminated, run a cycle on every tracked participant and stop tracking as
    * soon as the participant emits a transaction/completion corresponding to the cycle. The
    * transaction/completion corresponding to the cycle will not be included in the tracking result.
    * Tracking will also be aborted, if `body` throws an exception.
    *
    * @param participants
    *   the participants to track
    * @param extraTrackedParties
    *   the parties to track - the admin parties of `participants` will always be tracked.
    * @return
    *   the result of `body` and the tracking result
    */
  def trackingLedgerEvents[A](
      participants: Seq[ParticipantReference],
      extraTrackedParties: Seq[PartyId],
  )(body: => A)(implicit executionContext: ExecutionContext): (
      A,
      TrackingResult,
  ) = {
    // Completes only if there is a submission failure
    val submissionFailedP = Promise[Unit]()

    // Command id used to identify the flush commands at the end
    val finishCommandId = s"finish-tracking-${UUID.randomUUID()}"

    val trackedParties = extraTrackedParties ++ participants.map(_.id.adminParty)

    // Subscribe to transaction trees & completions for all participants
    val (completionsF, transactionsF) = (for (participant <- participants) yield {
      val completionObserver =
        new CollectUntilObserver[Completion](_.commandId == finishCommandId)
      val completionCloseable = participant.ledger_api.completions
        .subscribe(
          completionObserver,
          trackedParties,
          beginOffsetExclusive = participant.ledger_api.state.end(),
        )
      completionObserver.result.onComplete(_ => completionCloseable.close())
      submissionFailedP.future.onComplete(_ => completionCloseable.close())

      val transactionObserver =
        new CollectUntilObserver[UpdateWrapper]({
          case TransactionWrapper(tt) => tt.commandId == finishCommandId
          case _ => false
        })
      val updateFormat = getUpdateFormat(
        partyIds = trackedParties.toSet,
        filterTemplates = Seq.empty,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        includeReassignments = true,
      )
      val ledgerEnd = participant.ledger_api.state.end()
      val transactionCloseable = participant.ledger_api.updates
        .subscribe_updates(
          transactionObserver,
          updateFormat,
          beginOffsetExclusive = ledgerEnd,
        )
      transactionObserver.result.onComplete(_ => transactionCloseable.close())
      submissionFailedP.future.onComplete(_ => transactionCloseable.close())
      val transactions = transactionObserver.result.map(_.collect {
        case TransactionWrapper(transaction) => transaction
      })

      (
        participant -> completionObserver.result,
        participant -> transactions,
      )
    }).separate

    // Run body and abort the tracking on exceptions
    try {
      val result = body

      participants
        .parTraverse(participant =>
          Future(
            runCycle(
              participant.id.adminParty,
              participant,
              participant,
              finishCommandId,
            )
          )
        )
        .futureValue

      (result, new TrackingResult(completionsF.toMap, transactionsF.toMap))
    } catch {
      case NonFatal(ex) =>
        submissionFailedP.trySuccess(())
        throw ex
    }
  }

  private class CollectUntilObserver[A](isDone: A => Boolean) extends StreamObserver[A] {
    private val resultB: mutable.Builder[A, Seq[A]] = Seq.newBuilder[A]
    private val resultP: Promise[Seq[A]] = Promise[Seq[A]]()

    val result: Future[Seq[A]] = resultP.future

    override def onNext(value: A): Unit =
      if (isDone(value)) resultP.trySuccess(resultB.result())
      else resultB += value

    override def onError(t: Throwable): Unit = resultP.tryFailure(t)

    override def onCompleted(): Unit =
      resultP.tryFailure(new IllegalStateException("Subscription has terminated unexpectedly."))
  }

  class TrackingResult(
      completions: Map[ParticipantReference, Future[Seq[Completion]]],
      transactions: Map[ParticipantReference, Future[Seq[Transaction]]],
  ) {

    def assertStatusOk(participant: ParticipantReference): Assertion =
      assertExactlyOneCompletion(participant).status.value.code shouldBe Code.OK.value()

    def assertExactlyOneCompletion(
        participant: ParticipantReference
    ): Completion =
      awaitCompletions.toSeq.mapFilter { case (otherParticipant, completions) =>
        withClue(s"for $otherParticipant") {
          if (otherParticipant == participant) {
            Some(completions.loneElement)
          } else {
            completions shouldBe empty
            None
          }
        }
      }.loneElement

    def assertNoCompletionsExceptFor(participant: ParticipantReference): Assertion =
      forEvery(awaitCompletions) { case (otherParticipant, completions) =>
        if (otherParticipant == participant) succeed
        else
          withClue(s"for $otherParticipant") {
            completions shouldBe empty
          }
      }

    lazy val awaitCompletions: Map[ParticipantReference, Seq[Completion]] = completions.map {
      case (participant, completionsF) =>
        withClue(s"for $participant")(participant -> completionsF.futureValue)
    }

    lazy val awaitTransactions: Map[ParticipantReference, Seq[Transaction]] =
      transactions.map { case (participant, transactionsF) =>
        participant -> withClue(s"for $participant")(transactionsF.futureValue)
      }

    def assertNoTransactions(): Assertion = forEvery(awaitTransactions) {
      case (participant, transactions) =>
        withClue(s"for $participant")(transactions shouldBe empty)
    }

    def allCreated[TC](
        companion: ContractCompanion[TC, ?, ?]
    )(
        participant: ParticipantReference
    ): Seq[TC] =
      awaitTransactions(participant).flatMap(tx =>
        JavaDecodeUtil.decodeAllCreated(companion)(
          javaapi.data.Transaction.fromProto(toJavaProto(tx))
        )
      )

    def allArchived[TCid](
        companion: ContractCompanion[?, TCid, ?]
    )(
        participant: ParticipantReference
    ): Seq[TCid] =
      awaitTransactions(participant).flatMap(tx =>
        JavaDecodeUtil.decodeAllArchivedLedgerEffectsEvents(companion)(
          javaapi.data.Transaction.fromProto(toJavaProto(tx))
        )
      )
  }
}

object SecurityTestHelpers {

  /** Wraps a function updating a [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]].
    * Since the update usually involves updating a signature, the function receives an extra input
    * `updateSignatureWith` that should be used to update any signature.
    */
  trait MessageTransform[A] {

    def apply(
        updateSignatureWith: A => Option[SyncCryptoApi],
        message: SubmissionRequest,
    ): SubmissionRequest
  }

  trait SignedMessageTransform[A] {

    def apply(
        sender: LocalInstanceReference,
        message: SubmissionRequest,
        synchronizerId: PhysicalSynchronizerId,
    ): SignedContent[SubmissionRequest]
  }
}
