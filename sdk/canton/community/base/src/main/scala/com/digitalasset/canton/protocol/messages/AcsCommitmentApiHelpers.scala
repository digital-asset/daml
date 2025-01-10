// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import slick.jdbc.{GetResult, SetParameter}

final case class DomainSearchCommitmentPeriod(
    indexedSynchronizer: IndexedSynchronizer,
    fromExclusive: CantonTimestamp,
    toInclusive: CantonTimestamp,
) extends PrettyPrinting {
  override protected def pretty: Pretty[DomainSearchCommitmentPeriod] =
    prettyOfClass(
      param("synchronizerId", _.indexedSynchronizer.synchronizerId),
      param("fromExclusive", _.fromExclusive),
      param("toInclusive", _.toInclusive),
    )
}

sealed trait CommitmentPeriodState extends Product with Serializable with PrettyPrinting {
  def toInt: Int

  def toReceivedCommitmentStateProtoV30: v30.ReceivedCommitmentState =
    this match {
      case CommitmentPeriodState.Matched =>
        v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MATCH
      case CommitmentPeriodState.Mismatched =>
        v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MISMATCH
      case CommitmentPeriodState.Buffered =>
        v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_BUFFERED
      case CommitmentPeriodState.Outstanding =>
        v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_OUTSTANDING
      case CommitmentPeriodState.NotCompared =>
        v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_UNSPECIFIED
    }

  override protected def pretty: Pretty[CommitmentPeriodState] =
    prettyOfClass(
      param("state", _.toInt)
    )
}

sealed trait ValidSentPeriodState extends CommitmentPeriodState {
  def toSentCommitmentStateProtoV30: v30.SentCommitmentState =
    this match {
      case CommitmentPeriodState.Matched => v30.SentCommitmentState.SENT_COMMITMENT_STATE_MATCH
      case CommitmentPeriodState.Mismatched =>
        v30.SentCommitmentState.SENT_COMMITMENT_STATE_MISMATCH
      case CommitmentPeriodState.NotCompared =>
        v30.SentCommitmentState.SENT_COMMITMENT_STATE_NOT_COMPARED
      case CommitmentPeriodState.Outstanding =>
        v30.SentCommitmentState.SENT_COMMITMENT_STATE_NOT_COMPARED
    }
}

object CommitmentPeriodState extends {
  case object NotCompared extends ValidSentPeriodState { val toInt = 1 }
  case object Matched extends ValidSentPeriodState { val toInt = 2 }
  case object Mismatched extends ValidSentPeriodState { val toInt = 3 }
  case object Buffered extends CommitmentPeriodState { val toInt = 4 }
  case object Outstanding extends ValidSentPeriodState { val toInt = 5 }

  private val states: Map[Int, CommitmentPeriodState] =
    Seq(NotCompared, Matched, Mismatched, Buffered, Outstanding).map(x => (x.toInt, x)).toMap

  private val validSentStates: Map[Int, ValidSentPeriodState] =
    Seq(NotCompared, Matched, Mismatched, Outstanding).map(x => (x.toInt, x)).toMap

  def fromInt(i: Int): CommitmentPeriodState =
    states.getOrElse(i, NotCompared)

  def fromIntValidSentPeriodState(i: Int): Option[ValidSentPeriodState] =
    validSentStates.get(i)

  implicit val getCommitmentPeriodState: GetResult[CommitmentPeriodState] =
    GetResult(r => fromInt(r.nextInt()))

  implicit val setCommitmentPeriodState: SetParameter[CommitmentPeriodState] =
    (c, pp) => pp >> c.toInt

  def fromProtoV30(state: v30.SentCommitmentState): ParsingResult[CommitmentPeriodState] =
    state match {
      case v30.SentCommitmentState.SENT_COMMITMENT_STATE_MATCH =>
        Right(CommitmentPeriodState.Matched)
      case v30.SentCommitmentState.SENT_COMMITMENT_STATE_MISMATCH =>
        Right(CommitmentPeriodState.Mismatched)
      case v30.SentCommitmentState.SENT_COMMITMENT_STATE_NOT_COMPARED =>
        Right(CommitmentPeriodState.NotCompared)
      case _ => Left(UnrecognizedEnum.apply("sent_commitment_state", state.value))
    }

  def fromProtoV30(state: v30.ReceivedCommitmentState): ParsingResult[CommitmentPeriodState] =
    state match {
      case v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MATCH =>
        Right(CommitmentPeriodState.Matched)
      case v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_MISMATCH =>
        Right(CommitmentPeriodState.Mismatched)
      case v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_BUFFERED =>
        Right(CommitmentPeriodState.Buffered)
      case v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_OUTSTANDING =>
        Right(CommitmentPeriodState.Outstanding)
      case v30.ReceivedCommitmentState.RECEIVED_COMMITMENT_STATE_UNSPECIFIED =>
        Right(CommitmentPeriodState.NotCompared)
      case _ => Left(UnrecognizedEnum.apply("received_commitment_state", state.value))
    }
}

final case class SentAcsCommitment(
    synchronizerId: SynchronizerId,
    interval: CommitmentPeriod,
    counterParticipant: ParticipantId,
    sentCommitment: Option[CommitmentType],
    counterCommitment: Option[CommitmentType],
    state: ValidSentPeriodState,
)

object SentAcsCommitment {

  def compare(
      synchronizerId: SynchronizerId,
      computed: Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)],
      received: Iterable[AcsCommitment],
      outstanding: Iterable[(CommitmentPeriod, ParticipantId, ValidSentPeriodState)],
      verbose: Boolean,
  ): Iterable[SentAcsCommitment] =
    for {
      (period, participant, commitment) <- computed
      (_, _, state) = outstanding
        .find { case (outstandingPeriod, counterParticipant, _) =>
          counterParticipant == participant &&
          period.overlaps(outstandingPeriod)
        }
        .getOrElse((period, participant, CommitmentPeriodState.NotCompared))
      receivedCommitment =
        if (verbose)
          received
            .find(received =>
              received.sender == participant &&
                received.period.overlaps(period)
            )
            .map(_.commitment)
        else None

    } yield {
      SentAcsCommitment(
        synchronizerId,
        period,
        participant,
        receivedCommitment,
        Option.when(verbose)(commitment),
        state,
      )
    }

  def toProtoV30(sents: Iterable[SentAcsCommitment]): Seq[v30.SentAcsCommitmentPerSynchronizer] = {
    sents.groupBy(_.synchronizerId).map { case (domain, commitment) =>
      v30.SentAcsCommitmentPerSynchronizer(
        domain.toProtoPrimitive,
        commitment.map { comm =>
          v30.SentAcsCommitment(
            Some(
              v30.Interval(
                Some(comm.interval.fromExclusive.toProtoTimestamp),
                Some(comm.interval.toInclusive.toProtoTimestamp),
              )
            ),
            comm.counterParticipant.toProtoPrimitive,
            comm.sentCommitment,
            comm.counterCommitment,
            comm.state.toSentCommitmentStateProtoV30,
          )
        }.toSeq,
      )
    }
  }.toSeq
}

final case class ReceivedAcsCommitment(
    synchronizerId: SynchronizerId,
    interval: CommitmentPeriod,
    originCounterParticipant: ParticipantId,
    receivedCommitment: Option[CommitmentType],
    ownCommitment: Option[CommitmentType],
    state: CommitmentPeriodState,
)

object ReceivedAcsCommitment {

  def compare(
      synchronizerId: SynchronizerId,
      received: Iterable[AcsCommitment],
      computed: Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)],
      buffering: Iterable[AcsCommitment],
      outstanding: Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)],
      verbose: Boolean,
  ): Iterable[ReceivedAcsCommitment] =
    (for {
      recCmt <- received

      (_, _, state) = outstanding
        .find { case (period, counterParticipant, _) =>
          counterParticipant == recCmt.sender &&
          recCmt.period.overlaps(period)
        }
        .getOrElse(
          (
            recCmt.period,
            recCmt.sender,
            if (
              buffering.exists(b => b.sender == recCmt.sender && recCmt.period.overlaps(b.period))
            )
              CommitmentPeriodState.Buffered
            else
              CommitmentPeriodState.NotCompared,
          )
        )
      computedCommitment =
        if (verbose)
          computed
            .find { case (compPeriod, compCounterParticipant, _) =>
              compCounterParticipant == recCmt.sender &&
              recCmt.period.overlaps(compPeriod)
            }
            .map { case (_, _, commitment) => commitment }
        else None
    } yield {
      ReceivedAcsCommitment(
        synchronizerId,
        recCmt.period,
        recCmt.sender,
        Option.when(verbose)(recCmt.commitment),
        computedCommitment,
        state,
      )
    }) ++ buffering.map(cmt =>
      ReceivedAcsCommitment(
        cmt.synchronizerId,
        cmt.period,
        cmt.sender,
        Option.when(verbose)(cmt.commitment),
        None,
        CommitmentPeriodState.Buffered,
      )
    )
  def toProtoV30(
      received: Iterable[ReceivedAcsCommitment]
  ): Seq[v30.ReceivedAcsCommitmentPerSynchronizer] = {
    received.groupBy(_.synchronizerId).map { case (domain, commitment) =>
      v30.ReceivedAcsCommitmentPerSynchronizer(
        domain.toProtoPrimitive,
        commitment.map { cmt =>
          v30.ReceivedAcsCommitment(
            Some(
              v30.Interval(
                Some(cmt.interval.fromExclusive.toProtoTimestamp),
                Some(cmt.interval.toInclusive.toProtoTimestamp),
              )
            ),
            cmt.originCounterParticipant.toProtoPrimitive,
            cmt.receivedCommitment,
            cmt.ownCommitment,
            cmt.state.toReceivedCommitmentStateProtoV30,
          )
        }.toSeq,
      )
    }
  }.toSeq
}
