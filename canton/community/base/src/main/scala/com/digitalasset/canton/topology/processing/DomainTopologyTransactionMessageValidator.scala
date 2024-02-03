// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import cats.instances.list.*
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{SigningPublicKey, SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  DomainTopologyTransactionMessage,
  ProtocolMessage,
}
import com.digitalasset.canton.topology.TopologyManagerError.TopologyManagerAlarm
import com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot
import com.digitalasset.canton.topology.transaction.{
  ParticipantState,
  RequestSide,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Validate 2.x domain topology transaction messages
  *
  * The domain topology manager distributes the topology state through domain topology messages to
  * all registered and active members. This class is used to verify the correctness of the
  * given signature.
  *
  * Some care is required when validating the signature, as the key used by the domain topology dispatcher
  * is actually defined by the state that it changes.
  *
  * When a new participant connects for the first time, it will receive a first message which is self-consistent,
  * where the message is signed by a key for which we get all topology transactions in the message itself.
  *
  * Therefore, a participant can be sure to talk to the right domain by defining the domain-id on the domain connection,
  * because we will check the topology state against this domain-id too.
  *
  * Replay attacks are prevented using a max-sequencing timestamp which prevents that a message is replayed to different
  * recipients at a later point in time.
  */
// TODO(#15208) Remove
trait DomainTopologyTransactionMessageValidator {

  def initLastMessageTimestamp(lastMessageTs: Option[CantonTimestamp]): Unit = {}

  def extractTopologyUpdatesAndValidateEnvelope(
      ts: SequencedTime,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[List[SignedTopologyTransaction[TopologyChangeOp]]]

}

object DomainTopologyTransactionMessageValidator {

  def create(
      client: SyncCryptoClient[SyncCryptoApi],
      member: Member,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): DomainTopologyTransactionMessageValidator =
    new Impl(client, member, protocolVersion, timeouts, futureSupervisor, loggerFactory)

  object NoValidation extends DomainTopologyTransactionMessageValidator {

    override def extractTopologyUpdatesAndValidateEnvelope(
        ts: SequencedTime,
        envelopes: List[DefaultOpenEnvelope],
    )(implicit
        traceContext: TraceContext,
        closeContext: CloseContext,
    ): FutureUnlessShutdown[List[SignedTopologyTransaction[TopologyChangeOp]]] = {
      FutureUnlessShutdown.pure(
        envelopes
          .mapFilter(ProtocolMessage.select[DomainTopologyTransactionMessage])
          .map(_.protocolMessage)
          .flatMap(_.transactions)
      )
    }
  }

  class Impl(
      client: SyncCryptoClient[SyncCryptoApi],
      member: Member,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends DomainTopologyTransactionMessageValidator
      with NamedLogging {

    private val lastSequencingTimestamp = new AtomicReference[Option[CantonTimestamp]](None)

    override def initLastMessageTimestamp(lastMessageTs: Option[CantonTimestamp]): Unit = {
      lastSequencingTimestamp.getAndSet(lastMessageTs) match {
        case Some(value) =>
          noTracingLogger.error(
            s"Updating the last sequencing timestamp again from=${value} to=${lastMessageTs}"
          )
        case None =>
      }
    }

    private def validateFirstMessage(ts: SequencedTime, message: DomainTopologyTransactionMessage)(
        implicit traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = if (member.code != ParticipantId.Code) {
      // This only applies to the internal sequencer self-connection, which is okay,
      // as nobody can connect to the sequencer before the keys are registered
      logger.debug(
        s"Skipping domain topology transaction signature validation as the right keys are not yet known at ${ts.value}"
      )
      EitherT.pure(())
    } else {
      for {
        // validate that all topology transactions are properly authorized in first snapshot
        store <- SnapshotAuthorizationValidator.validateTransactions(
          timeouts,
          futureSupervisor,
          loggerFactory,
        )(message.transactions)
        // validate that the domain trust certificate is present
        // this ensures that the initial first message really came from the domain topology
        // manager and was really meant for this participant
        _ <- EitherT.cond[FutureUnlessShutdown](
          message.transactions
            .filter(_.transaction.op == TopologyChangeOp.Add)
            .map(_.transaction.element.mapping)
            .exists {
              case ParticipantState(RequestSide.From, domain, participant, permission) =>
                permission.isActive && domain == client.domainId && participant == member
              case _ => false
            },
          (),
          "Initial domain topology transaction message does not contain the participant state (side=From) for this participant!",
        )
        // finally, validate the signature of the domain topology manager using the keys
        // found in the initial message
        snapshot = StoreBasedTopologySnapshot.headstateOfAuthorizedStore(store, loggerFactory)
        keys <- EitherT
          .right(snapshot.allKeys(client.domainId.member))
          .mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          validateMessageAgainstKeys(ts, keys.signingKeys, message)
        )
      } yield ()
    }

    private def validateMessageAgainstKeys(
        ts: SequencedTime,
        keys: Seq[SigningPublicKey],
        message: DomainTopologyTransactionMessage,
    )(implicit traceContext: TraceContext): Either[String, Unit] = {
      val hash = message.hashToSign(client.pureCrypto)
      for {
        key <- keys
          .find(_.id == message.domainTopologyManagerSignature.signedBy)
          .toRight(
            s"Domain manager signature with unknown key=${message.domainTopologyManagerSignature.signedBy}. Known=${keys
                .map(_.id)}. Skipping ${message.transactions.length} transactions!"
          )
        _ <- client.pureCrypto
          .verifySignature(hash, key, message.domainTopologyManagerSignature)
          .leftMap(error =>
            s"Signature checking of envelope failed: ${error}. Skipping ${message.transactions.length} transactions!"
          )
        // check that the message was not replayed. the domain topology manager will submit these messages
        // with the max sequence time which is included in the hash of the message itself.
        // so if this message is sequenced after the given timestamp, then somebody tried to replay it
        //
        // replaying within the max-sequencing-time window is not a problem for already onboarded nodes,
        // as they will just deduplicate the transaction within the tx processor.
        //
        // replaying within the max-sequencing-time window to not yet onboarded nodes is still possible, but
        // the issue is rather unlikely: replaying a remove doesn't matter. replaying an add also doesn't matter
        // as it would be contained in the state. so the only scenario it matters is if you quickly add an
        // add and then remove it immediately. then you can replay the add.
        _ <- Either.cond(
          message.notSequencedAfter >= ts.value,
          (),
          s"Detected malicious replay of a domain topology transaction message: Sequenced at ${ts.value}, but max-sequencing-time is ${message.notSequencedAfter}. Skipping ${message.transactions.length} transactions!",
        )
      } yield {
        logger.debug(
          s"Successfully validated domain manager signature at ts=${ts.value} with key ${message.domainTopologyManagerSignature.signedBy}"
        )
      }
    }

    private def validateMessage(
        ts: SequencedTime,
        lastTs: Option[CantonTimestamp],
        message: DomainTopologyTransactionMessage,
    )(implicit
        traceContext: TraceContext,
        closeContext: CloseContext,
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      for {
        snapshot <- EitherT.right(
          SyncCryptoClient.getSnapshotForTimestampUS(
            client,
            ts.value,
            lastTs,
            protocolVersion,
            warnIfApproximate = false,
          )
        )
        keys <- EitherT
          .right(
            snapshot.ipsSnapshot.signingKeys(client.domainId.member)
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        _ <-
          // first message is validated specially
          if (keys.isEmpty) {
            validateFirstMessage(ts, message)
          } else
            EitherT.fromEither[FutureUnlessShutdown](validateMessageAgainstKeys(ts, keys, message))
      } yield ()
    }

    override def extractTopologyUpdatesAndValidateEnvelope(
        ts: SequencedTime,
        envelopes: List[DefaultOpenEnvelope],
    )(implicit
        traceContext: TraceContext,
        closeContext: CloseContext,
    ): FutureUnlessShutdown[List[SignedTopologyTransaction[TopologyChangeOp]]] = {
      val messages = envelopes
        .mapFilter(ProtocolMessage.select[DomainTopologyTransactionMessage])
        .map(_.protocolMessage)
      val lastTs = lastSequencingTimestamp.getAndSet(Some(ts.value))
      NonEmpty.from(messages) match {
        case None => FutureUnlessShutdown.pure(List.empty)
        case Some(messages) if messages.sizeCompare(1) > 0 =>
          TopologyManagerAlarm
            .Warn(
              s"Received batch with ${messages.size} envelopes, but I expect only a single one!"
            )
            .report()
          FutureUnlessShutdown.pure(List.empty)
        case Some(messages) =>
          validateMessage(ts, lastTs, messages.head1).fold(
            err => {
              TopologyManagerAlarm.Warn(err).report()
              logger.info(
                s"The failing message contained the following transactions: ${messages.head1.transactions
                    .mkString("\n  ")}"
              )
              List.empty
            },
            _ => {
              messages.head1.transactions
            },
          )
      }
    }
  }
}
