// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Monitor topology updates and alert on missing keys */
class MissingKeysAlerter(
    participantId: ParticipantId,
    domainId: DomainId,
    client: DomainTopologyClient,
    cryptoPrivateStore: CryptoPrivateStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def init()(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      encryptionKeys <- client.currentSnapshotApproximation.encryptionKeys(participantId)
      signingKeys <- client.currentSnapshotApproximation.signingKeys(participantId)
    } yield {
      encryptionKeys.foreach(key => alertOnMissingKey(key.fingerprint, KeyPurpose.Encryption))
      signingKeys.foreach(key => alertOnMissingKey(key.fingerprint, KeyPurpose.Signing))
    }
  }

  def attachToTopologyProcessorOld(): TopologyTransactionProcessingSubscriber =
    new TopologyTransactionProcessingSubscriber {
      override def observed(
          sequencedTimestamp: SequencedTime,
          effectiveTimestamp: EffectiveTime,
          sequencerCounter: SequencerCounter,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.pure(
          processTransactions(effectiveTimestamp.value, transactions)
        )
    }

  private def processTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Unit = {
    // scan state and alarm if the domain suggest that I use a key which I don't have
    transactions.view
      .filter(_.operation == TopologyChangeOp.Add)
      .map(_.transaction.element.mapping)
      .foreach {
        case ParticipantState(side, `domainId`, `participantId`, permission, trustLevel)
            if side != RequestSide.To =>
          logger.info(
            s"Domain $domainId update my participant permission as of $timestamp to $permission, $trustLevel"
          )
        case okm @ OwnerToKeyMapping(`participantId`, _) =>
          alertOnMissingKey(okm.key.fingerprint, okm.key.purpose)
        case _ => ()
      }
  }

  private def alertOnMissingKey(fingerprint: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): Unit = {
    lazy val errorMsg =
      s"Error checking if key $fingerprint associated with this participant node on domain $domainId is present in the public crypto store"
    cryptoPrivateStore.existsPrivateKey(fingerprint, purpose).value.onComplete {
      case Success(Right(false)) =>
        logger.error(
          s"On domain $domainId, the key $fingerprint for $purpose is associated with this participant node, but this key is not present in the private crypto store."
        )
      case Success(Left(storeError)) => logger.error(errorMsg, storeError)
      case Failure(exception) => logger.error(errorMsg, exception)
      case _ => ()
    }
  }

}
