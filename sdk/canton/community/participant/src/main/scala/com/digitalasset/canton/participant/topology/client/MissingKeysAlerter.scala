// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Monitor topology updates and alert on missing keys */
class MissingKeysAlerter(
    participantId: ParticipantId,
    synchronizerId: SynchronizerId,
    client: SynchronizerTopologyClient,
    cryptoPrivateStore: CryptoPrivateStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def init()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      encryptionKeys <- client.currentSnapshotApproximation.encryptionKeys(participantId)
      signingKeys <- client.currentSnapshotApproximation.signingKeys(
        participantId,
        SigningKeyUsage.All,
      )
    } yield {
      encryptionKeys.foreach(key => alertOnMissingKey(key.fingerprint, KeyPurpose.Encryption))
      signingKeys.foreach(key => alertOnMissingKey(key.fingerprint, KeyPurpose.Signing))
    }

  def attachToTopologyProcessor(): TopologyTransactionProcessingSubscriber =
    new TopologyTransactionProcessingSubscriber {
      override def observed(
          sequencedTimestamp: SequencedTime,
          effectiveTimestamp: EffectiveTime,
          sequencerCounter: SequencerCounter,
          transactions: Seq[GenericSignedTopologyTransaction],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.pure(
          processTransactions(effectiveTimestamp.value, transactions)
        )
    }

  private def processTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit =
    // scan state and alarm if the domain suggest that I use a key which I don't have
    transactions.view
      .filter(tx => tx.operation == TopologyChangeOp.Replace && !tx.isProposal)
      .map(_.mapping)
      .foreach {
        case ParticipantSynchronizerPermission(
              `synchronizerId`,
              `participantId`,
              permission,
              _,
              _,
            ) =>
          logger.info(
            s"Domain $synchronizerId update my participant permission as of $timestamp to $permission"
          )
        case OwnerToKeyMapping(`participantId`, keys) =>
          keys.foreach(k => alertOnMissingKey(k.fingerprint, k.purpose))
        case _ => ()
      }

  private def alertOnMissingKey(fingerprint: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): Unit = {
    lazy val errorMsg =
      s"Error checking if key $fingerprint associated with this participant node on domain $synchronizerId is present in the public crypto store"
    cryptoPrivateStore.existsPrivateKey(fingerprint, purpose).value.onComplete {
      case Success(Outcome(Right(false))) =>
        logger.error(
          s"On domain $synchronizerId, the key $fingerprint for $purpose is associated with this participant node, but this key is not present in the private crypto store."
        )
      case Success(Outcome(Left(storeError))) => logger.error(errorMsg, storeError)
      case Success(AbortedDueToShutdown) => ()
      case Failure(exception) => logger.error(errorMsg, exception)
      case _ => ()
    }
  }

}
