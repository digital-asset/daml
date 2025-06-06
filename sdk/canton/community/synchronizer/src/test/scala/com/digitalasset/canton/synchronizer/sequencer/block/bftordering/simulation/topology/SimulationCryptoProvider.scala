// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.KeyPurpose.Signing
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.provider.symbolic.{
  SymbolicCrypto,
  SymbolicPrivateCrypto,
  SymbolicPureCrypto,
}
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import org.scalatest.Assertions.fail

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

final case class SimulationCryptoProvider(
    thisNode: BftNodeId,
    nodesToTopologyData: Map[BftNodeId, NodeSimulationTopologyData],
    crypto: SymbolicCrypto,
    timestamp: CantonTimestamp,
    loggerFactory: NamedLoggerFactory,
) extends CryptoProvider[SimulationEnv] {

  private def fetchSigningKey(): Either[SyncCryptoError, Fingerprint] =
    nodesToTopologyData.get(thisNode) match {
      case Some(topologyData) =>
        Right(
          // We pick the last, to be similar to the real implementation that picks the latest added one (that is still valid)
          topologyData.keysForTimestamp(timestamp).lastOption match {
            case Some(value) =>
              value.publicKey.fingerprint
            case None =>
              fail(s"There are no signing keys for $thisNode at $timestamp")
          }
        )
      case None =>
        lazy val keyNotFound = Left(
          SyncCryptoError.KeyNotAvailable(
            SequencerNodeId
              .fromBftNodeId(thisNode)
              .getOrElse(
                fail(s"Failed to convert BFT node ID $thisNode to SequencerId")
              ),
            Signing,
            timestamp,
            Seq.empty,
          )
        )
        keyNotFound
    }

  private def validKeys(member: BftNodeId): Map[Fingerprint, SigningPublicKey] =
    nodesToTopologyData.get(member) match {
      case Some(topologyData) =>
        topologyData
          .keysForTimestamp(timestamp)
          .map { keyPair =>
            keyPair.publicKey.fingerprint -> keyPair.publicKey
          }
          .toMap
      case None => Map.empty
    }

  override def signHash(hash: Hash, operationId: String)(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): SimulationFuture[Either[SyncCryptoError, Signature]] = SimulationFuture(s"sign($hash)") { () =>
    Try {
      innerSign(hash)
    }
  }

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): SimulationFuture[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    SimulationFuture("signMessage") { () =>
      Try {
        innerSign(CryptoProvider.hashForMessage(message, message.from, authenticatedMessageType))
          .map(SignedMessage(message, _))
      }
    }

  private def innerSign(hash: Hash): Either[SyncCryptoError, Signature] =
    for {
      fingerprint <- fetchSigningKey()
      signature =
        crypto.sign(hash, fingerprint, SigningKeyUsage.ProtocolOnly)
    } yield signature

  override def verifySignature(
      hash: Hash,
      member: BftNodeId,
      signature: Signature,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): SimulationFuture[Either[SignatureCheckError, Unit]] =
    SimulationFuture(s"verifySignature($hash, $member)") { () =>
      Try {
        innerVerifySignature(
          hash,
          validKeys(member),
          signature,
          member,
          SigningKeyUsage.ProtocolOnly,
        )
      }
    }

  private def innerVerifySignature(
      hash: Hash,
      validKeys: Map[Fingerprint, SigningPublicKey],
      signature: Signature,
      signerStr: => String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] =
    validKeys.get(signature.signedBy) match {
      case Some(key) =>
        crypto.pureCrypto.verifySignature(hash, key, signature, usage)
      case None =>
        val error =
          if (validKeys.isEmpty)
            SignerHasNoValidKeys(
              s"There are no valid keys for $signerStr but received message signed with ${signature.signedBy}"
            )
          else
            SignatureWithWrongKey(
              s"Key ${signature.signedBy.unwrap} used to generate signature is not a valid key for $signerStr. Valid keys are ${validKeys.values
                  .map(_.fingerprint.unwrap)} at $timestamp"
            )
        Left(error)
    }
}

object SimulationCryptoProvider {

  def create(
      thisNode: BftNodeId,
      sequencerToTopologyData: Map[BftNodeId, NodeSimulationTopologyData],
      timestamp: CantonTimestamp,
      loggerFactory: NamedLoggerFactory,
  ): SimulationCryptoProvider = {

    implicit val ec: ExecutionContext =
      DirectExecutionContext(loggerFactory.getLogger(this.getClass))

    val pureCrypto = new SymbolicPureCrypto()
    val cryptoPublicStore = new InMemoryCryptoPublicStore(loggerFactory)
    val cryptoPrivateStore =
      new InMemoryCryptoPrivateStore(ReleaseProtocolVersion.latest, loggerFactory)
    val privateCrypto =
      new SymbolicPrivateCrypto(pureCrypto, cryptoPrivateStore, ProcessingTimeout(), loggerFactory)

    implicit val traceContext: TraceContext = TraceContext.empty

    sequencerToTopologyData
      .get(thisNode)
      .foreach { topologyData =>
        topologyData.keysForTimestamp(timestamp).foreach { keys =>
          Await.result(
            cryptoPrivateStore
              .storePrivateKey(keys.privateKey, None)
              .value
              .unwrap,
            10.seconds,
          )
        }
      }

    val crypto =
      new SymbolicCrypto(
        pureCrypto,
        privateCrypto,
        cryptoPrivateStore,
        cryptoPublicStore,
        ProcessingTimeout(),
        loggerFactory,
      )

    SimulationCryptoProvider(
      thisNode,
      sequencerToTopologyData,
      crypto,
      timestamp,
      loggerFactory,
    )
  }
}
