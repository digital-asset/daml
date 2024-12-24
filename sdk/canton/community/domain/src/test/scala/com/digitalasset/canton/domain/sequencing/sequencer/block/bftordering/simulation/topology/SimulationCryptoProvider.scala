// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
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
import com.digitalasset.canton.crypto.{
  Fingerprint,
  Hash,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SigningPublicKey,
  SyncCryptoError,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

class SimulationCryptoProvider(
    val thisPeer: SequencerId,
    val peerIdsToTopologyData: Map[SequencerId, SimulationTopologyData],
    val crypto: SymbolicCrypto,
    val timestamp: CantonTimestamp,
    val loggerFactory: NamedLoggerFactory,
) extends CryptoProvider[SimulationEnv] {
  private def fetchSigningKey(): Either[SyncCryptoError, Fingerprint] = {
    val keyNotFound = Left(SyncCryptoError.KeyNotAvailable(thisPeer, Signing, timestamp, Seq.empty))

    peerIdsToTopologyData.get(thisPeer) match {
      case Some(topologyData) =>
        Right(topologyData.signingPublicKey.fingerprint)
      case None =>
        keyNotFound
    }
  }

  private def validKeys(member: SequencerId): Map[Fingerprint, SigningPublicKey] =
    peerIdsToTopologyData.get(member) match {
      case Some(topologyData) =>
        Map(topologyData.signingPublicKey.fingerprint -> topologyData.signingPublicKey)
      case None => Map.empty
    }

  override def sign(hash: Hash, usage: NonEmpty[Set[SigningKeyUsage]])(implicit
      traceContext: TraceContext
  ): SimulationFuture[Either[SyncCryptoError, Signature]] = SimulationFuture { () =>
    Try {
      innerSign(hash)
    }
  }

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      hashPurpose: HashPurpose,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): SimulationFuture[Either[SyncCryptoError, SignedMessage[MessageT]]] = SimulationFuture { () =>
    Try {
      innerSign(CryptoProvider.hashForMessage(message, message.from, hashPurpose))
        .map(SignedMessage(message, _))
    }
  }

  private def innerSign(hash: Hash): Either[SyncCryptoError, Signature] =
    for {
      fingerprint <- fetchSigningKey()
      signature =
        crypto.sign(hash, fingerprint, SigningKeyUsage.ProtocolOnly)
    } yield signature

  override def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
      traceContext: TraceContext
  ): SimulationFuture[Either[SignatureCheckError, Unit]] = SimulationFuture { () =>
    Try {
      innerVerifySignature(hash, validKeys(member), signature, member.toString)
    }
  }

  private def innerVerifySignature(
      hash: Hash,
      validKeys: Map[Fingerprint, SigningPublicKey],
      signature: Signature,
      signerStr: => String,
  ): Either[SignatureCheckError, Unit] =
    validKeys.get(signature.signedBy) match {
      case Some(key) =>
        crypto.pureCrypto.verifySignature(hash, key, signature)
      case None =>
        val error =
          if (validKeys.isEmpty)
            SignerHasNoValidKeys(
              s"There are no valid keys for $signerStr but received message signed with ${signature.signedBy}"
            )
          else
            SignatureWithWrongKey(
              s"Key ${signature.signedBy} used to generate signature is not a valid key for $signerStr. Valid keys are ${validKeys.values
                  .map(_.fingerprint.unwrap)}"
            )
        Left(error)
    }
}

object SimulationCryptoProvider {
  def create(
      thisPeer: SequencerId,
      sequencerToTopologyData: Map[SequencerId, SimulationTopologyData],
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
      .get(thisPeer)
      .foreach { topologyData =>
        Await.result(
          cryptoPrivateStore.storePrivateKey(topologyData.signingPrivateKey, None).value.unwrap,
          10.seconds,
        )
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

    new SimulationCryptoProvider(
      thisPeer,
      sequencerToTopologyData,
      crypto,
      timestamp,
      loggerFactory,
    )
  }
}
