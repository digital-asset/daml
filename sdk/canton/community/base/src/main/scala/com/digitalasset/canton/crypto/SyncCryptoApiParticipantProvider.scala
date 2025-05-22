// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.SyncCryptoError.{KeyNotAvailable, SyncCryptoEncryptionError}
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner
import com.digitalasset.canton.crypto.verifier.SyncCryptoVerifier
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.{
  IdentityProvidingServiceClient,
  SynchronizerTopologyClient,
  TopologyClientApi,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** Crypto API Provider class
  *
  * The utility class combines the information provided by the IPSclient, the pure crypto functions
  * and the signing and decryption operations on a private key vault in order to automatically
  * resolve the right keys to use for signing / decryption based on synchronizer and timestamp. This
  * API is intended only for participants and covers all usages of protocol signing keys, thus,
  * session keys will be used if they are enabled.
  *
  * TODO(#23810): Reuse SyncCryptoApiParticipantProvider for all nodes and not only participants
  */
class SyncCryptoApiParticipantProvider(
    val member: Member,
    val ips: IdentityProvidingServiceClient,
    val crypto: Crypto,
    sessionSigningKeysConfig: SessionSigningKeysConfig,
    verificationParallelismLimit: PositiveInt,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {

  require(ips != null)

  def pureCrypto: CryptoPureApi = crypto.pureCrypto

  private val synchronizerCryptoClientCache: TrieMap[SynchronizerId, SynchronizerCryptoClient] =
    TrieMap.empty

  private def createSynchronizerCryptoClient(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerTopologyClient: SynchronizerTopologyClient,
  ) =
    SynchronizerCryptoClient.createWithOptionalSessionKeys(
      member,
      synchronizerId,
      synchronizerTopologyClient,
      staticSynchronizerParameters,
      SynchronizerCrypto(crypto, staticSynchronizerParameters),
      sessionSigningKeysConfig,
      verificationParallelismLimit,
      timeouts,
      futureSupervisor,
      loggerFactory.append("synchronizerId", synchronizerId.toString),
      synchronizerCryptoClientCache,
    )

  private def createOrUpdateCache(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerTopologyClient: SynchronizerTopologyClient,
  ) =
    synchronizerCryptoClientCache.getOrElseUpdate(
      synchronizerId,
      createSynchronizerCryptoClient(
        synchronizerId,
        staticSynchronizerParameters,
        synchronizerTopologyClient,
      ),
    )

  def tryForSynchronizer(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): SynchronizerCryptoClient =
    createOrUpdateCache(
      synchronizerId,
      staticSynchronizerParameters,
      ips.tryForSynchronizer(synchronizerId),
    )

  def forSynchronizer(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): Option[SynchronizerCryptoClient] =
    ips.forSynchronizer(synchronizerId).map { domainTopologyClient =>
      createOrUpdateCache(
        synchronizerId,
        staticSynchronizerParameters,
        domainTopologyClient,
      )
    }

}

trait SyncCryptoClient[+T <: SyncCryptoApi] extends TopologyClientApi[T] {
  this: HasFutureSupervision =>

  val pureCrypto: SynchronizerCryptoPureApi

  /** Returns a snapshot of the current member topology for the given synchronizer. The future will
    * log a warning and await the snapshot if the data is not there yet.
    *
    * The snapshot returned by this method should be used for validating transaction and transfer
    * requests (Phase 2 - 7). Use the request timestamp as parameter for this method. Do not use a
    * response or result timestamp, because all validation steps must use the same topology
    * snapshot.
    */
  def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  protected def awaitIpsSnapshotInternal(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  def awaitIpsSnapshot(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshot] =
    supervisedUS(description, warnAfter)(awaitIpsSnapshotInternal(timestamp))

}

object SyncCryptoClient {

  /** Computes the snapshot for the desired timestamp, assuming that the last (relevant) update to
    * the topology state happened at or before `previousTimestamp`. If `previousTimestampO` is
    * [[scala.None$]] and `desiredTimestamp` is currently not known
    * [[com.digitalasset.canton.topology.client.TopologyClientApi.topologyKnownUntilTimestamp]],
    * then the current approximation is returned and if `warnIfApproximate` is set a warning is
    * logged.
    */
  def getSnapshotForTimestamp(
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[SyncCryptoApi] =
    getSnapshotForTimestampInternal[FutureUnlessShutdown](
      client,
      desiredTimestamp,
      previousTimestampO,
      warnIfApproximate,
    )(
      (timestamp, traceContext) => client.snapshot(timestamp)(traceContext),
      (description, timestamp, traceContext) =>
        client.awaitSnapshotUSSupervised(description)(timestamp)(traceContext),
      (snapshot, traceContext) =>
        snapshot
          .findDynamicSynchronizerParametersOrDefault(
            protocolVersion = protocolVersion,
            warnOnUsingDefault = false,
          )(traceContext),
    )

  // Base version of getSnapshotForTimestamp abstracting over the effect type to allow for
  // a `Future` and `FutureUnlessShutdown` version. Once we migrate all usages to the US version, this abstraction
  // should not be needed anymore
  private def getSnapshotForTimestampInternal[F[_]](
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(
      getSnapshot: (CantonTimestamp, TraceContext) => F[SyncCryptoApi],
      awaitSnapshotSupervised: (String, CantonTimestamp, TraceContext) => F[SyncCryptoApi],
      dynamicSynchronizerParameters: (
          TopologySnapshot,
          TraceContext,
      ) => F[DynamicSynchronizerParameters],
  )(implicit
      loggingContext: ErrorLoggingContext,
      monad: Monad[F],
  ): F[SyncCryptoApi] = {
    val traceContext: TraceContext = loggingContext.traceContext

    def lookupDynamicSynchronizerParameters(
        timestamp: CantonTimestamp
    ): F[DynamicSynchronizerParameters] =
      for {
        snapshot <- awaitSnapshotSupervised(
          s"searching for topology change delay at $timestamp for desired timestamp $desiredTimestamp and known until ${client.topologyKnownUntilTimestamp}",
          timestamp,
          loggingContext.traceContext,
        )
        synchronizerParams <- dynamicSynchronizerParameters(
          snapshot.ipsSnapshot,
          loggingContext.traceContext,
        )
      } yield synchronizerParams

    computeTimestampForValidation(
      desiredTimestamp,
      previousTimestampO,
      client.topologyKnownUntilTimestamp,
      client.approximateTimestamp,
      warnIfApproximate,
    )(
      lookupDynamicSynchronizerParameters
    ).flatMap { timestamp =>
      if (timestamp <= client.topologyKnownUntilTimestamp) {
        loggingContext.debug(
          s"Getting topology snapshot at $timestamp; desired=$desiredTimestamp, known until ${client.topologyKnownUntilTimestamp}; previous $previousTimestampO"
        )
        getSnapshot(timestamp, traceContext)
      } else {
        loggingContext.debug(
          s"Waiting for topology snapshot at $timestamp; desired=$desiredTimestamp, known until ${client.topologyKnownUntilTimestamp}; previous $previousTimestampO"
        )
        awaitSnapshotSupervised(
          s"requesting topology snapshot at $timestamp; desired=$desiredTimestamp, previousO=$previousTimestampO, known until=${client.topologyKnownUntilTimestamp}",
          timestamp,
          traceContext,
        )
      }
    }
  }

  private def computeTimestampForValidation[F[_]](
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      topologyKnownUntilTimestamp: CantonTimestamp,
      currentApproximateTimestamp: CantonTimestamp,
      warnIfApproximate: Boolean,
  )(
      synchronizerParamsLookup: CantonTimestamp => F[DynamicSynchronizerParameters]
  )(implicit
      loggingContext: ErrorLoggingContext,
      // executionContext: ExecutionContext,
      monad: Monad[F],
  ): F[CantonTimestamp] =
    if (desiredTimestamp <= topologyKnownUntilTimestamp) {
      monad.pure(desiredTimestamp)
    } else {
      previousTimestampO match {
        case None =>
          LoggerUtil.logAtLevel(
            if (warnIfApproximate) Level.WARN else Level.INFO,
            s"Using approximate topology snapshot at $currentApproximateTimestamp for desired timestamp $desiredTimestamp",
          )
          monad.pure(currentApproximateTimestamp)
        case Some(previousTimestamp) =>
          if (desiredTimestamp <= previousTimestamp.immediateSuccessor)
            monad.pure(desiredTimestamp)
          else {
            import scala.Ordered.orderingToOrdered
            synchronizerParamsLookup(previousTimestamp).map { previousSynchronizerParams =>
              val delay = previousSynchronizerParams.topologyChangeDelay
              val diff = desiredTimestamp - previousTimestamp
              val snapshotTimestamp =
                if (diff > delay.unwrap) {
                  // `desiredTimestamp` is larger than `previousTimestamp` plus the `delay`,
                  // so timestamps cannot overflow here
                  checked(previousTimestamp.plus(delay.unwrap).immediateSuccessor)
                } else desiredTimestamp
              snapshotTimestamp
            }
          }
      }
    }

}

/** Crypto operations on a particular synchronizer
  */
class SynchronizerCryptoClient private (
    val member: Member,
    val physicalSynchronizerId: PhysicalSynchronizerId,
    val synchronizerCryptoClientCache: TrieMap[SynchronizerId, SynchronizerCryptoClient],
    val ips: SynchronizerTopologyClient,
    val crypto: SynchronizerCrypto,
    val syncCryptoSigner: SyncCryptoSigner,
    val syncCryptoVerifier: SyncCryptoVerifier,
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SyncCryptoClient[SynchronizerSnapshotSyncCryptoApi]
    with HasFutureSupervision
    with NamedLogging
    with FlagCloseable {

  val synchronizerId: SynchronizerId = physicalSynchronizerId.logical

  override val pureCrypto: SynchronizerCryptoPureApi = crypto.pureCrypto

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SynchronizerSnapshotSyncCryptoApi] =
    ips.snapshot(timestamp).map(create)

  override def trySnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): SynchronizerSnapshotSyncCryptoApi =
    create(ips.trySnapshot(timestamp))

  override def headSnapshot(implicit
      traceContext: TraceContext
  ): SynchronizerSnapshotSyncCryptoApi =
    create(ips.headSnapshot)

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SynchronizerSnapshotSyncCryptoApi] =
    ips.awaitSnapshot(timestamp).map(create)

  def create(snapshot: TopologySnapshot): SynchronizerSnapshotSyncCryptoApi =
    new SynchronizerSnapshotSyncCryptoApi(
      synchronizerId,
      snapshot,
      crypto,
      syncCryptoSigner,
      syncCryptoVerifier,
      loggerFactory,
    )

  override def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.snapshot(timestamp)

  override protected def awaitIpsSnapshotInternal(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.awaitSnapshot(timestamp)

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    ips.snapshotAvailable(timestamp)

  override def awaitTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    ips.awaitTimestamp(timestamp)

  override def awaitSequencedTimestamp(timestampInclusive: SequencedTime)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] = ips.awaitSequencedTimestamp(timestampInclusive)

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): SynchronizerSnapshotSyncCryptoApi =
    create(ips.currentSnapshotApproximation)

  override def topologyKnownUntilTimestamp: CantonTimestamp = ips.topologyKnownUntilTimestamp

  override def approximateTimestamp: CantonTimestamp = ips.approximateTimestamp

  override def onClosed(): Unit = {
    synchronizerCryptoClientCache.remove(synchronizerId).discard
    LifeCycle.close(ips)(logger)
  }

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    ips.awaitMaxTimestamp(sequencedTime)
}

object SynchronizerCryptoClient {

  def create(
      member: Member,
      synchronizerId: SynchronizerId,
      ips: SynchronizerTopologyClient,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerCrypto: SynchronizerCrypto,
      verificationParallelismLimit: PositiveInt,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): SynchronizerCryptoClient = {
    val syncCryptoSignerWithLongTermKeys = SyncCryptoSigner.createWithLongTermKeys(
      member,
      synchronizerCrypto,
      loggerFactory,
    )
    new SynchronizerCryptoClient(
      member,
      PhysicalSynchronizerId(synchronizerId, staticSynchronizerParameters),
      TrieMap.empty,
      ips,
      synchronizerCrypto,
      syncCryptoSignerWithLongTermKeys,
      SyncCryptoVerifier.create(
        synchronizerId,
        staticSynchronizerParameters,
        synchronizerCrypto.pureCrypto,
        verificationParallelismLimit,
        loggerFactory,
      ),
      timeouts,
      futureSupervisor,
      loggerFactory.append("synchronizerId", synchronizerId.toString),
    )
  }

  /** Generates a new sync crypto that can use session signing keys if they are enabled in Canton's
    * configuration.
    */
  def createWithOptionalSessionKeys(
      member: Member,
      synchronizerId: SynchronizerId,
      ips: SynchronizerTopologyClient,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerCrypto: SynchronizerCrypto,
      sessionSigningKeysConfig: SessionSigningKeysConfig,
      verificationParallelismLimit: PositiveInt,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      synchronizerCryptoClientCache: TrieMap[SynchronizerId, SynchronizerCryptoClient] =
        TrieMap.empty,
  )(implicit
      executionContext: ExecutionContext
  ): SynchronizerCryptoClient = {
    val syncCryptoSignerWithSessionKeys = SyncCryptoSigner.createWithOptionalSessionKeys(
      synchronizerId,
      staticSynchronizerParameters,
      member,
      synchronizerCrypto,
      sessionSigningKeysConfig,
      loggerFactory,
    )
    new SynchronizerCryptoClient(
      member,
      PhysicalSynchronizerId(synchronizerId, staticSynchronizerParameters),
      synchronizerCryptoClientCache,
      ips,
      synchronizerCrypto,
      syncCryptoSignerWithSessionKeys,
      SyncCryptoVerifier.create(
        synchronizerId,
        staticSynchronizerParameters,
        synchronizerCrypto.pureCrypto,
        verificationParallelismLimit,
        loggerFactory,
      ),
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
  }

}

/** crypto operations for a (synchronizer,timestamp) */
class SynchronizerSnapshotSyncCryptoApi(
    val synchronizerId: SynchronizerId,
    override val ipsSnapshot: TopologySnapshot,
    val crypto: SynchronizerCrypto,
    val syncCryptoSigner: SyncCryptoSigner,
    val syncCryptoVerifier: SyncCryptoVerifier,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoApi
    with NamedLogging {

  override val pureCrypto: CryptoPureApi = crypto.pureCrypto

  override def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    syncCryptoSigner.sign(ipsSnapshot, hash, usage)

  override def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    syncCryptoVerifier.verifySignature(ipsSnapshot, hash, signer, signature, usage)

  override def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    syncCryptoVerifier.verifySignatures(ipsSnapshot, hash, signer, signatures, usage)

  override def verifyMediatorSignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      mediatorGroup <- EitherT(
        ipsSnapshot.mediatorGroups().map { groups =>
          groups
            .find(_.index == mediatorGroupIndex)
            .toRight(
              SignatureCheckError.MemberGroupDoesNotExist(
                s"Unknown mediator group with index $mediatorGroupIndex"
              )
            )
        }
      )
      _ <- syncCryptoVerifier.verifyGroupSignatures(
        ipsSnapshot,
        hash,
        mediatorGroup.active,
        mediatorGroup.threshold,
        mediatorGroup.toString,
        signatures,
        usage,
      )
    } yield ()

  override def verifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      sequencerGroup <- EitherT(
        ipsSnapshot
          .sequencerGroup()
          .map(
            _.toRight(
              SignatureCheckError.MemberGroupDoesNotExist(
                "Sequencer group not found"
              )
            )
          )
      )
      _ <- syncCryptoVerifier.verifyGroupSignatures(
        ipsSnapshot,
        hash,
        sequencerGroup.active,
        sequencerGroup.threshold,
        sequencerGroup.toString,
        signatures,
        usage,
      )
    } yield ()

  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncCryptoError, M] =
    crypto.privateCrypto
      .decrypt(encryptedMessage)(deserialize)
      .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoDecryptionError(err))

  /** Encrypts a message for the given members
    *
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective member.
    */
  override def encryptFor[M <: HasToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (MemberType, SyncCryptoError), Map[
    MemberType,
    AsymmetricEncrypted[M],
  ]] = {
    def encryptFor(keys: Map[Member, EncryptionPublicKey])(
        member: MemberType
    ): Either[(MemberType, SyncCryptoError), (MemberType, AsymmetricEncrypted[M])] = keys
      .get(member)
      .toRight(
        member -> KeyNotAvailable(
          member,
          KeyPurpose.Encryption,
          ipsSnapshot.timestamp,
          Seq.empty,
        )
      )
      .flatMap(k =>
        pureCrypto
          .encryptWith(message, k)
          .bimap(error => member -> SyncCryptoEncryptionError(error), member -> _)
      )

    EitherT(
      ipsSnapshot
        .encryptionKey(members)
        .map { keys =>
          members
            .traverse(encryptFor(keys))
            .map(_.toMap)
        }
    )
  }
}
