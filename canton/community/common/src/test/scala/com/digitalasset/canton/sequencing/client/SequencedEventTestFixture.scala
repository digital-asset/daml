// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.messages.{EnvelopeContent, InformeeMessage}
import com.digitalasset.canton.sequencing.SequencerAggregator.MessageAggregationConfig
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  SequencerAggregator,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities.namespace
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertions.fail
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{ExecutionContext, Future}

class SequencedEventTestFixture(
    loggerFactory: NamedLoggerFactory,
    testedProtocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit private val traceContext: TraceContext, executionContext: ExecutionContext)
    extends AutoCloseable {
  import ScalaFutures.*
  def fixtureTraceContext: TraceContext = traceContext

  private lazy val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()(
    executionContext
  )

  lazy val defaultDomainId: DomainId = DefaultTestIdentities.domainId
  lazy val subscriberId: ParticipantId = ParticipantId("participant1-id")
  lazy val sequencerAlice: SequencerId = DefaultTestIdentities.sequencerIdX
  lazy val subscriberCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactoryX(loggerFactory).forOwnerAndDomain(subscriberId, defaultDomainId)
  private lazy val sequencerCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactoryX(loggerFactory).forOwnerAndDomain(sequencerAlice, defaultDomainId)
  lazy val updatedCounter: Long = 42L
  val sequencerBob: SequencerId = SequencerId(
    UniqueIdentifier(Identifier.tryCreate("da2"), namespace)
  )
  val sequencerCarlos: SequencerId = SequencerId(
    UniqueIdentifier(Identifier.tryCreate("da3"), namespace)
  )
  implicit val actorSystem: ActorSystem = ActorSystem(
    classOf[SequencedEventTestFixture].getSimpleName
  )
  implicit val materializer: Materializer = Materializer(actorSystem)

  private val alice = ParticipantId(UniqueIdentifier.tryCreate("participant", "alice"))
  private val bob = ParticipantId(UniqueIdentifier.tryCreate("participant", "bob"))
  private val carlos = ParticipantId(UniqueIdentifier.tryCreate("participant", "carlos"))
  private val signatureAlice = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureAlice1"),
    alice.uid.namespace.fingerprint,
  )
  private val signatureBob = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureBob1"),
    bob.uid.namespace.fingerprint,
  )
  private val signatureCarlos = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureCarlos1"),
    carlos.uid.namespace.fingerprint,
  )
  lazy val aliceEvents: Seq[OrdinarySerializedEvent] = (1 to 5).map(s =>
    createEvent(
      timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
      counter = updatedCounter + s.toLong,
      signatureOverride = Some(signatureAlice),
    ).futureValue
  )
  lazy val bobEvents: Seq[OrdinarySerializedEvent] = (1 to 5).map(s =>
    createEvent(
      timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
      counter = updatedCounter + s.toLong,
      signatureOverride = Some(signatureBob),
    ).futureValue
  )
  lazy val carlosEvents: Seq[OrdinarySerializedEvent] = (1 to 5).map(s =>
    createEvent(
      timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
      counter = updatedCounter + s.toLong,
      signatureOverride = Some(signatureCarlos),
    ).futureValue
  )

  def mkAggregator(
      config: MessageAggregationConfig = MessageAggregationConfig(
        NonEmptyUtil.fromUnsafe(Set(sequencerAlice)),
        PositiveInt.tryCreate(1),
      )
  ) =
    new SequencerAggregator(
      cryptoPureApi = subscriberCryptoApi.pureCrypto,
      eventInboxSize = PositiveInt.tryCreate(2),
      loggerFactory = loggerFactory,
      initialConfig = config,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
    )

  def config(
      expectedSequencers: Set[SequencerId] = Set(sequencerAlice),
      sequencerTrustThreshold: Int = 1,
  ): MessageAggregationConfig =
    MessageAggregationConfig(
      NonEmptyUtil.fromUnsafe(expectedSequencers),
      PositiveInt.tryCreate(sequencerTrustThreshold),
    )

  def mkValidator(
      syncCryptoApi: DomainSyncCryptoClient = subscriberCryptoApi
  )(implicit executionContext: ExecutionContext): SequencedEventValidatorImpl = {
    new SequencedEventValidatorImpl(
      unauthenticated = false,
      optimistic = false,
      defaultDomainId,
      testedProtocolVersion,
      syncCryptoApi,
      loggerFactory,
      timeouts,
    )(executionContext)
  }

  def createEvent(
      domainId: DomainId = defaultDomainId,
      signatureOverride: Option[Signature] = None,
      serializedOverride: Option[ByteString] = None,
      counter: Long = updatedCounter,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  ): Future[OrdinarySerializedEvent] = {
    import cats.syntax.option.*
    val message = {
      val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
      InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
    }
    val deliver: Deliver[ClosedEnvelope] = Deliver.create[ClosedEnvelope](
      SequencerCounter(counter),
      timestamp,
      domainId,
      MessageId.tryCreate("test").some,
      Batch(
        List(
          ClosedEnvelope.create(
            serializedOverride.getOrElse(
              EnvelopeContent.tryCreate(message, testedProtocolVersion).toByteString
            ),
            Recipients.cc(subscriberId),
            Seq.empty,
            testedProtocolVersion,
          )
        ),
        testedProtocolVersion,
      ),
      testedProtocolVersion,
    )

    for {
      sig <- signatureOverride
        .map(Future.successful)
        .getOrElse(sign(deliver.getCryptographicEvidence, deliver.timestamp))
    } yield OrdinarySequencedEvent(
      SignedContent(deliver, sig, timestampOfSigningKey, testedProtocolVersion),
      None,
    )(
      traceContext
    )
  }

  def createEventWithCounterAndTs(
      counter: Long,
      timestamp: CantonTimestamp,
      customSerialization: Option[ByteString] = None,
      messageIdO: Option[MessageId] = None,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  )(implicit executionContext: ExecutionContext): Future[OrdinarySerializedEvent] = {
    val event =
      SequencerTestUtils.mockDeliverClosedEnvelope(
        counter = counter,
        timestamp = timestamp,
        deserializedFrom = customSerialization,
        messageId = messageIdO,
      )
    for {
      signature <- sign(
        customSerialization.getOrElse(event.getCryptographicEvidence),
        event.timestamp,
      )
    } yield OrdinarySequencedEvent(
      SignedContent(event, signature, timestampOfSigningKey, testedProtocolVersion),
      None,
    )(traceContext)
  }

  def ts(offset: Int) = CantonTimestamp.Epoch.plusSeconds(offset.toLong)

  def sign(bytes: ByteString, timestamp: CantonTimestamp)(implicit
      executionContext: ExecutionContext
  ): Future[Signature] =
    for {
      cryptoApi <- sequencerCryptoApi.snapshot(timestamp)
      signature <- cryptoApi
        .sign(hash(bytes))
        .value
        .map(_.valueOr(err => fail(s"Failed to sign: $err")))(executionContext)
    } yield signature

  def hash(bytes: ByteString): Hash =
    sequencerCryptoApi.pureCrypto.digest(HashPurpose.SequencedEventSignature, bytes)

  override def close(): Unit = {
    actorSystem.terminate().futureValue(PatienceConfiguration.Timeout(Span(3, Seconds)))
    materializer.shutdown()
  }
}
