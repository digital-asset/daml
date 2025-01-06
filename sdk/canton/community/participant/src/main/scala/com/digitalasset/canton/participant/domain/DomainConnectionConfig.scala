// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.DomainTimeTrackerConfig
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString

import java.net.URI

/** The domain connection configuration object
  *
  * @param synchronizer alias to be used internally to refer to this domain connection
  * @param sequencerConnections Configuration for the sequencers. In case of BFT domain - there could be sequencers with multiple connections.
  *                             Each sequencer can also support high availability, so multiple endpoints could be provided for each individual sequencer.
  * @param manualConnect if set to true (default false), the domain is not connected automatically on startup.
  * @param synchronizerId if the synchronizer id is known, then it can be passed as an argument. during the handshake, the
  *                 participant will check that the synchronizer id on the remote port is indeed the one given
  *                 in the configuration. the synchronizer id can not be faked by a domain. therefore, this additional
  *                 check can be used to really ensure that you are talking to the right domain.
  * @param priority the priority of this domain connection. if there are more than one domain connections,
  *                 the [[com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter]]
  *                 will pick the domain connection with the highest priority if possible.
  * @param initialRetryDelay domain connections are "resilient". i.e. if a connection is lost, the system will keep
  *                          trying to reconnect to a domain.
  * @param maxRetryDelay control the backoff parameter such that the retry interval does not grow above this value
  * @param timeTracker the domain time tracker settings. don't change it unless you know what you are doing.
  * @param initializeFromTrustedDomain if false will automatically generate a DomainTrustCertificate when connecting to a new domain.
  */
final case class DomainConnectionConfig(
    synchronizerAlias: SynchronizerAlias,
    sequencerConnections: SequencerConnections,
    manualConnect: Boolean = false,
    synchronizerId: Option[SynchronizerId] = None,
    priority: Int = 0,
    initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
    maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
    timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    initializeFromTrustedDomain: Boolean = false,
) extends HasVersionedWrapper[DomainConnectionConfig]
    with PrettyPrinting {

  override protected def companionObj = DomainConnectionConfig

  /** Helper methods to avoid having to use NonEmpty[Seq in the console */
  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: String,
      additionalConnections: String*
  ): Either[String, DomainConnectionConfig] =
    addEndpoints(sequencerAlias, new URI(connection), additionalConnections.map(new URI(_))*)

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): Either[String, DomainConnectionConfig] = for {
    sequencerConnections <- sequencerConnections.addEndpoints(
      sequencerAlias,
      connection,
      additionalConnections*
    )
  } yield (
    copy(sequencerConnections = sequencerConnections)
  )

  def addConnection(connection: SequencerConnection): Either[String, DomainConnectionConfig] = for {
    sequencerConnections <- sequencerConnections.addEndpoints(connection.sequencerAlias, connection)
  } yield (
    copy(sequencerConnections = sequencerConnections)
  )

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): DomainConnectionConfig =
    copy(sequencerConnections = sequencerConnections.withCertificates(sequencerAlias, certificates))

  override protected def pretty: Pretty[DomainConnectionConfig] =
    prettyOfClass(
      param("domain", _.synchronizerAlias),
      param("sequencerConnections", _.sequencerConnections),
      param("manualConnect", _.manualConnect),
      paramIfDefined("synchronizerId", _.synchronizerId),
      paramIfDefined("priority", x => Option.when(x.priority != 0)(x.priority)),
      paramIfDefined("initialRetryDelay", _.initialRetryDelay),
      paramIfDefined("maxRetryDelay", _.maxRetryDelay),
      paramIfNotDefault("timeTracker", _.timeTracker, DomainTimeTrackerConfig()),
      paramIfNotDefault("initializeFromTrustedDomain", _.initializeFromTrustedDomain, false),
    )

  def toProtoV30: v30.DomainConnectionConfig =
    v30.DomainConnectionConfig(
      synchronizerAlias = synchronizerAlias.unwrap,
      sequencerConnections = sequencerConnections.toProtoV30.some,
      manualConnect = manualConnect,
      synchronizerId = synchronizerId.fold("")(_.toProtoPrimitive),
      priority = priority,
      initialRetryDelay = initialRetryDelay.map(_.toProtoPrimitive),
      maxRetryDelay = maxRetryDelay.map(_.toProtoPrimitive),
      timeTracker = timeTracker.toProtoV30.some,
      initializeFromTrustedDomain = initializeFromTrustedDomain,
    )
}

object DomainConnectionConfig
    extends HasVersionedMessageCompanion[DomainConnectionConfig]
    with HasVersionedMessageCompanionDbHelpers[DomainConnectionConfig] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(v30.DomainConnectionConfig)(fromProtoV30),
      _.toProtoV30,
    )
  )
  override def name: String = "domain connection config"

  def grpc(
      sequencerAlias: SequencerAlias,
      synchronizerAlias: SynchronizerAlias,
      connection: String,
      manualConnect: Boolean = false,
      synchronizerId: Option[SynchronizerId] = None,
      certificates: Option[ByteString] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
      initializeFromTrustedDomain: Boolean = false,
  ): DomainConnectionConfig =
    DomainConnectionConfig(
      synchronizerAlias,
      SequencerConnections.single(
        GrpcSequencerConnection.tryCreate(connection, certificates, sequencerAlias)
      ),
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedDomain,
    )

  def fromProtoV30(
      domainConnectionConfigP: v30.DomainConnectionConfig
  ): ParsingResult[DomainConnectionConfig] = {
    val v30.DomainConnectionConfig(
      synchronizerAlias,
      sequencerConnectionsPO,
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelayP,
      maxRetryDelayP,
      timeTrackerP,
      initializeFromTrustedDomain,
    ) =
      domainConnectionConfigP
    for {
      alias <- SynchronizerAlias
        .create(synchronizerAlias)
        .leftMap(err => InvariantViolation(s"DomainConnectionConfig.synchronizer_alias", err))
      sequencerConnections <- ProtoConverter
        .required("sequencerConnections", sequencerConnectionsPO)
        .flatMap(SequencerConnections.fromProtoV30)
      synchronizerId <- OptionUtil
        .emptyStringAsNone(synchronizerId)
        .traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      initialRetryDelay <- initialRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay")
      )
      maxRetryDelay <- maxRetryDelayP.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("maxRetryDelay")
      )
      timeTracker <- ProtoConverter.parseRequired(
        DomainTimeTrackerConfig.fromProto,
        "timeTracker",
        timeTrackerP,
      )
    } yield DomainConnectionConfig(
      alias,
      sequencerConnections,
      manualConnect,
      synchronizerId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedDomain,
    )
  }
}
