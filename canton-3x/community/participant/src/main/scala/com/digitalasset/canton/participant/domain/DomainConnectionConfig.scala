// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v0
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{DomainAlias, SequencerAlias}
import com.google.protobuf.ByteString

import java.net.URI

/** The domain connection configuration object
  *
  * @param domain alias to be used internally to refer to this domain connection
  * @param sequencerConnections Configuration for the sequencers. In case of BFT domain - there could be sequencers with multiple connections.
  *                             Each sequencer can also support high availability, so multiple endpoints could be provided for each individual sequencer.
  * @param manualConnect if set to true (default false), the domain is not connected automatically on startup.
  * @param domainId if the domain-id is known, then it can be passed as an argument. during the handshake, the
  *                 participant will check that the domain-id on the remote port is indeed the one given
  *                 in the configuration. the domain-id can not be faked by a domain. therefore, this additional
  *                 check can be used to really ensure that you are talking to the right domain.
  * @param priority the priority of this domain connection. if there are more than one domain connections,
  *                 the [[com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter]]
  *                 will pick the domain connection with the highest priority if possible.
  * @param initialRetryDelay domain connections are "resilient". i.e. if a connection is lost, the system will keep
  *                          trying to reconnect to a domain.
  * @param maxRetryDelay control the backoff parameter such that the retry interval does not grow above this value
  * @param timeTracker the domain time tracker settings. don't change it unless you know what you are doing.
  */
final case class DomainConnectionConfig(
    domain: DomainAlias,
    sequencerConnections: SequencerConnections,
    manualConnect: Boolean = false,
    domainId: Option[DomainId] = None,
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
  ): DomainConnectionConfig =
    addEndpoints(sequencerAlias, new URI(connection), additionalConnections.map(new URI(_)): _*)

  def addEndpoints(
      sequencerAlias: SequencerAlias,
      connection: URI,
      additionalConnections: URI*
  ): DomainConnectionConfig =
    copy(sequencerConnections =
      sequencerConnections.addEndpoints(sequencerAlias, connection, additionalConnections: _*)
    )
  def addConnection(connection: SequencerConnection): DomainConnectionConfig = {
    copy(sequencerConnections =
      sequencerConnections.addEndpoints(connection.sequencerAlias, connection)
    )
  }

  def withCertificates(
      sequencerAlias: SequencerAlias,
      certificates: ByteString,
  ): DomainConnectionConfig =
    copy(sequencerConnections = sequencerConnections.withCertificates(sequencerAlias, certificates))

  override def pretty: Pretty[DomainConnectionConfig] =
    prettyOfClass(
      param("domain", _.domain),
      param("sequencerConnections", _.sequencerConnections),
      param("manualConnect", _.manualConnect),
      paramIfDefined("domainId", _.domainId),
      paramIfDefined("priority", x => Option.when(x.priority != 0)(x.priority)),
      paramIfDefined("initialRetryDelay", _.initialRetryDelay),
      paramIfDefined("maxRetryDelay", _.maxRetryDelay),
      paramIfNotDefault("timeTracker", _.timeTracker, DomainTimeTrackerConfig()),
      paramIfNotDefault("initializeFromTrustedDomain", _.initializeFromTrustedDomain, false),
    )

  def toProtoV0: v0.DomainConnectionConfig =
    v0.DomainConnectionConfig(
      domainAlias = domain.unwrap,
      sequencerConnections = sequencerConnections.toProtoV0,
      manualConnect = manualConnect,
      domainId = domainId.fold("")(_.toProtoPrimitive),
      priority = priority,
      initialRetryDelay = initialRetryDelay.map(_.toProtoPrimitive),
      maxRetryDelay = maxRetryDelay.map(_.toProtoPrimitive),
      timeTracker = timeTracker.toProtoV0.some,
      sequencerTrustThreshold = sequencerConnections.sequencerTrustThreshold.unwrap,
      initializeFromTrustedDomain = initializeFromTrustedDomain,
    )
}

object DomainConnectionConfig
    extends HasVersionedMessageCompanion[DomainConnectionConfig]
    with HasVersionedMessageCompanionDbHelpers[DomainConnectionConfig] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.DomainConnectionConfig)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )
  override def name: String = "domain connection config"

  def grpc(
      sequencerAlias: SequencerAlias,
      domainAlias: DomainAlias,
      connection: String,
      manualConnect: Boolean = false,
      domainId: Option[DomainId] = None,
      certificates: Option[ByteString] = None,
      priority: Int = 0,
      initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
      maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
      timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
      initializeFromTrustedDomain: Boolean = false,
  ): DomainConnectionConfig =
    DomainConnectionConfig(
      domainAlias,
      SequencerConnections.single(
        GrpcSequencerConnection.tryCreate(connection, certificates, sequencerAlias)
      ),
      manualConnect,
      domainId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedDomain,
    )

  def fromProtoV0(
      domainConnectionConfigP: v0.DomainConnectionConfig
  ): ParsingResult[DomainConnectionConfig] = {
    val v0.DomainConnectionConfig(
      domainAlias,
      sequencerConnectionP,
      manualConnect,
      domainId,
      priority,
      initialRetryDelayP,
      maxRetryDelayP,
      timeTrackerP,
      sequencerTrustThreshold,
      initializeFromTrustedDomain,
    ) =
      domainConnectionConfigP
    for {
      alias <- DomainAlias
        .create(domainAlias)
        .leftMap(err => InvariantViolation(s"DomainConnectionConfig.DomainAlias: $err"))
      sequencerConnections <- SequencerConnections.fromProtoV0(
        sequencerConnectionP,
        sequencerTrustThreshold,
      )
      domainId <- OptionUtil
        .emptyStringAsNone(domainId)
        .traverse(DomainId.fromProtoPrimitive(_, "domain_id"))
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
      domainId,
      priority,
      initialRetryDelay,
      maxRetryDelay,
      timeTracker,
      initializeFromTrustedDomain,
    )
  }
}
