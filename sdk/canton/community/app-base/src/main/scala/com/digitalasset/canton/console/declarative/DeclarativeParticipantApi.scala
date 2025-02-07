// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.SynchronizerConnectivity.ListConnectedSynchronizers
import com.digitalasset.canton.admin.api.client.commands.{
  GrpcAdminCommand,
  LedgerApiCommands,
  ParticipantAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.LedgerApiUser
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.ClientConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.GrpcAdminCommandRunner
import com.digitalasset.canton.console.declarative.DeclarativeApi.UpdateResult
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, LifeCycle, RunOnShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{
  Namespace,
  ParticipantId,
  PartyId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias, config}
import com.digitalasset.daml.lf.archive.DarParser
import com.google.protobuf.ByteString
import pureconfig.error.CannotConvert

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext

/** Declarative participant config
  *
  * @param checkSelfConsistency if set to true (default), then after every sync operation we'll check again if it really succeeded
  * @param fetchedDarDirectory temporary directory to store the dars to
  * @param dars which dars should be uploaded
  * @param parties which parties should be allocated
  * @param removeParties if true, then any "excess party" found on the node will be deleted
  * @param users which users should be allocated
  * @param removeUsers if true, then any "excess user" found on the node will be deleted
  * @param connections which connections should be configured
  * @param removeConnections if true, then any excess connection will be disabled
  */
final case class DeclarativeParticipantConfig(
    checkSelfConsistency: Boolean = true,
    fetchedDarDirectory: File = new File("fetched-dars"),
    dars: Seq[DeclarativeDarConfig] = Seq(),
    parties: Seq[DeclarativePartyConfig] = Seq(),
    removeParties: Boolean = false,
    users: Seq[DeclarativeUserConfig] = Seq(),
    removeUsers: Boolean = false,
    connections: NonEmpty[Seq[DeclarativeConnectionConfig]],
    removeConnections: Boolean = false,
)

/** Declarative dar definition
  *
  * @param path the path (or URL) to the dar or dar directory
  * @param requestHeaders optionally add additional request headers to download the dar
  * @param expectedMainPackage which package id should be expected as the main package
  */
final case class DeclarativeDarConfig(
    path: String,
    requestHeaders: Map[String, String] = Map(),
    expectedMainPackage: Option[String] = None,
)

/** Declarative party definition
  *
  * @param id the prefix of the party
  * @param namespace the namespace (defaults to the participants namespace)
  * @param synchronizers if not empty, the party will be added to the selected synchronizers only, refered to by alias
  * @param permission the permission of the hosting participant
  */
final case class DeclarativePartyConfig(
    id: String,
    namespace: Option[Namespace],
    synchronizers: Seq[String] = Seq.empty,
    permission: ParticipantPermission = ParticipantPermission.Submission,
)

/** Declarative user rights definition
  *
  * @param actAs the name of the parties the user can act as. parties must exist. if they don't contain a namespace, then
  *              the participants namespace will be used
  * @param readAs the name of the parties the user can read as.
  * @param readAsAnyParty if true then the user can read as any party
  * @param participantAdmin if true then the user can act as a participant admin
  * @param identityProviderAdmin if true, then the user can act as an identity provider admin
  */
final case class DeclarativeUserRightsConfig(
    actAs: Set[String] = Set(),
    readAs: Set[String] = Set(),
    readAsAnyParty: Boolean = false,
    participantAdmin: Boolean = false,
    identityProviderAdmin: Boolean = false,
)

/** Declaratively control users
  *
  * @param id the user id
  * @param primaryParty the primary party that should be used for the user
  * @param isDeactivated if true then the user is deactivatedd
  * @param annotations a property bag of annotations that can be stored alongside the user
  * @param identityProviderId the idp of the given user
  * @param rights the rights granted to the party
  */
final case class DeclarativeUserConfig(
    id: String,
    primaryParty: Option[String] = None,
    isDeactivated: Boolean = false,
    annotations: Map[String, String] = Map.empty,
    identityProviderId: String = "",
    rights: DeclarativeUserRightsConfig = DeclarativeUserRightsConfig(),
)(val resourceVersion: String = "") {

  def mapPartiesToNamespace(namespace: Namespace): DeclarativeUserConfig = {
    def mapParty(party: String): String =
      if (party.contains(UniqueIdentifier.delimiter)) party
      else
        UniqueIdentifier.tryCreate(party, namespace).toProtoPrimitive
    copy(
      primaryParty = primaryParty.map(mapParty),
      rights = rights.copy(actAs = rights.actAs.map(mapParty), readAs = rights.readAs.map(mapParty)),
    )(resourceVersion)
  }

  def needsUserChange(other: DeclarativeUserConfig): Boolean =
    primaryParty != other.primaryParty || isDeactivated != other.isDeactivated || annotations != other.annotations

}

/** Declaratively define sequencer endpoints
  *
  * @param endpoints the list of endpoints for the given sequencer. all endpoints must be of the same sequencer (same-id)
  * @param transportSecurity if true then TLS will be used
  * @param customTrustCertificates if the TLS certificate used cannot be validated against the JVMs trust store, then a
  *                                trust store can be provided
  */
final case class DeclarativeSequencerConnectionConfig(
    endpoints: NonEmpty[Seq[Endpoint]],
    transportSecurity: Boolean = false,
    customTrustCertificates: Option[File] = None,
)(customTrustCertificatesFromNode: Option[ByteString] = None) {
  def customTrustCertificatesAsByteString: Either[String, Option[ByteString]] =
    customTrustCertificates
      .traverse(x => BinaryFileUtil.readByteStringFromFile(x.getPath))
      .map(_.orElse(customTrustCertificatesFromNode))

  def isEquivalent(other: DeclarativeSequencerConnectionConfig): Boolean =
    endpoints == other.endpoints && transportSecurity == other.transportSecurity && customTrustCertificatesAsByteString == other.customTrustCertificatesAsByteString

}

/** Declarative synchronizer connection configuration
  *
  * @param synchronizerAlias the alias to refer to this connection
  * @param connections the list of sequencers with endpoints
  * @param manualConnect if true then the connection should be manual and require explicitly operator action
  * @param priority sets the priority of the connection. if a transaction can be sent to several synchronizers, it will
  *                 use the one with the highest priority
  * @param initializeFromTrustedSynchronizer if true then the participant assumes that the synchronizer trust certificate
  *                                          of the participant is already issued
  * @param trustThreshold from how many sequencers does the node have to receive a notification to trust that it was really
  *                       observed
  */
final case class DeclarativeConnectionConfig(
    synchronizerAlias: String,
    connections: NonEmpty[Map[String, DeclarativeSequencerConnectionConfig]],
    manualConnect: Boolean = false,
    priority: Int = 0,
    initializeFromTrustedSynchronizer: Boolean = false,
    trustThreshold: PositiveInt = PositiveInt.one,
) {

  def isEquivalent(other: DeclarativeConnectionConfig): Boolean =
    manualConnect == other.manualConnect &&
      priority == other.priority &&
      initializeFromTrustedSynchronizer == other.initializeFromTrustedSynchronizer &&
      trustThreshold == other.trustThreshold &&
      connections.keySet == other.connections.keySet &&
      connections.forall { case (name, conn) =>
        other.connections.get(name).exists(_.isEquivalent(conn))
      }

  def toSynchronizerConnectionConfig: SynchronizerConnectionConfig =
    SynchronizerConnectionConfig(
      synchronizerAlias = SynchronizerAlias.tryCreate(synchronizerAlias),
      sequencerConnections = SequencerConnections
        .many(
          connections = connections.map { case (alias, conn) =>
            GrpcSequencerConnection(
              endpoints = conn.endpoints,
              transportSecurity = conn.transportSecurity,
              sequencerAlias = SequencerAlias.tryCreate(alias),
              customTrustCertificates = conn.customTrustCertificatesAsByteString.toOption.flatten,
            )
          }.toSeq,
          sequencerTrustThreshold = trustThreshold,
          submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
        )
        .getOrElse(throw new IllegalArgumentException("Cannot create sequencer connections")),
      manualConnect = manualConnect,
      priority = priority,
      initializeFromTrustedSynchronizer = initializeFromTrustedSynchronizer,
    )

}

class DeclarativeParticipantApi(
    val name: String,
    ledgerApiConfig: ClientConfig,
    adminApiConfig: ClientConfig,
    override val consistencyTimeout: config.NonNegativeDuration,
    adminToken: => Option[CantonAdminToken],
    runnerFactory: String => GrpcAdminCommandRunner,
    val closeContext: CloseContext,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DeclarativeApi[DeclarativeParticipantConfig, ParticipantId] {

  private val adminApiRunner = runnerFactory(CantonGrpcUtil.ApiName.AdminApi)
  private val ledgerApiRunner = runnerFactory(CantonGrpcUtil.ApiName.LedgerApi)
  closeContext.context
    .runOnShutdown(new RunOnShutdown {
      override def name: String = "stop-declarative-api"
      override def done: Boolean = false
      override def run(): Unit =
        LifeCycle.close(adminApiRunner, ledgerApiRunner)(logger)
    })(TraceContext.empty)
    .discard

  override protected def activeAdminToken: Option[CantonAdminToken] = adminToken

  private def queryApi[Result](
      runner: GrpcAdminCommandRunner,
      cfg: ClientConfig,
      command: GrpcAdminCommand[_, _, Result],
  )(implicit traceContext: TraceContext): Either[String, Result] = if (
    closeContext.context.isClosing
  )
    Left("Node is shutting down")
  else
    activeAdminToken.fold(Left("Node instance is passive"): Either[String, Result])(token =>
      runner.runCommandWithExistingTrace(name, command, cfg, Some(token.secret)).toEither
    )

  private def queryAdminApi[Result](
      command: GrpcAdminCommand[_, _, Result]
  )(implicit traceContext: TraceContext): Either[String, Result] =
    queryApi(adminApiRunner, adminApiConfig, command)

  private def queryLedgerApi[Result](
      command: GrpcAdminCommand[_, _, Result]
  )(implicit traceContext: TraceContext): Either[String, Result] =
    queryApi(ledgerApiRunner, ledgerApiConfig, command)

  override protected def readConfig(
      file: File
  )(implicit traceContext: TraceContext): Either[String, DeclarativeParticipantConfig] =
    DeclarativeParticipantApi.readConfig(file)

  override protected def prepare(config: DeclarativeParticipantConfig)(implicit
      traceContext: TraceContext
  ): Either[String, ParticipantId] =
    for {
      _ <- createDarDirectoryIfNecessary(config.fetchedDarDirectory, config.dars)
      uid <- queryAdminApi(TopologyAdminCommands.Init.GetId())
    } yield ParticipantId(uid)

  override protected def sync(config: DeclarativeParticipantConfig, context: ParticipantId)(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] =
    for {
      connections <- syncConnections(
        config.connections,
        config.removeConnections,
        config.checkSelfConsistency,
      )
      parties <- syncParties(
        context,
        config.parties,
        config.removeParties,
        config.checkSelfConsistency,
      )
      users <- syncUsers(context, config.users, config.removeUsers, config.checkSelfConsistency)
      dars <- syncDars(
        config.dars,
        config.checkSelfConsistency,
        config.fetchedDarDirectory,
      )
    } yield Seq(connections, parties, users, dars).foldLeft(UpdateResult())(_.merge(_))

  private def createDarDirectoryIfNecessary(
      fetchedDarDirectory: File,
      dars: Seq[DeclarativeDarConfig],
  ): Either[String, Unit] =
    if (dars.exists(_.path.startsWith("http"))) {
      Either.cond(
        (fetchedDarDirectory.isDirectory && fetchedDarDirectory.canWrite) || fetchedDarDirectory
          .mkdirs(),
        (),
        s"Unable to create directory for fetched dars: $fetchedDarDirectory",
      )
    } else Right(())

  private def syncParties(
      participantId: ParticipantId,
      parties: Seq[DeclarativePartyConfig],
      removeParties: Boolean,
      checkSelfConsistency: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] = {

    val nodeNamespace = participantId.namespace

    def baseQuery(synchronizerId: SynchronizerId): BaseQuery =
      BaseQuery(
        filterStore = SynchronizerStore(synchronizerId).filterName,
        proposals = false,
        timeQuery = TimeQuery.HeadState,
        ops = TopologyChangeOp.Replace.some,
        filterSigningKey = "",
        protocolVersion = None,
      )

    def fetchHosted(filterParty: String, synchronizerId: SynchronizerId) = queryAdminApi(
      TopologyAdminCommands.Read.ListPartyToParticipant(
        baseQuery(synchronizerId),
        filterParty = filterParty,
        filterParticipant = participantId.filterString,
      )
    )

    def createTopologyTx(
        uid: UniqueIdentifier,
        synchronizerId: SynchronizerId,
        permission: ParticipantPermission,
    ) =
      for {
        mapping <- PartyToParticipant.create(
          PartyId(uid),
          threshold = PositiveInt.one,
          Seq(
            HostingParticipant(participantId, permission)
          ),
        )
        _ <- queryAdminApi(
          TopologyAdminCommands.Write.Propose(
            mapping,
            signedBy = Seq.empty,
            store = SynchronizerStore(synchronizerId).filterName,
            mustFullyAuthorize = true,
            waitToBecomeEffective = Some(consistencyTimeout),
          )
        ).map(_ => ())
      } yield ()

    def removeParty(uid: UniqueIdentifier, synchronizerId: SynchronizerId): Either[String, Unit] =
      for {
        current <- fetchHosted(filterParty = uid.toProtoPrimitive, synchronizerId)
          .flatMap(_.headOption.toRight(s"Party not found for removal?: $uid"))
        _ <- queryAdminApi(
          TopologyAdminCommands.Write.Propose(
            current.item,
            signedBy = Seq.empty,
            store = SynchronizerStore(synchronizerId).filterName,
            mustFullyAuthorize = true,
            change = TopologyChangeOp.Remove,
            waitToBecomeEffective = Some(consistencyTimeout),
          )
        )
      } yield {}

    def awaitLedgerApiServer(
        parties: Seq[(UniqueIdentifier, SynchronizerId)]
    ): Either[String, Boolean] =
      for {
        observed <- queryLedgerApi(
          LedgerApiCommands.PartyManagementService.ListKnownParties(identityProviderId = "")
        )
        observedUids <- observed
          .traverse(details => UniqueIdentifier.fromProtoPrimitive(details.party, "party"))
          .leftMap(_.toString)
      } yield {
        val observedSet = observedUids.toSet
        parties.map(_._1).toSet.subsetOf(observedSet)
      }

    queryAdminApi(ListConnectedSynchronizers())
      .flatMap { found =>
        Either.cond(found.nonEmpty, found, "No connected synchronizer found. Cannot sync parties")
      }
      .flatMap { synchronizerIds =>
        val wanted = parties.flatMap { p =>
          val party = UniqueIdentifier.tryCreate(p.id, p.namespace.getOrElse(nodeNamespace))
          synchronizerIds
            .filter(s =>
              p.synchronizers.isEmpty || p.synchronizers.contains(s.synchronizerAlias.unwrap)
            )
            .map(s => ((party, s.synchronizerId), p.permission))
        }

        def fetchAll() =
          // fold synchronizers and found parties and find the ones that are allocated to our node
          synchronizerIds.map(_.synchronizerId).flatTraverse { synchronizerId =>
            fetchHosted(filterParty = "", synchronizerId).map(_.flatMap { party2Participant =>
              val maybePermission = party2Participant.item.participants
                .find(_.participantId == participantId)
                .map(_.permission)
              maybePermission
                .map(permission =>
                  ((party2Participant.item.partyId.uid, synchronizerId), permission)
                )
                .toList
            })
          }

        run[(UniqueIdentifier, SynchronizerId), ParticipantPermission](
          "party",
          removeParties,
          checkSelfConsistency,
          want = wanted,
          fetch = _ => fetchAll(),
          add = { case ((uid, synchronizerId), permission) =>
            createTopologyTx(uid, synchronizerId, permission)
          },
          upd = { case ((uid, synchronizerId), wantPermission, _) =>
            createTopologyTx(uid, synchronizerId, wantPermission)
          },
          rm = { case (u, s) => removeParty(u, s) },
          await = Some(awaitLedgerApiServer),
        )
      }
  }

  private def syncUsers(
      participantId: ParticipantId,
      users: Seq[DeclarativeUserConfig],
      removeExcess: Boolean,
      checkSelfConsistency: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] = {

    def fetchUsers(limit: PositiveInt): Either[String, Seq[(String, DeclarativeUserConfig)]] =
      queryLedgerApi(
        LedgerApiCommands.Users.List(
          filterUser = "",
          pageToken = "",
          pageSize = limit.unwrap,
          identityProviderId = "",
        )
      ).flatMap(_.users.filter(_.id != "participant_admin").traverse {
        case LedgerApiUser(id, primaryParty, isDeactivated, metadata, identityProviderId) =>
          queryLedgerApi(
            LedgerApiCommands.Users.Rights.List(id = id, identityProviderId = identityProviderId)
          ).map { rights =>
            (
              id,
              DeclarativeUserConfig(
                id = id,
                primaryParty = primaryParty.map(_.toProtoPrimitive),
                isDeactivated = isDeactivated,
                annotations = metadata.annotations,
                identityProviderId = identityProviderId,
                rights = DeclarativeUserRightsConfig(
                  actAs = rights.actAs.map(_.toProtoPrimitive),
                  readAs = rights.readAs.map(_.toProtoPrimitive),
                  participantAdmin = rights.participantAdmin,
                  identityProviderAdmin = rights.identityProviderAdmin,
                  readAsAnyParty = rights.readAsAnyParty,
                ),
              )(resourceVersion = metadata.resourceVersion),
            )
          }
      })

    def createUser(user: DeclarativeUserConfig): Either[String, Unit] =
      queryLedgerApi(
        LedgerApiCommands.Users.Create(
          id = user.id,
          actAs = user.rights.actAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
          primaryParty = user.primaryParty.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
          readAs = user.rights.readAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
          participantAdmin = user.rights.participantAdmin,
          identityProviderAdmin = user.rights.identityProviderAdmin,
          isDeactivated = user.isDeactivated,
          annotations = user.annotations,
          identityProviderId = user.identityProviderId,
          readAsAnyParty = user.rights.readAsAnyParty,
        )
      ).map(_ => ())

    def updateUser(
        desired: DeclarativeUserConfig,
        existing: DeclarativeUserConfig,
    ): Either[String, Unit] =
      // user settings are spread over user & rights
      if (desired.needsUserChange(existing)) {
        queryLedgerApi(
          LedgerApiCommands.Users.Update(
            id = desired.id,
            identityProviderId = existing.identityProviderId,
            primaryPartyUpdate = Option.when(desired.primaryParty != existing.primaryParty)(
              desired.primaryParty.map(PartyId.tryFromProtoPrimitive)
            ),
            isDeactivatedUpdate =
              Option.when(desired.isDeactivated != existing.isDeactivated)(desired.isDeactivated),
            annotationsUpdate =
              Option.when(desired.annotations != existing.annotations)(desired.annotations),
            resourceVersionO = existing.resourceVersion.some,
          )
        ).map(_ => ())
      } else Either.unit

    def updateRights(id: String, identityProviderId: String)(
        desired: DeclarativeUserRightsConfig,
        existing: DeclarativeUserRightsConfig,
    ): Either[String, Unit] =
      if (desired != existing) {
        def grantOrRevoke(have: Boolean, want: Boolean): (Boolean, Boolean) =
          if (have != want) if (want) (true, false) else (false, true) else (false, false)
        def grantOrRevokeSet(have: Set[String], want: Set[String]): (Set[String], Set[String]) = {
          val grant = want.diff(have)
          val revoke = have.diff(want)
          (grant, revoke)
        }
        val (grantParticipantAdmin, revokeParticipantAdmin) =
          grantOrRevoke(existing.participantAdmin, desired.participantAdmin)
        val (grantIdpAdmin, revokeIdpAdmin) = grantOrRevoke(
          existing.identityProviderAdmin,
          desired.identityProviderAdmin,
        )
        val (grantReadAsAny, revokeReadAsAny) =
          grantOrRevoke(existing.readAsAnyParty, desired.readAsAnyParty)
        val (grantReadAs, revokeReadAs) =
          grantOrRevokeSet(existing.readAs, desired.readAs)
        val (grantActAs, revokeActAs) =
          grantOrRevokeSet(existing.actAs, desired.actAs)
        val grantE =
          if (
            grantParticipantAdmin || grantIdpAdmin || grantReadAsAny || grantReadAs.nonEmpty || grantActAs.nonEmpty
          ) {
            queryLedgerApi(
              LedgerApiCommands.Users.Rights.Grant(
                id = id,
                actAs = grantActAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                readAs = grantReadAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                identityProviderId = identityProviderId,
                participantAdmin = grantParticipantAdmin,
                readAsAnyParty = grantReadAsAny,
                identityProviderAdmin = grantIdpAdmin,
              )
            ).map(_ => ())
          } else Either.unit
        val revokeE =
          if (
            revokeParticipantAdmin || revokeIdpAdmin || revokeReadAsAny || revokeReadAs.nonEmpty || revokeActAs.nonEmpty
          ) {
            queryLedgerApi(
              LedgerApiCommands.Users.Rights.Revoke(
                id = id,
                actAs = revokeActAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                readAs = revokeReadAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                identityProviderId = identityProviderId,
                participantAdmin = revokeParticipantAdmin,
                readAsAnyParty = revokeReadAsAny,
                identityProviderAdmin = revokeIdpAdmin,
              )
            ).map(_ => ())
          } else Either.unit
        grantE.flatMap(_ => revokeE)
      } else Either.unit

    def updateUserIdp(
        desired: DeclarativeUserConfig,
        existing: DeclarativeUserConfig,
    ): Either[String, Unit] =
      if (desired.identityProviderId != existing.identityProviderId) {
        queryLedgerApi(
          LedgerApiCommands.Users.UpdateIdp(
            id = desired.id,
            sourceIdentityProviderId = existing.identityProviderId,
            targetIdentityProviderId = desired.identityProviderId,
          )
        )
      } else Either.unit

    run[String, DeclarativeUserConfig](
      "users",
      removeExcess,
      checkSelfConsistency,
      users.map(user => (user.id, user.mapPartiesToNamespace(participantId.uid.namespace))),
      fetch = fetchUsers,
      add = { case (_, user) => createUser(user) },
      upd = { case (_, desired, existing) =>
        for {
          _ <- updateUser(desired, existing)
          _ <- updateRights(desired.id, desired.identityProviderId)(desired.rights, existing.rights)
          _ <- updateUserIdp(desired, existing)
        } yield ()
      },
      rm = id =>
        queryLedgerApi(LedgerApiCommands.Users.Delete(id, identityProviderId = "")).map(_ => ()),
    )
  }

  private def syncConnections(
      connections: Seq[DeclarativeConnectionConfig],
      removeConnections: Boolean,
      checkSelfConsistent: Boolean,
  )(implicit traceContext: TraceContext): Either[String, UpdateResult] = {

    def toDeclarative(config: SynchronizerConnectionConfig): (String, DeclarativeConnectionConfig) =
      (
        config.synchronizerAlias.unwrap,
        DeclarativeConnectionConfig(
          synchronizerAlias = config.synchronizerAlias.unwrap,
          connections = config.sequencerConnections.aliasToConnection.map {
            case (alias, connection: GrpcSequencerConnection) =>
              (
                alias.unwrap,
                DeclarativeSequencerConnectionConfig(
                  endpoints = connection.endpoints,
                  transportSecurity = connection.transportSecurity,
                  customTrustCertificates = None,
                )(customTrustCertificatesFromNode = connection.customTrustCertificates),
              )
          }.toMap,
          manualConnect = config.manualConnect,
          priority = config.priority,
          initializeFromTrustedSynchronizer = config.initializeFromTrustedSynchronizer,
          trustThreshold = config.sequencerConnections.sequencerTrustThreshold,
        ),
      )

    def fetchConnections(): Either[String, (Seq[(String, DeclarativeConnectionConfig)])] =
      queryAdminApi(ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers)
        .map(_.map(_._1).map(toDeclarative))

    def removeSynchronizerConnection(alias: String): Either[String, Unit] = {
      val synchronizerAlias = SynchronizerAlias.tryCreate(alias)
      // cannot really remove connections for now, just disconnect and disable
      for {
        currentO <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers
        ).map(_.find(_._1.synchronizerAlias == synchronizerAlias))
        current <- currentO
          .toRight(s"Unable to find configuration for synchronizer $alias")
          .map(_._1)
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.DisconnectSynchronizer(
            synchronizerAlias
          )
        )
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ModifySynchronizerConnection(
            current.copy(manualConnect = true),
            sequencerConnectionValidation = SequencerConnectionValidation.Disabled,
          )
        )
      } yield ()

    }

    run[String, DeclarativeConnectionConfig](
      "connections",
      removeExcess = removeConnections,
      checkSelfConsistent = checkSelfConsistent,
      want = connections.map(c => (c.synchronizerAlias, c)),
      fetch = _ => fetchConnections(),
      add = { case (_, config) =>
        queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ConnectSynchronizer(
            config.toSynchronizerConnectionConfig,
            sequencerConnectionValidation = SequencerConnectionValidation.Active,
          )
        )
      },
      upd = { case (_, config, existing) =>
        if (config.isEquivalent(existing)) Either.unit
        else
          queryAdminApi(
            ParticipantAdminCommands.SynchronizerConnectivity.ModifySynchronizerConnection(
              config.toSynchronizerConnectionConfig,
              sequencerConnectionValidation = SequencerConnectionValidation.Active,
            )
          )
      },
      rm = removeSynchronizerConnection,
      compare = Some { case (x, y) => x.isEquivalent(y) },
    )

  }

  private val matchDar = "([^/]+).dar".r

  private def mirrorDarsIfNecessary(fetchDarDirectory: File, dars: Seq[DeclarativeDarConfig])(
      implicit traceContext: TraceContext
  ): Seq[(File, Option[String])] = dars.flatMap { dar =>
    if (dar.path.startsWith("http")) {
      val matched = matchDar
        .findFirstMatchIn(dar.path)
      if (matched.isEmpty) {
        logger.warn(s"Cannot fetch DAR from URL without .dar extension: ${dar.path}")
      }
      matched.flatMap { matched =>
        val output = new File(fetchDarDirectory, matched.matched)
        logger.info(s"Downloading ${dar.path} to $output")
        BinaryFileUtil
          .downloadFile(dar.path, output.toString, dar.requestHeaders)
          .leftMap { err =>
            logger.warn(s"Failed to download ${dar.path}: $err")
          }
          .toOption
          .map(_ => (output, dar.expectedMainPackage))
      }.toList
    } else List((new File(dar.path), dar.expectedMainPackage))
  }

  private def computeWanted(dars: Seq[(File, Option[String])])(implicit
      traceContext: TraceContext
  ): Seq[(String, String)] =
    dars.flatMap { case (item, expected) =>
      val bufInput = new FileInputStream(item)
      val zipInputStream = new ZipInputStream(bufInput)
      DarParser
        .readArchive("file", zipInputStream)
        .toOption
        .flatMap { loaded =>
          expected match {
            case Some(value) if value != loaded.main.getHash =>
              logger.warn(s"DAR $item has main package ${loaded.main.getHash} but expected $value")
              None
            case _ => Some((loaded.main.getHash, item.toString))
          }
        }
        .toList
    }

  private def syncDars(
      dars: Seq[DeclarativeDarConfig],
      checkSelfConsistency: Boolean,
      fetchDarDirectory: File,
  )(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] = {
    val want = computeWanted(mirrorDarsIfNecessary(fetchDarDirectory, dars))
    def fetchDars(limit: PositiveInt): Either[String, Seq[(String, String)]] =
      for {
        dars <- queryAdminApi(ParticipantAdminCommands.Package.ListDars(filterName = "", limit))
      } yield dars.map(_.main).map((_, "<ignored string>"))
    run[String, String](
      "dars",
      removeExcess = false,
      checkSelfConsistency,
      want = want,
      fetch = fetchDars,
      add = { case (_, file) =>
        queryAdminApi(
          ParticipantAdminCommands.Package.UploadDar(
            darPath = file,
            vetAllPackages = true,
            synchronizeVetting = false,
            description = "Uploaded by declarative API",
            expectedMainPackageId = "",
            requestHeaders = Map.empty,
            logger,
          )
        ).map(_ => ())
      },
      upd = { case (hash, desired, existing) => Either.unit },
      rm = _ => Either.unit, // not implemented in canton yet
      onlyCheckKeys = true,
    )

  }

}

object DeclarativeParticipantApi {

  def readConfig(
      file: File
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[String, DeclarativeParticipantConfig] = {
    import DeclarativeParticipantApi.Readers.*
    DeclarativeApi.readConfigImpl[DeclarativeParticipantConfig](file, "participant-state")
  }

  object Readers {
    import com.daml.nonempty.NonEmptyUtil.instances.*
    import pureconfig.ConfigReader
    import pureconfig.generic.semiauto.*
    // import canton config to include the implicit that prevents unknown keys

    implicit val declarativeParticipantConfigReader: ConfigReader[DeclarativeParticipantConfig] = {
      implicit val darConfigReader: ConfigReader[DeclarativeDarConfig] =
        deriveReader[DeclarativeDarConfig]
      implicit val namespaceReader: ConfigReader[Namespace] = ConfigReader.fromString[Namespace] {
        str =>
          Fingerprint
            .create(str)
            .map(Namespace(_))
            .leftMap(err => CannotConvert(str, "Namespace", err))
      }
      implicit val permissionReader: ConfigReader[ParticipantPermission] =
        ConfigReader.fromString[ParticipantPermission] { str =>
          str.toUpperCase match {
            case "CONFIRMATION" => Right(ParticipantPermission.Confirmation)
            case "SUBMISSION" => Right(ParticipantPermission.Submission)
            case "OBSERVATION" => Right(ParticipantPermission.Observation)
            case other =>
              Left(CannotConvert(str, "ParticipantPermission", "Not a valid permission"))
          }
        }

      implicit val partyConfigReader: ConfigReader[DeclarativePartyConfig] =
        deriveReader[DeclarativePartyConfig]
      implicit val rightsConfigReader: ConfigReader[DeclarativeUserRightsConfig] =
        deriveReader[DeclarativeUserRightsConfig]
      implicit val userConfigReader: ConfigReader[DeclarativeUserConfig] =
        deriveReader[DeclarativeUserConfig]
      implicit val endpointReader: ConfigReader[Endpoint] = deriveReader[Endpoint]
      implicit val sequencerConnectionConfigReader
          : ConfigReader[DeclarativeSequencerConnectionConfig] =
        deriveReader[DeclarativeSequencerConnectionConfig].emap { parsed =>
          parsed.customTrustCertificatesAsByteString
            .leftMap(err => CannotConvert(parsed.customTrustCertificates.toString, "bytes", err))
            .map(_ => parsed)
        }
      implicit val connectionConfigReader: ConfigReader[DeclarativeConnectionConfig] =
        deriveReader[DeclarativeConnectionConfig]
      deriveReader[DeclarativeParticipantConfig]
    }

  }

}
