// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import cats.syntax.either.*
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfig
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
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api
import com.digitalasset.canton.ledger.api.IdentityProviderId
import com.digitalasset.canton.lifecycle.{CloseContext, LifeCycle, RunOnClosing}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DeclarativeApiMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnectionValidation}
import com.digitalasset.canton.topology.admin.grpc.{BaseQuery, TopologyStoreId}
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{SynchronizerAlias, config}
import com.digitalasset.daml.lf.archive.DarParser
import com.google.protobuf.field_mask.FieldMask

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class DeclarativeParticipantApi(
    override val name: String,
    ledgerApiConfig: ClientConfig,
    adminApiConfig: ClientConfig,
    override val consistencyTimeout: config.NonNegativeDuration,
    adminToken: => Option[CantonAdminToken],
    runnerFactory: String => GrpcAdminCommandRunner,
    val closeContext: CloseContext,
    val metrics: DeclarativeApiMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DeclarativeApi[DeclarativeParticipantConfig, ParticipantId] {

  private val adminApiRunner = runnerFactory(CantonGrpcUtil.ApiName.AdminApi)
  private val ledgerApiRunner = runnerFactory(CantonGrpcUtil.ApiName.LedgerApi)
  closeContext.context
    .runOnOrAfterClose(new RunOnClosing {
      override def name: String = "stop-declarative-api"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit =
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
    // TODO(#25043) the "remove" process will likely cause noise as we need to run the order
    //   in reverse. it will still work but just be noisy. need to fix that.
    for {
      connections <- syncConnections(
        config.connections,
        config.removeConnections,
        config.checkSelfConsistency,
      )
      idps <- syncIdps(
        config.idps,
        config.removeIdps,
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
    } yield Seq(connections, idps, parties, users, dars).foldLeft(UpdateResult())(_.merge(_))

  private def createDarDirectoryIfNecessary(
      fetchedDarDirectory: File,
      dars: Seq[DeclarativeDarConfig],
  ): Either[String, Unit] =
    if (dars.exists(_.location.startsWith("http"))) {
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
        store = TopologyStoreId.Synchronizer(synchronizerId),
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
            store = TopologyStoreId.Synchronizer(synchronizerId),
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
            store = TopologyStoreId.Synchronizer(synchronizerId),
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
        idps <- queryLedgerApi(LedgerApiCommands.IdentityProviderConfigs.List())
        // loop over all idps and include default one
        observed <- (idps.map(_.identityProviderId) :+ "").flatTraverse(idp =>
          queryLedgerApi(
            LedgerApiCommands.PartyManagementService.ListKnownParties(identityProviderId = idp)
          )
        )
        observedUids <- observed
          .traverse(details => UniqueIdentifier.fromProtoPrimitive(details.party, "party"))
          .leftMap(_.toString)
      } yield {
        val observedSet = observedUids.toSet
        parties.map(_._1).toSet.subsetOf(observedSet)
      }

    queryAdminApi(ListConnectedSynchronizers())
      .flatMap { synchronizerIds =>
        val wanted = parties.flatMap { p =>
          val party =
            if (p.party.contains(UniqueIdentifier.delimiter))
              UniqueIdentifier.tryFromProtoPrimitive(p.party)
            else
              UniqueIdentifier.tryCreate(p.party, nodeNamespace)
          synchronizerIds
            .filter(s =>
              p.synchronizers.isEmpty || p.synchronizers.contains(s.synchronizerAlias.unwrap)
            )
            .map(s => ((party, s.synchronizerId.logical), p.permission))
        }

        def fetchAll() =
          // fold synchronizers and found parties and find the ones that are allocated to our node
          synchronizerIds.map(_.synchronizerId.logical).flatTraverse { synchronizerId =>
            fetchHosted(filterParty = "", synchronizerId).map(_.flatMap { party2Participant =>
              val maybePermission = party2Participant.item.participants
                .find(_.participantId == participantId)
                .map(_.permission)
              maybePermission
                .map(permission =>
                  (
                    (party2Participant.item.partyId.uid, synchronizerId),
                    ParticipantPermissionConfig.fromInternal(permission),
                  )
                )
                .toList
            })
          }

        run[(UniqueIdentifier, SynchronizerId), ParticipantPermissionConfig](
          "party",
          removeParties,
          checkSelfConsistency,
          want = wanted,
          fetch = _ => fetchAll(),
          add = { case ((uid, synchronizerId), permission) =>
            createTopologyTx(uid, synchronizerId, permission.toNative)
          },
          upd = { case ((uid, synchronizerId), wantPermission, _) =>
            createTopologyTx(uid, synchronizerId, wantPermission.toNative)
          },
          rm = { case ((u, s), current) => removeParty(u, s) },
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

    // temporarily cache the available parties
    val idpParties = mutable.Map[String, Set[String]]()
    def getIdpParties(idp: String): Either[String, Set[String]] =
      idpParties.get(idp) match {
        case Some(parties) => Right(parties)
        case None =>
          queryLedgerApi(
            LedgerApiCommands.PartyManagementService.ListKnownParties(identityProviderId = idp)
          ).map { parties =>
            val partySet = parties.map(_.party).toSet
            idpParties.put(idp, partySet).discard
            partySet
          }
      }

    def fetchUsers(limit: PositiveInt): Either[String, Seq[(String, DeclarativeUserConfig)]] =
      for {
        // meeh, we need to iterate over all idps to load all users
        idps <- queryLedgerApi(
          LedgerApiCommands.IdentityProviderConfigs.List()
        ).map(_.map(_.identityProviderId))
        users <- (idps :+ "") // empty string to load default idp
          .traverse(idp =>
            queryLedgerApi(
              LedgerApiCommands.Users.List(
                filterUser = "",
                pageToken = "",
                pageSize = limit.unwrap,
                identityProviderId = idp,
              )
            )
          )
          .map(_.flatMap(_.users.filter(_.id != "participant_admin")))
        parsedUsers <- users.traverse {
          case LedgerApiUser(id, primaryParty, isDeactivated, metadata, identityProviderId) =>
            queryLedgerApi(
              LedgerApiCommands.Users.Rights.List(id = id, identityProviderId = identityProviderId)
            ).map { rights =>
              (
                id,
                DeclarativeUserConfig(
                  user = id,
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
        }
      } yield parsedUsers.take(limit.value)

    def createUser(user: DeclarativeUserConfig): Either[String, Unit] =
      queryLedgerApi(
        LedgerApiCommands.Users.Create(
          id = user.user,
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
            id = desired.user,
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
            id = desired.user,
            sourceIdentityProviderId = existing.identityProviderId,
            targetIdentityProviderId = desired.identityProviderId,
          )
        )
      } else Either.unit

    def activePartyFilter(idp: String, user: String): Either[String, String => Boolean] =
      getIdpParties(idp).map { parties => party =>
        {
          if (!parties.contains(party)) {
            logger.info(s"User $user refers to party $party not yet known to the ledger api server")
            false
          } else true
        }
      }

    val wantedE =
      users.traverse { user =>
        activePartyFilter(user.identityProviderId, user.user).map { filter =>
          (
            user.user,
            user.mapPartiesToNamespace(
              participantId.uid.namespace,
              filter,
            ),
          )

        }
      }
    wantedE.flatMap { wanted =>
      run[String, DeclarativeUserConfig](
        "users",
        removeExcess,
        checkSelfConsistency,
        want = wanted,
        fetch = fetchUsers,
        add = { case (_, user) =>
          createUser(user)
        },
        upd = { case (_, desired, existing) =>
          for {
            _ <- updateUser(desired, existing)
            _ <- updateUserIdp(desired, existing)
            _ <- updateRights(desired.user, desired.identityProviderId)(
              desired.rights,
              existing.rights,
            )
          } yield ()
        },
        rm = (id, v) => {
          queryLedgerApi(
            LedgerApiCommands.Users.Delete(id, identityProviderId = v.identityProviderId)
          ).map(_ => ())
        },
      )
    }
  }

  private def syncConnections(
      connections: Seq[DeclarativeConnectionConfig],
      removeConnections: Boolean,
      checkSelfConsistent: Boolean,
  )(implicit traceContext: TraceContext): Either[String, UpdateResult] = {

    def toDeclarative(
        config: SynchronizerConnectionConfig
    ): (SynchronizerAlias, DeclarativeConnectionConfig) =
      (
        config.synchronizerAlias,
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

    def fetchConnections(): Either[String, Seq[(SynchronizerAlias, DeclarativeConnectionConfig)]] =
      queryAdminApi(ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers)
        .map(_.map { case (synchronizerConnectionConfig, _, _) => synchronizerConnectionConfig }
          .map(toDeclarative))

    def removeSynchronizerConnection(
        synchronizerAlias: SynchronizerAlias
    ): Either[String, Unit] =
      // cannot really remove connections for now, just disconnect and disable
      for {
        currentO <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ListRegisteredSynchronizers
        ).map(_.collectFirst {
          case (config, psidO, _) if config.synchronizerAlias == synchronizerAlias =>
            (config, psidO)
        })
        current <- currentO
          .toRight(s"Unable to find configuration for synchronizer $synchronizerAlias")
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.DisconnectSynchronizer(
            synchronizerAlias
          )
        )
        (currentConfig, psidO) = current
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ModifySynchronizerConnection(
            config = currentConfig.copy(manualConnect = true),
            synchronizerId = psidO.toOption,
            sequencerConnectionValidation = SequencerConnectionValidation.Disabled,
          )
        )
      } yield ()

    def add(config: DeclarativeConnectionConfig): Either[String, Unit] =
      for {
        synchronizerConnectionConfig <- config.toSynchronizerConnectionConfig
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ConnectSynchronizer(
            synchronizerConnectionConfig,
            sequencerConnectionValidation = SequencerConnectionValidation.Active,
          )
        )
      } yield ()

    def update(config: DeclarativeConnectionConfig) =
      for {
        synchronizerConnectionConfig <- config.toSynchronizerConnectionConfig
        _ <- queryAdminApi(
          ParticipantAdminCommands.SynchronizerConnectivity.ModifySynchronizerConnection(
            // TODO(#25344) Allow to specify the PSId for declarative configs
            synchronizerId = None,
            synchronizerConnectionConfig,
            sequencerConnectionValidation = SequencerConnectionValidation.Active,
          )
        )
      } yield ()

    run[SynchronizerAlias, DeclarativeConnectionConfig](
      "connections",
      removeExcess = removeConnections,
      checkSelfConsistent = checkSelfConsistent,
      want = connections.map(c => (SynchronizerAlias.tryCreate(c.synchronizerAlias), c)),
      fetch = _ => fetchConnections(),
      add = { case (_, config) => add(config) },
      upd = { case (_, config, existing) =>
        if (config.isEquivalent(existing)) Either.unit
        else
          update(config)
      },
      rm = (alias, _) => removeSynchronizerConnection(alias),
      compare = Some { case (x, y) => x.isEquivalent(y) },
    )

  }

  private def syncIdps(
      idps: Seq[DeclarativeIdpConfig],
      removeIdps: Boolean,
      checkSelfConsistent: Boolean,
  )(implicit traceContext: TraceContext): Either[String, UpdateResult] = {

    def toDeclarative(api: IdentityProviderConfig): DeclarativeIdpConfig = DeclarativeIdpConfig(
      identityProviderId = api.identityProviderId,
      isDeactivated = api.isDeactivated,
      jwksUrl = api.jwksUrl,
      issuer = api.issuer,
      audience = Option.when(api.audience.nonEmpty)(api.audience),
    )

    def fetchIdps(): Either[String, Seq[(String, DeclarativeIdpConfig)]] =
      queryLedgerApi(
        LedgerApiCommands.IdentityProviderConfigs.List()
      ).map(_.map(c => (c.identityProviderId, toDeclarative(c))))

    def add(config: DeclarativeIdpConfig): Either[String, Unit] =
      queryLedgerApi(
        LedgerApiCommands.IdentityProviderConfigs.Create(
          identityProviderId = config.apiIdentityProviderId,
          isDeactivated = config.isDeactivated,
          jwksUrl = config.apiJwksUrl,
          issuer = config.issuer,
          audience = config.audience,
        )
      ).map(_ => ())

    def update(config: DeclarativeIdpConfig): Either[String, Unit] =
      queryLedgerApi(
        LedgerApiCommands.IdentityProviderConfigs.Update(
          identityProviderConfig = api.IdentityProviderConfig(
            identityProviderId = config.apiIdentityProviderId,
            isDeactivated = config.isDeactivated,
            jwksUrl = config.apiJwksUrl,
            issuer = config.issuer,
            audience = config.audience,
          ),
          updateMask = FieldMask(Seq("is_deactivated", "jwks_url", "audience", "issuer")),
        )
      ).map(_ => ())

    def removeIdp(idpName: String): Either[String, Unit] =
      queryLedgerApi(
        LedgerApiCommands.IdentityProviderConfigs.Delete(
          identityProviderId = IdentityProviderId.Id.assertFromString(idpName)
        )
      ).map(_ => ())

    run[String, DeclarativeIdpConfig](
      "idps",
      removeExcess = removeIdps,
      checkSelfConsistent = checkSelfConsistent,
      want = idps.map(c => (c.identityProviderId, c)),
      fetch = _ => fetchIdps(),
      add = { case (_, config) => add(config) },
      upd = { case (_, config, _) => update(config) },
      rm = (idp, _) => removeIdp(idp),
    )
  }

  private val matchDar = "([^/]+).dar".r

  private def mirrorDarsIfNecessary(fetchDarDirectory: File, dars: Seq[DeclarativeDarConfig])(
      implicit traceContext: TraceContext
  ): Seq[(File, Option[String])] = dars.flatMap { dar =>
    if (dar.location.startsWith("http")) {
      val matched = matchDar
        .findFirstMatchIn(dar.location)
      if (matched.isEmpty) {
        logger.warn(s"Cannot fetch DAR from URL without .dar extension: ${dar.location}")
      }
      matched.flatMap { matched =>
        val output = new File(fetchDarDirectory, matched.matched)
        logger.info(s"Downloading ${dar.location} to $output")
        BinaryFileUtil
          .downloadFile(dar.location, output.toString, dar.requestHeaders)
          .leftMap { err =>
            logger.warn(s"Failed to download ${dar.location}: $err")
          }
          .toOption
          .map(_ => (output, dar.expectedMainPackage))
      }.toList
    } else List((new File(dar.location), dar.expectedMainPackage))
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
      } yield dars
        .filterNot(_.name == "AdminWorkflows")
        .map(_.mainPackageId)
        .map((_, "<ignored string>"))
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
      rm = (_, _) => Either.unit, // not implemented in canton yet
      onlyCheckKeys = true,
    )

  }

}
