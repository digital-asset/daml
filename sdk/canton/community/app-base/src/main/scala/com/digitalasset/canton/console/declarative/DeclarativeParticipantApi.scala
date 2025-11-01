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
import com.digitalasset.canton.admin.api.client.data.{
  LedgerApiUser,
  ListConnectedSynchronizersResult,
}
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.ClientConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandErrors.{CommandError, GenericCommandError}
import com.digitalasset.canton.console.declarative.DeclarativeApi.UpdateResult
import com.digitalasset.canton.console.declarative.DeclarativeParticipantApi.{
  Err,
  NotFound,
  QueryResult,
}
import com.digitalasset.canton.console.{CommandSuccessful, GrpcAdminCommandRunner}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api
import com.digitalasset.canton.ledger.api.IdentityProviderId
import com.digitalasset.canton.lifecycle.{CloseContext, LifeCycle, RunOnClosing}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DeclarativeApiMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.AdminWorkflowServices
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
import com.digitalasset.canton.util.{BinaryFileUtil, MonadUtil}
import com.digitalasset.canton.{SynchronizerAlias, config}
import com.digitalasset.daml.lf.archive.DarParser
import com.google.protobuf.field_mask.FieldMask

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import scala.annotation.tailrec
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
      command: GrpcAdminCommand[?, ?, Result],
  )(implicit traceContext: TraceContext): Either[QueryResult, Result] = if (
    closeContext.context.isClosing
  )
    Left(Err("Node is shutting down"))
  else
    activeAdminToken.fold(Left(Err("Node instance is passive")): Either[QueryResult, Result])(
      token =>
        runner.runCommandWithExistingTrace(name, command, cfg, Some(token.secret)) match {
          case CommandSuccessful(value) => Right(value)
          case GenericCommandError(cause) if cause.contains("NOT_FOUND/") =>
            Left(NotFound(cause))
          case c: CommandError => Left(Err(c.cause))
        }
    )

  private def queryAdminApi[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext): Either[String, Result] =
    queryApi(adminApiRunner, adminApiConfig, command).leftMap(_.str)

  private def queryLedgerApi[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext): Either[String, Result] =
    queryApi(ledgerApiRunner, ledgerApiConfig, command).leftMap(_.str)

  private def toOptionalE[Result](
      res: Either[QueryResult, Result]
  ): Either[String, Option[Result]] = res match {
    case Right(v) => Right(Some(v))
    case Left(NotFound(_)) => Right(None)
    case Left(Err(str)) => Left(str)
  }

  private def queryAdminApiIfExists[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext): Either[String, Option[Result]] =
    toOptionalE(queryApi(adminApiRunner, adminApiConfig, command))

  private def queryLedgerApiIfExists[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext): Either[String, Option[Result]] =
    toOptionalE(queryApi(ledgerApiRunner, ledgerApiConfig, command))

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
        store = synchronizerId,
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

    def fetchHostedTuple(filterParty: String, synchronizerId: SynchronizerId) =
      fetchHosted(filterParty, synchronizerId).map(_.flatMap { party2Participant =>
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
            store = synchronizerId,
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
            store = synchronizerId,
            mustFullyAuthorize = true,
            change = TopologyChangeOp.Remove,
            waitToBecomeEffective = Some(consistencyTimeout),
          )
        )
      } yield {}

    def awaitLedgerApiServer(
        parties: Seq[(UniqueIdentifier, SynchronizerId)]
    ): Either[String, Boolean] = {
      @tailrec
      def go(
          idps: List[String],
          pending: Set[PartyId],
      ): Either[String, Set[PartyId]] = if (pending.isEmpty) Right(Set.empty)
      else
        idps match {
          case Nil => Right(pending)
          case next :: rest =>
            val unknown = findPartiesNotKnownToIdp(next, pending)
            unknown match {
              case Right(unknown) => go(rest, unknown)
              case Left(value) => Left(value)
            }
        }

      for {
        idps <- queryLedgerApi(LedgerApiCommands.IdentityProviderConfigs.List())
        // loop over all idps and include default one
        notFound <- go(
          (idps.map(_.identityProviderId) :+ "").toList,
          parties.map { case (uid, _) => PartyId(uid) }.toSet,
        )
      } yield {
        notFound.isEmpty
      }
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
            .map(s => ((party, s.synchronizerId), p.permission))
        }

        def fetchAll() =
          // fold synchronizers and found parties and find the ones that are allocated to our node
          synchronizerIds.map(_.synchronizerId).flatTraverse { synchronizerId =>
            fetchHostedTuple(filterParty = "", synchronizerId)
          }

        run[(UniqueIdentifier, SynchronizerId), ParticipantPermissionConfig](
          "party",
          removeParties,
          checkSelfConsistency,
          want = wanted,
          fetch = _ => fetchAll(),
          get = { case (uid, synchronizerId) =>
            fetchHostedTuple(uid.toProtoPrimitive, synchronizerId).map(_.toList).flatMap {
              case (_, v) :: Nil => Right(Some(v))
              case Nil => Right(None)
              case rst =>
                Left(
                  s"Multiple entries found for party $uid and synchronizer $synchronizerId: $rst"
                )
            }
          },
          add = { case ((uid, synchronizerId), permission) =>
            createTopologyTx(uid, synchronizerId, permission.toNative)
          },
          upd = { case ((uid, synchronizerId), wantPermission, _) =>
            createTopologyTx(uid, synchronizerId, wantPermission.toNative)
          },
          rm = { case ((u, s), current) =>
            removeParty(u, s)
          },
          await = Some(awaitLedgerApiServer),
        )
      }
  }

  private def findPartiesNotKnownToIdp(idp: String, parties: Set[PartyId])(implicit
      traceContext: TraceContext
  ) =
    queryLedgerApi(
      LedgerApiCommands.PartyManagementService.GetParties(
        parties = parties.toList,
        identityProviderId = idp,
        failOnNotFound = false,
      )
    ).map { found =>
      parties -- found.keySet
    }

  private def syncUsers(
      participantId: ParticipantId,
      users: Seq[DeclarativeUserConfig],
      removeExcess: Boolean,
      checkSelfConsistency: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] = {

    def fetchUserRights(user: LedgerApiUser): Either[String, DeclarativeUserConfig] = user match {
      case LedgerApiUser(id, primaryParty, isDeactivated, metadata, identityProviderId) =>
        queryLedgerApi(
          LedgerApiCommands.Users.Rights.List(id = id, identityProviderId = identityProviderId)
        ).map { rights =>
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
          )(resourceVersion = metadata.resourceVersion)
        }
    }

    val idpsE = queryLedgerApi(
      LedgerApiCommands.IdentityProviderConfigs.List()
    ).map(x => "" +: x.map(_.identityProviderId)) // empty string to load default idp)

    def getUser(id: String): Either[String, Option[DeclarativeUserConfig]] = idpsE.flatMap { idps =>
      MonadUtil
        .foldLeftM(None: Option[LedgerApiUser], idps) {
          case (Some(cfg), _) => Right(Some(cfg))
          case (None, idp) =>
            // using list user and not get user as get user will return permission denied and log warning
            // however, as user ids are unique, this will give the same result
            queryLedgerApiIfExists(
              LedgerApiCommands.Users
                .List(filterUser = id, identityProviderId = idp, pageToken = "", pageSize = 10000)
            ).map { res =>
              res.flatMap(_.users.find(_.id == id))
            }
        }
        .flatMap {
          case Some(user) => fetchUserRights(user).map(c => Some(c))
          case None => Right(None)
        }
    }

    def fetchUsers(limit: PositiveInt): Either[String, Seq[(String, DeclarativeUserConfig)]] =
      for {
        // meeh, we need to iterate over all idps to load all users
        idps <- idpsE
        users <- idps
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
        parsedUsers <- users.traverse { user =>
          fetchUserRights(user).map(cfg => (cfg.user, cfg))
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
          executeAs = user.rights.executeAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
          executeAsAnyParty = user.rights.executeAsAnyParty,
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
        val (grantExecuteAsAny, revokeExecuteAsAny) =
          grantOrRevoke(existing.executeAsAnyParty, desired.executeAsAnyParty)
        val (grantReadAs, revokeReadAs) =
          grantOrRevokeSet(existing.readAs, desired.readAs)
        val (grantExecuteAs, revokeExecuteAs) =
          grantOrRevokeSet(existing.readAs, desired.readAs)
        val (grantActAs, revokeActAs) =
          grantOrRevokeSet(existing.actAs, desired.actAs)
        val grantE =
          if (
            grantParticipantAdmin || grantIdpAdmin || grantReadAsAny || grantReadAs.nonEmpty || grantActAs.nonEmpty || grantExecuteAsAny || grantExecuteAs.nonEmpty
          ) {
            queryLedgerApi(
              LedgerApiCommands.Users.Rights.Grant(
                id = id,
                actAs = grantActAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                readAs = grantReadAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                executeAs = grantExecuteAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                identityProviderId = identityProviderId,
                participantAdmin = grantParticipantAdmin,
                readAsAnyParty = grantReadAsAny,
                executeAsAnyParty = grantExecuteAsAny,
                identityProviderAdmin = grantIdpAdmin,
              )
            ).map(_ => ())
          } else Either.unit
        val revokeE =
          if (
            revokeParticipantAdmin || revokeIdpAdmin || revokeReadAsAny || revokeReadAs.nonEmpty || revokeActAs.nonEmpty || revokeExecuteAsAny || revokeExecuteAs.nonEmpty
          ) {
            queryLedgerApi(
              LedgerApiCommands.Users.Rights.Revoke(
                id = id,
                actAs = revokeActAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                readAs = revokeReadAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                executeAs = revokeExecuteAs.map(PartyId.tryFromProtoPrimitive).map(_.toLf),
                identityProviderId = identityProviderId,
                participantAdmin = revokeParticipantAdmin,
                readAsAnyParty = revokeReadAsAny,
                executeAsAnyParty = revokeExecuteAsAny,
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

    val wantedE =
      users.traverse { user =>
        val mapped = user.mapPartiesToNamespace(
          participantId.uid.namespace
        )

        // Ledger API server can only assign to parties it has already seen. Therefore, we remove
        // the parties not yet known. This can happen if we don't have a synchronizer connection
        // when setting up the users.
        findPartiesNotKnownToIdp(user.identityProviderId, mapped.referencedParties).map { unknown =>
          if (unknown.nonEmpty) {
            logger.info(
              s"User ${user.user} with idp=${user.identityProviderId} refers to the following parties not known to the Ledger API server: $unknown"
            )
          }
          (user.user, mapped.removeParties(unknown))
        }
      }

    wantedE.flatMap { wanted =>
      run[String, DeclarativeUserConfig](
        "users",
        removeExcess,
        checkSelfConsistency,
        want = wanted,
        fetch = fetchUsers,
        get = getUser,
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

    def getConnection(
        alias: SynchronizerAlias
    ): Either[String, Option[DeclarativeConnectionConfig]] =
      fetchConnections().map(_.collectFirst { case (a, c) if a == alias => c })

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
      get = getConnection,
      add = { case (_, config) =>
        add(config)
      },
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

    def getIdp(idpName: String): Either[String, Option[DeclarativeIdpConfig]] =
      queryLedgerApiIfExists(
        LedgerApiCommands.IdentityProviderConfigs.Get(identityProviderId =
          IdentityProviderId.Id.assertFromString(idpName)
        )
      ).map(_.map(toDeclarative))

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
      get = getIdp,
      add = { case (_, config) =>
        add(config)
      },
      upd = { case (_, config, _) =>
        update(config)
      },
      rm = (idp, _) => removeIdp(idp),
    )
  }

  private val matchDar = "([^/]+).dar".r

  private def mirrorDarsIfNecessary(fetchDarDirectory: File, dars: Seq[DeclarativeDarConfig])(
      implicit traceContext: TraceContext
  ): Seq[(File, Option[String], Seq[String])] = dars.flatMap { dar =>
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
          .map(_ => (output, dar.expectedMainPackage, dar.synchronizers))
      }.toList
    } else List((new File(dar.location), dar.expectedMainPackage, dar.synchronizers))
  }

  private def computeWanted(
      dars: Seq[(File, Option[String], Seq[String])],
      connectedSynchronizers: Seq[ListConnectedSynchronizersResult],
  )(implicit
      traceContext: TraceContext
  ): Seq[((String, SynchronizerId), String)] =
    dars.flatMap { case (item, expected, synchronizers) =>
      val bufInput = new FileInputStream(item)
      val zipInputStream = new ZipInputStream(bufInput)
      DarParser
        .readArchive("file", zipInputStream)
        .toList
        .flatMap { loaded =>
          expected match {
            case Some(value) if value != loaded.main.getHash =>
              logger.warn(s"DAR $item has main package ${loaded.main.getHash} but expected $value")
              Seq.empty
            case _ =>
              connectedSynchronizers
                .filter(s =>
                  synchronizers.isEmpty || synchronizers.contains(s.synchronizerAlias.unwrap)
                )
                .map(s => ((loaded.main.getHash, s.synchronizerId), item.toString))
          }
        }
    }

  private def syncDars(
      dars: Seq[DeclarativeDarConfig],
      checkSelfConsistency: Boolean,
      fetchDarDirectory: File,
  )(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult] =
    queryAdminApi(TopologyAdminCommands.Init.GetId()).flatMap { participantId =>
      queryAdminApi(ListConnectedSynchronizers()).flatMap { connectedSynchronizers =>
        val want =
          computeWanted(mirrorDarsIfNecessary(fetchDarDirectory, dars), connectedSynchronizers)

        def fetchVettedPackages(store: Option[TopologyStoreId]) =
          queryAdminApi(
            TopologyAdminCommands.Read.ListVettedPackages(
              BaseQuery(
                store = store,
                proposals = false,
                timeQuery = TimeQuery.HeadState,
                ops = Some(TopologyChangeOp.Replace),
                filterSigningKey = "",
                protocolVersion = None,
              ),
              filterParticipant = ParticipantId(participantId).filterString,
            )
          )

        def fetchDars(limit: PositiveInt): Either[String, Seq[((String, SynchronizerId), String)]] =
          for {
            dars <- queryAdminApi(ParticipantAdminCommands.Package.ListDars(filterName = "", limit))
            vettedPackages <- fetchVettedPackages(None)
          } yield {
            val packageToSynchronizers = vettedPackages
              .flatMap { vp =>
                Seq(vp.context.storeId)
                  .collect { case TopologyStoreId.Synchronizer(synchronizerId) => synchronizerId }
                  .flatMap(synchronizerId =>
                    vp.item.packages
                      .map(_.packageId -> synchronizerId.bimap(identity, _.logical).merge)
                  )
              }
              .groupBy { case (packageId, _) => packageId }
              .map { case (packageId, values) =>
                (packageId: String, values.map { case (_, synchronizerId) => synchronizerId })
              }
            val actualDars = dars
              .filterNot(dar => AdminWorkflowServices.AdminWorkflowNames.contains(dar.name))
              .map(_.mainPackageId)
              .flatMap(pkgId =>
                packageToSynchronizers
                  .getOrElse(pkgId, Seq.empty)
                  .map(synchronizerId => pkgId -> synchronizerId)
              )
              .map((_, "<ignored string>"))
            actualDars
          }

        def getDar(
            mainPkgAndSynchronizerId: (String, SynchronizerId)
        ): Either[String, Option[String]] = mainPkgAndSynchronizerId match {
          case (mainPkgId, synchronizerId) =>
            // check that package is vetted
            fetchVettedPackages(store = Some(TopologyStoreId.Synchronizer(synchronizerId)))
              .map { res =>
                res
                  .find(_.item.packages.exists(_.packageId == mainPkgId))
                  .map(_ => "<ignored string>")
              }
              .flatMap {
                case None => Right(None)
                case Some(_) =>
                  // and verify that dar exists
                  queryAdminApiIfExists(
                    ParticipantAdminCommands.Package.GetDarContents(mainPackageId = mainPkgId)
                  ).map(_.map(_ => "<ignored string>"))
              }
        }

        run[(String, SynchronizerId), String](
          "dars",
          removeExcess = false,
          checkSelfConsistency,
          want = want,
          fetch = fetchDars,
          get = getDar,
          add = { case ((_hash, synchronizerId), file) =>
            queryAdminApi(
              ParticipantAdminCommands.Package.UploadDar(
                darPath = file,
                synchronizerId = Some(synchronizerId),
                vetAllPackages = true,
                synchronizeVetting = true,
                description = s"Uploaded by declarative API: $file",
                expectedMainPackageId = "",
                requestHeaders = Map.empty,
                logger,
              )
            ).map(_ => ())
          },
          upd = { case ((hash, synchronizerId), desired, existing) =>
            Either.unit
          },
          rm = (_, _) => Either.unit, // not implemented in canton yet
          onlyCheckKeys = true,
        )
      }
    }

}

object DeclarativeParticipantApi {

  private sealed trait QueryResult {
    def str: String
  }
  private final case class Err(str: String) extends QueryResult
  private final case class NotFound(str: String) extends QueryResult
}
