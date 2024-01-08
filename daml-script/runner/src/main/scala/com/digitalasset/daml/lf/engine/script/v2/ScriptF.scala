// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v2

import java.time.Clock
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.FrontStack
import com.daml.lf.{CompiledPackages, command}
import com.daml.lf.data.Ref.{
  Identifier,
  Name,
  PackageId,
  PackageName,
  PackageVersion,
  Party,
  UserId,
}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.engine.script.v2.ledgerinteraction.{ScriptLedgerClient, SubmitError}
import com.daml.lf.language.{Ast, LanguageVersion, StablePackagesV2}
import com.daml.lf.speedy.{ArrayList, SError, SValue}
import com.daml.lf.speedy.SBuiltin.SBVariantCon
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import scalaz.{Foldable, OneAnd}
import scalaz.syntax.traverse._
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import com.daml.script.converter.Converter.{toContractId, toText}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.speedy.SBuiltin.SBToAny

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ScriptF {
  val left = SEBuiltin(
    SBVariantCon(StablePackagesV2.Either, Name.assertFromString("Left"), 0)
  )
  val right = SEBuiltin(
    SBVariantCon(StablePackagesV2.Either, Name.assertFromString("Right"), 1)
  )

  sealed trait Cmd {
    private[lf] def executeWithRunner(env: Env, @annotation.unused runner: v2.Runner)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = execute(env)

    private[lf] def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr]
  }
  // The environment that the `execute` function gets access to.
  final class Env(
      val scriptIds: ScriptIds,
      val timeMode: ScriptTimeMode,
      private var _clients: Participants[ScriptLedgerClient],
      compiledPackages: CompiledPackages,
  ) {
    def clients = _clients
    val valueTranslator = new ValueTranslator(
      pkgInterface = compiledPackages.pkgInterface,
      requireV1ContractIdSuffix = false,
    )
    val utcClock = Clock.systemUTC()
    def addPartyParticipantMapping(party: Party, participant: Participant) = {
      _clients =
        _clients.copy(party_participants = _clients.party_participants + (party -> participant))
    }
    def lookupChoice(
        tmplId: Identifier,
        ifaceId: Option[Identifier],
        choice: Name,
    ): Either[String, Ast.TemplateChoiceSignature] =
      compiledPackages.pkgInterface.lookupChoice(tmplId, ifaceId, choice).left.map(_.pretty)

    def lookupKeyTy(id: Identifier): Either[String, Ast.Type] =
      compiledPackages.pkgInterface.lookupTemplateKey(id) match {
        case Right(key) => Right(key.typ)
        case Left(err) => Left(err.pretty)
      }

    def lookupInterfaceViewTy(id: Identifier): Either[String, Ast.Type] =
      compiledPackages.pkgInterface.lookupInterface(id) match {
        case Right(key) => Right(key.view)
        case Left(err) => Left(err.pretty)
      }

    def translateValue(ty: Ast.Type, value: Value): Either[String, SValue] =
      valueTranslator.strictTranslateValue(ty, value).left.map(_.toString)

    def lookupLanguageVersion(packageId: PackageId): Either[String, LanguageVersion] = {
      compiledPackages.pkgInterface.lookupPackageLanguageVersion(packageId) match {
        case Right(lv) => Right(lv)
        case Left(err) => Left(err.pretty)
      }
    }

  }

  final case class Throw(exc: SAny) extends Cmd {
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      Future.failed(
        free.InterpretationError(
          SError
            .SErrorDamlException(IE.UnhandledException(exc.ty, exc.value.toUnnormalizedValue))
        )
      )
  }

  final case class Catch(act: SValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      runner.run(SEAppAtomic(SEValue(act), Array(SEValue(SUnit)))).transformWith {
        case Success(v) =>
          Future.successful(SEAppAtomic(right, Array(SEValue(v))))
        case Failure(
              free.InterpretationError(
                SError.SErrorDamlException(IE.UnhandledException(typ, value))
              )
            ) =>
          Future.successful(
            SELet1(
              // Consider using env.translateValue to reduce what we're building here
              SEImportValue(typ, value),
              SELet1(
                SEAppAtomic(SEBuiltin(SBToAny(typ)), Array(SELocS(1))),
                SEAppAtomic(left, Array(SELocS(1))),
              ),
            )
          )
        case Failure(e) => Future.failed(e)
      }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future.failed(new NotImplementedError)
  }

  final case class TrySubmitConcurrently(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmdss: List[List[command.ApiCommand]],
      stackTrace: StackTrace,
  ) extends Cmd {
    override def execute(
        env: Env
    )(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(
          env.clients.getPartiesParticipant(actAs)
        )
        resItems <- client
          .trySubmitConcurrently(
            actAs,
            readAs,
            cmdss,
            stackTrace.topFrame,
            env.lookupLanguageVersion,
          )
        res <- Converter.toFuture(
          Converter
            .fromSubmitResultList[SubmitError](
              env.lookupChoice,
              env.valueTranslator,
              _.toDamlSubmitError(env),
              env.scriptIds,
              resItems,
              client.enableContractUpgrading,
            )
        )
      } yield SEValue(res)
  }

  final case class TrySubmit(data: SubmitData) extends Cmd {

    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.trySubmit(
          data.actAs,
          data.readAs,
          data.disclosures,
          data.cmds,
          data.stackTrace.topFrame,
          env.lookupLanguageVersion,
        )
        res <- Converter.toFuture(
          Converter
            .fromSubmitResult[SubmitError](
              env.lookupChoice,
              env.valueTranslator,
              _.toDamlSubmitError(env),
              env.scriptIds,
              submitRes,
              client.enableContractUpgrading,
            )
        )
      } yield SEValue(res)
  }

  final case class Submit(data: SubmitData) extends Cmd {
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submit(
          data.actAs,
          data.readAs,
          data.disclosures,
          data.cmds,
          data.stackTrace.topFrame,
        )
        v <- submitRes match {
          case Right(results) =>
            Converter.toFuture(
              results
                .to(FrontStack)
                .traverse(
                  Converter
                    .fromCommandResult(
                      env.lookupChoice,
                      env.valueTranslator,
                      env.scriptIds,
                      _,
                      client.enableContractUpgrading,
                    )
                )
                .map(results => SEValue(SList(results)))
            )
          case Left(statusEx) => Future.failed(statusEx)
        }
      } yield v
  }

  final case class SubmitMustFail(data: SubmitData) extends Cmd {
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submitMustFail(
          data.actAs,
          data.readAs,
          data.disclosures,
          data.cmds,
          data.stackTrace.topFrame,
        )
        v <- submitRes match {
          case Right(()) =>
            Future.successful(SEValue(SUnit))
          case Left(()) =>
            Future.failed(
              SError.SErrorDamlException(
                interpretation.Error.UserError("Expected submit to fail but it succeeded")
              )
            )
        }
      } yield v

  }
  final case class SubmitTree(data: SubmitData) extends Cmd {
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        _ <- Converter.noDisclosures(data.disclosures)
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submitTree(
          data.actAs,
          data.readAs,
          data.disclosures,
          data.cmds,
          data.stackTrace.topFrame,
        )
        res <- Converter.toFuture(
          Converter.translateTransactionTree(
            env.lookupChoice,
            env.valueTranslator,
            env.scriptIds,
            submitRes,
            client.enableContractUpgrading,
          )
        )
      } yield SEValue(res)
  }
  final case class QueryACS(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
  ) extends Cmd {
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(parties)
        )
        acs <- client.query(parties, tplId)
        res <- Converter.toFuture(
          acs
            .to(FrontStack)
            .traverse(
              Converter
                .fromCreated(env.valueTranslator, _, client.enableContractUpgrading)
            )
        )
      } yield SEValue(SList(res))

  }
  final case class QueryContractId(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      cid: ContractId,
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractId(parties, tplId, cid)
        optR <- Converter.toFuture(
          optR.traverse(c =>
            Converter
              .fromContract(env.valueTranslator, c, client.enableContractUpgrading)
              .map(makePair(_, SText(c.blob.toHexString)))
          )
        )
      } yield SEValue(SOptional(optR))
  }

  private[this] def makePair(v1: SValue, v2: SValue): SValue = {
    import com.daml.script.converter.Converter.record
    record(StablePackagesV2.Tuple2, ("_1", v1), ("_2", v2))
  }

  final case class QueryInterface(
      parties: OneAnd[Set, Party],
      interfaceId: Identifier,
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = {

      for {
        viewType <- Converter.toFuture(env.lookupInterfaceViewTy(interfaceId))
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        list <- client.queryInterface(parties, interfaceId, viewType)
        list <- Converter.toFuture(
          list
            .to(FrontStack)
            .traverse { case (cid, optView) =>
              optView match {
                case None =>
                  Right(makePair(SContractId(cid), SOptional(None)))
                case Some(view) =>
                  for {
                    view <- Converter.fromInterfaceView(
                      env.valueTranslator,
                      viewType,
                      view,
                    )
                  } yield {
                    makePair(SContractId(cid), SOptional(Some(view)))
                  }
              }
            }
        )
      } yield SEValue(SList(list))
    }
  }

  final case class QueryInterfaceContractId(
      parties: OneAnd[Set, Party],
      interfaceId: Identifier,
      cid: ContractId,
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = {
      for {
        viewType <- Converter.toFuture(env.lookupInterfaceViewTy(interfaceId))
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryInterfaceContractId(parties, interfaceId, viewType, cid)
        optR <- Converter.toFuture(
          optR.traverse(Converter.fromInterfaceView(env.valueTranslator, viewType, _))
        )
      } yield SEValue(SOptional(optR))
    }
  }

  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
  ) extends Cmd {
    private def translateKey(
        env: Env,
        enableContractUpgrading: Boolean,
    )(id: Identifier, v: Value): Either[String, SValue] =
      for {
        keyTy <- env.lookupKeyTy(id)
        translatorConfig =
          if (enableContractUpgrading) preprocessing.ValueTranslator.Config.Castable
          else preprocessing.ValueTranslator.Config.Strict
        translated <- env.valueTranslator
          .translateValue(keyTy, v, translatorConfig)
          .left
          .map(_.message)
      } yield translated

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractKey(
          parties,
          tplId,
          key.key,
          translateKey(env, client.enableContractUpgrading),
        )
        optR <- Converter.toFuture(
          optR.traverse(
            Converter.fromCreated(env.valueTranslator, _, client.enableContractUpgrading)
          )
        )
      } yield SEValue(SOptional(optR))
  }
  final case class AllocParty(
      displayName: String,
      idHint: String,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- env.clients.getParticipant(participant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        party <- client.allocateParty(idHint, displayName)

      } yield {
        participant.foreach(env.addPartyParticipantMapping(party, _))
        SEValue(SParty(party))
      }

  }
  final case class ListKnownParties(
      participant: Option[Participant]
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- env.clients.getParticipant(participant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        partyDetails <- client.listKnownParties()
        partyDetails_ <- Converter.toFuture(
          partyDetails
            .traverse(details => Converter.fromPartyDetails(env.scriptIds, details))
        )
      } yield SEValue(SList(partyDetails_.to(FrontStack)))

  }
  final case class GetTime() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        time <- env.timeMode match {
          case ScriptTimeMode.Static => {
            // We don’t parametrize this by participant since this
            // is only useful in static time mode and using the time
            // service with multiple participants is very dodgy.
            for {
              client <- Converter.toFuture(env.clients.getParticipant(None))
              t <- client.getStaticTime()
            } yield t
          }
          case ScriptTimeMode.WallClock =>
            Future {
              Timestamp.assertFromInstant(env.utcClock.instant())
            }
        }
      } yield SEValue(STimestamp(time))

  }
  final case class SetTime(time: Timestamp) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      env.timeMode match {
        case ScriptTimeMode.Static =>
          for {
            // We don’t parametrize this by participant since this
            // is only useful in static time mode and using the time
            // service with multiple participants is very dodgy.
            client <- Converter.toFuture(env.clients.getParticipant(None))
            _ <- client.setStaticTime(time)
          } yield SEValue(SUnit)
        case ScriptTimeMode.WallClock =>
          Future.failed(
            new RuntimeException("setTime is not supported in wallclock mode")
          )

      }
  }

  final case class Sleep(micros: Long) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future {
      sleepAtLeast(micros * 1000)
      SEValue(SUnit)
    }

    private def sleepAtLeast(totalNanos: Long) = {
      // Thread.sleep can wake up earlier so we loop it to guarantee a minimum
      // sleep time
      val t0 = System.nanoTime
      var nanosLeft = totalNanos
      while (nanosLeft > 0) {
        java.util.concurrent.TimeUnit.NANOSECONDS.sleep(nanosLeft)
        val t1 = System.nanoTime
        nanosLeft = totalNanos - (t1 - t0)
      }
    }
  }

  final case class ValidateUserId(
      userName: String
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = {
      val errorOption =
        UserId.fromString(userName) match {
          case Right(_) => None // valid
          case Left(message) => Some(SText(message)) // invalid; with error message
        }
      Future.successful(SEValue(SOptional(errorOption)))
    }
  }

  final case class CreateUser(
      user: User,
      rights: List[UserRight],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        res <- client.createUser(user, rights)
        res <- Converter.toFuture(
          Converter.fromOptional[Unit](res, _ => Right(SUnit))
        )
      } yield SEValue(res)
  }

  final case class GetUser(
      userId: UserId,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        user <- client.getUser(userId)
        user <- Converter.toFuture(
          Converter.fromOptional(user, Converter.fromUser(env.scriptIds, _))
        )
      } yield SEValue(user)
  }

  final case class DeleteUser(
      userId: UserId,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        res <- client.deleteUser(userId)
        res <- Converter.toFuture(
          Converter.fromOptional[Unit](res, _ => Right(SUnit))
        )
      } yield SEValue(res)
  }

  final case class ListAllUsers(
      participant: Option[Participant]
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        users <- client.listAllUsers()
        users <- Converter.toFuture(
          users.to(FrontStack).traverse(Converter.fromUser(env.scriptIds, _))
        )
      } yield SEValue(SList(users))
  }

  final case class GrantUserRights(
      userId: UserId,
      rights: List[UserRight],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.grantUserRights(userId, rights)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(SList(_)),
          )
        )
      } yield SEValue(rights)
  }

  final case class RevokeUserRights(
      userId: UserId,
      rights: List[UserRight],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.revokeUserRights(userId, rights)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(SList(_)),
          )
        )
      } yield SEValue(rights)
  }

  final case class ListUserRights(
      userId: UserId,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.listUserRights(userId)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(SList(_)),
          )
        )
      } yield SEValue(rights)
  }

  final case class VetPackages(
      packages: List[ScriptLedgerClient.ReadablePackageId]
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        _ <- client.vetPackages(packages)
      } yield SEValue(SUnit)
  }

  final case class UnvetPackages(
      packages: List[ScriptLedgerClient.ReadablePackageId]
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        _ <- client.unvetPackages(packages)
      } yield SEValue(SUnit)
  }

  final case class ListVettedPackages() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        packages <- client.listVettedPackages()
      } yield SEValue(
        SList(packages.to(FrontStack).map(Converter.fromReadablePackageId(env.scriptIds, _)))
      )
  }

  final case class ListAllPackages() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        packages <- client.listAllPackages()
      } yield SEValue(
        SList(packages.to(FrontStack).map(Converter.fromReadablePackageId(env.scriptIds, _)))
      )
  }

  final case class VetDar(
      darName: String,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.vetDar(darName)
      } yield SEValue(SUnit)
  }

  final case class UnvetDar(
      darName: String,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.unvetDar(darName)
      } yield SEValue(SUnit)
  }

  final case class SetProvidePackageId(shouldProvide: Boolean) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        _ <- env.clients.default_participant.fold(Future.unit)(
          _.setProvidePackageId(shouldProvide)
        )
        _ <- Future.traverse(env.clients.participants.toList) { case (_, v) =>
          v.setProvidePackageId(shouldProvide)
        }
      } yield SEValue(SUnit)
  }

  final case class TryCommands(act: SValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      runner.run(SEValue(act)).transformWith {
        case Success(v) =>
          Future.successful(SEAppAtomic(right, Array(SEValue(v))))
        case Failure(
              Script.FailedCmd(cmdName, _, err)
            ) =>
          import com.daml.lf.scenario.{Pretty, Error}
          val msg = err match {
            case e: Error => Pretty.prettyError(e).render(10000)
            case e => e.getMessage
          }

          val name = err match {
            case Error.RunnerException(speedy.SError.SErrorDamlException(iErr)) =>
              iErr.getClass.getSimpleName
            case e => e.getClass.getSimpleName
          }

          import com.daml.script.converter.Converter.record
          Future.successful(
            SEApp(
              left,
              Array(
                record(
                  StablePackagesV2.Tuple3,
                  ("_1", SText(cmdName)),
                  ("_2", SText(name)),
                  ("_3", SText(msg)),
                )
              ),
            )
          )
        case Failure(e) => Future.failed(e)
      }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future.failed(new NotImplementedError)
  }

  // Shared between Submit, SubmitMustFail and SubmitTree
  final case class SubmitData(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmds: List[command.ApiCommand],
      disclosures: List[Disclosure],
      stackTrace: StackTrace,
  )

  final case class Ctx(knownPackages: Map[String, PackageId], compiledPackages: CompiledPackages)

  private def parseSubmit(v: SValue, stackTrace: StackTrace): Either[String, SubmitData] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(_, _, ArrayList(hdAct, SList(tlAct))),
              SList(readAs),
              SList(disclosures),
              SList(cmds),
            ),
          ) =>
        for {
          actAs <- OneAnd(hdAct, tlAct.toList).traverse(Converter.toParty)
          readAs <- readAs.traverse(Converter.toParty)
          cmds <- cmds.toList.traverse(Converter.toCommand)
          disclosures <- disclosures.toImmArray.toList.traverse(Converter.toDisclosure)
        } yield SubmitData(
          actAs = toOneAndSet(actAs),
          readAs = readAs.toSet,
          cmds = cmds,
          disclosures = disclosures,
          stackTrace = stackTrace,
        )
      case _ => Left(s"Expected Submit payload but got $v")
    }

  private def parseSubmitConcurrently(
      v: SValue,
      stackTrace: StackTrace,
  ): Either[String, TrySubmitConcurrently] = {
    def convert(
        actAs: OneAnd[List, SValue],
        readAs: List[SValue],
        cmdss: List[List[SValue]],
    ) =
      for {
        actAs <- actAs.traverse(Converter.toParty(_)).map(toOneAndSet(_))
        readAs <- readAs.traverse(Converter.toParty(_))
        cmdss <- cmdss.traverse(_.traverse(Converter.toCommand(_)))
      } yield TrySubmitConcurrently(actAs, readAs.toSet, cmdss, stackTrace)
    v match {
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(_, _, ArrayList(hdAct, SList(tlAct))),
              SList(read),
              cmdss,
            ),
          ) =>
        Converter.toList(cmdss, (Converter.toList(_, Right(_)))) match {
          case Right(cmdss) => convert(OneAnd(hdAct, tlAct.toList), read.toList, cmdss)
          case _ => Left(s"Expected TrySubmitConcurrently payload but got $v")
        }

      case _ => Left(s"Expected TrySubmitConcurrently payload but got $v")
    }
  }

  private def parseQueryACS(v: SValue): Either[String, QueryACS] =
    v match {
      case SRecord(_, _, ArrayList(readAs, tplId)) =>
        for {
          readAs <- Converter.toParties(readAs)
          tplId <- Converter
            .typeRepToIdentifier(tplId)
        } yield QueryACS(readAs, tplId)
      case _ => Left(s"Expected QueryACS payload but got $v")
    }

  private def parseQueryContractId(v: SValue): Either[String, QueryContractId] =
    v match {
      case SRecord(_, _, ArrayList(actAs, tplId, cid)) =>
        for {
          actAs <- Converter.toParties(actAs)
          tplId <- Converter.typeRepToIdentifier(tplId)
          cid <- toContractId(cid)
        } yield QueryContractId(actAs, tplId, cid)
      case _ => Left(s"Expected QueryContractId payload but got $v")
    }

  private def parseQueryInterface(v: SValue): Either[String, QueryInterface] =
    v match {
      case SRecord(_, _, ArrayList(actAs, interfaceId)) =>
        for {
          actAs <- Converter.toParties(actAs)
          interfaceId <- Converter.typeRepToIdentifier(interfaceId)
        } yield QueryInterface(actAs, interfaceId)
      case _ => Left(s"Expected QueryInterface payload but got $v")
    }

  private def parseQueryInterfaceContractId(v: SValue): Either[String, QueryInterfaceContractId] =
    v match {
      case SRecord(_, _, ArrayList(actAs, interfaceId, cid)) =>
        for {
          actAs <- Converter.toParties(actAs)
          interfaceId <- Converter.typeRepToIdentifier(interfaceId)
          cid <- toContractId(cid)
        } yield QueryInterfaceContractId(actAs, interfaceId, cid)
      case _ => Left(s"Expected QueryInterfaceContractId payload but got $v")
    }

  private def parseQueryContractKey(v: SValue): Either[String, QueryContractKey] =
    v match {
      case SRecord(_, _, ArrayList(actAs, tplId, key)) =>
        for {
          actAs <- Converter.toParties(actAs)
          tplId <- Converter.typeRepToIdentifier(tplId)
          key <- Converter.toAnyContractKey(key)
        } yield QueryContractKey(actAs, tplId, key)
      case _ => Left(s"Expected QueryContractKey payload but got $v")
    }

  private def parseAllocParty(v: SValue): Either[String, AllocParty] =
    v match {
      case SRecord(_, _, ArrayList(SText(displayName), SText(idHint), participantName)) =>
        for {
          participantName <- Converter.toParticipantName(participantName)
        } yield AllocParty(displayName, idHint, participantName)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }

  private def parseListKnownParties(v: SValue): Either[String, ListKnownParties] =
    v match {
      case SRecord(_, _, ArrayList(participantName)) =>
        for {
          participantName <- Converter.toParticipantName(participantName)
        } yield ListKnownParties(participantName)
      case _ => Left(s"Expected ListKnownParties payload but got $v")
    }

  private def parseEmpty[A](result: A)(v: SValue): Either[String, A] =
    v match {
      case SRecord(_, _, ArrayList()) => Right(result)
      case _ => Left(s"Expected ${result.getClass.getSimpleName} payload but got $v")
    }

  private def parseSetTime(v: SValue): Either[String, SetTime] =
    v match {
      case SRecord(_, _, ArrayList(time)) =>
        for {
          time <- Converter.toTimestamp(time)
        } yield SetTime(time)
      case _ => Left(s"Expected SetTime payload but got $v")
    }

  private def parseSleep(v: SValue): Either[String, Sleep] =
    v match {
      case SRecord(_, _, ArrayList(SRecord(_, _, ArrayList(SInt64(micros))))) =>
        Right(Sleep(micros))
      case _ => Left(s"Expected Sleep payload but got $v")
    }

  private def parseCatch(v: SValue): Either[String, Catch] =
    v match {
      // Catch includes a dummy field for old style typeclass LF encoding, we ignore it here.
      case SRecord(_, _, ArrayList(act, _)) => Right(Catch(act))
      case _ => Left(s"Expected Catch payload but got $v")
    }

  private def parseThrow(v: SValue): Either[String, Throw] =
    v match {
      case SRecord(_, _, ArrayList(exc: SAny)) => Right(Throw(exc))
      case _ => Left(s"Expected Throw payload but got $v")
    }

  private def parseValidateUserId(v: SValue): Either[String, ValidateUserId] =
    v match {
      case SRecord(_, _, ArrayList(userName)) =>
        for {
          userName <- toText(userName)
        } yield ValidateUserId(userName)
      case _ => Left(s"Expected ValidateUserId payload but got $v")
    }

  private def parseCreateUser(v: SValue): Either[String, CreateUser] =
    v match {
      case SRecord(_, _, ArrayList(user, rights, participant)) =>
        for {
          user <- Converter.toUser(user)
          participant <- Converter.toParticipantName(participant)
          rights <- Converter.toList(rights, Converter.toUserRight)
        } yield CreateUser(user, rights, participant)
      case _ => Left(s"Exected CreateUser payload but got $v")
    }

  private def parseGetUser(v: SValue): Either[String, GetUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
        } yield GetUser(userId, participant)
      case _ => Left(s"Expected GetUser payload but got $v")
    }

  private def parseDeleteUser(v: SValue): Either[String, DeleteUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
        } yield DeleteUser(userId, participant)
      case _ => Left(s"Expected DeleteUser payload but got $v")
    }

  private def parseListAllUsers(v: SValue): Either[String, ListAllUsers] =
    v match {
      case SRecord(_, _, ArrayList(participant)) =>
        for {
          participant <- Converter.toParticipantName(participant)
        } yield ListAllUsers(participant)
      case _ => Left(s"Expected ListAllUsers payload but got $v")
    }

  private def parseGrantUserRights(v: SValue): Either[String, GrantUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toParticipantName(participant)
        } yield GrantUserRights(userId, rights, participant)
      case _ => Left(s"Expected GrantUserRights payload but got $v")
    }

  private def parseRevokeUserRights(v: SValue): Either[String, RevokeUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toParticipantName(participant)
        } yield RevokeUserRights(userId, rights, participant)
      case _ => Left(s"Expected RevokeUserRights payload but got $v")
    }

  private def parseListUserRights(v: SValue): Either[String, ListUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
        } yield ListUserRights(userId, participant)
      case _ => Left(s"Expected ListUserRights payload but got $v")
    }

  private def parseChangePackages(
      v: SValue
  ): Either[String, List[ScriptLedgerClient.ReadablePackageId]] = {
    def toReadablePackageId(s: SValue): Either[String, ScriptLedgerClient.ReadablePackageId] =
      s match {
        case SRecord(_, _, ArrayList(SText(name), SText(version))) =>
          for {
            pname <- PackageName.fromString(name)
            pversion <- PackageVersion.fromString(version)
          } yield ScriptLedgerClient.ReadablePackageId(pname, pversion)
        case _ => Left(s"Expected PackageName but got $s")
      }
    v match {
      case SRecord(_, _, ArrayList(packages)) =>
        Converter.toList(packages, toReadablePackageId)
      case _ => Left(s"Expected Packages payload but got $v")
    }
  }

  private def parseDarVettingChange[A](
      v: SValue,
      wrap: (String, Option[Participant]) => A,
  ): Either[String, A] =
    v match {
      case SRecord(_, _, ArrayList(SText(name), participant)) =>
        for {
          participant <- Converter.toParticipantName(participant)
        } yield wrap(name, participant)
      case _ => Left(s"Expected VetDar payload but got $v")
    }

  private def parseSetProvidePackageId(
      v: SValue
  ): Either[String, SetProvidePackageId] =
    v match {
      case SRecord(_, _, ArrayList(SBool(enabled))) =>
        Right(SetProvidePackageId(enabled))
      case _ => Left(s"Expected SetProvidePackageId payload but got $v")
    }

  private def parseTryCommands(v: SValue): Either[String, TryCommands] =
    v match {
      case SRecord(_, _, ArrayList(act)) => Right(TryCommands(act))
      case _ => Left(s"Expected TryCommands payload but got $v")
    }

  def parse(
      commandName: String,
      version: Long,
      v: SValue,
      stackTrace: StackTrace,
  ): Either[String, Cmd] =
    (commandName, version) match {
      case ("Submit", 1) => parseSubmit(v, stackTrace).map(Submit(_))
      case ("SubmitMustFail", 1) => parseSubmit(v, stackTrace).map(SubmitMustFail(_))
      case ("SubmitTree", 1) => parseSubmit(v, stackTrace).map(SubmitTree(_))
      case ("TrySubmit", 1) => parseSubmit(v, stackTrace).map(TrySubmit(_))
      case ("TrySubmitConcurrently", 1) => parseSubmitConcurrently(v, stackTrace)
      case ("QueryACS", 1) => parseQueryACS(v)
      case ("QueryContractId", 1) => parseQueryContractId(v)
      case ("QueryInterface", 1) => parseQueryInterface(v)
      case ("QueryInterfaceContractId", 1) => parseQueryInterfaceContractId(v)
      case ("QueryContractKey", 1) => parseQueryContractKey(v)
      case ("AllocateParty", 1) => parseAllocParty(v)
      case ("ListKnownParties", 1) => parseListKnownParties(v)
      case ("GetTime", 1) => parseEmpty(GetTime())(v)
      case ("SetTime", 1) => parseSetTime(v)
      case ("Sleep", 1) => parseSleep(v)
      case ("Catch", 1) => parseCatch(v)
      case ("Throw", 1) => parseThrow(v)
      case ("ValidateUserId", 1) => parseValidateUserId(v)
      case ("CreateUser", 1) => parseCreateUser(v)
      case ("GetUser", 1) => parseGetUser(v)
      case ("DeleteUser", 1) => parseDeleteUser(v)
      case ("ListAllUsers", 1) => parseListAllUsers(v)
      case ("GrantUserRights", 1) => parseGrantUserRights(v)
      case ("RevokeUserRights", 1) => parseRevokeUserRights(v)
      case ("ListUserRights", 1) => parseListUserRights(v)
      case ("VetPackages", 1) => parseChangePackages(v).map(VetPackages)
      case ("UnvetPackages", 1) => parseChangePackages(v).map(UnvetPackages)
      case ("ListVettedPackages", 1) => parseEmpty(ListVettedPackages())(v)
      case ("ListAllPackages", 1) => parseEmpty(ListAllPackages())(v)
      case ("VetDar", 1) => parseDarVettingChange(v, VetDar)
      case ("UnvetDar", 1) => parseDarVettingChange(v, UnvetDar)
      case ("SetProvidePackageId", 1) => parseSetProvidePackageId(v)
      case ("TryCommands", 1) => parseTryCommands(v)
      case _ => Left(s"Unknown command $commandName - Version $version")
    }

  // TODO: Update SetProvidePackageId to `SetProvidePackageId`, which should only change whether the packageID is given, so for translation of commands and query template filter
  // Translation of results is always enabled if the flag is

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
