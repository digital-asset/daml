// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v1

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SExpr.{SEAppAtomic, SEValue, SExpr}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy.PureMachine
import com.daml.lf.speedy.{ArrayList, SError, SValue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.lf.{CompiledPackages, command}
import com.daml.script.converter.Converter.{toContractId, toText}
import org.apache.pekko.stream.Materializer
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.{Foldable, OneAnd}

import java.time.Clock
import scala.concurrent.{ExecutionContext, Future}

sealed trait ScriptF

object ScriptF {

  final case class Catch(act: SValue, handle: SValue, continue: SValue) extends ScriptF
  final case class Throw(exc: SAny) extends ScriptF

  sealed trait Cmd extends ScriptF with Script.FailableCmd {
    def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr]
  }
  // The environment that the `execute` function gets access to.
  final class Env(
      val scriptIds: ScriptIds,
      val timeMode: ScriptTimeMode,
      private var _clients: Participants[v1.ledgerinteraction.ScriptLedgerClient],
      machine: PureMachine,
  ) {
    def clients = _clients
    def compiledPackages = machine.compiledPackages
    val valueTranslator = new ValueTranslator(
      pkgInterface = compiledPackages.pkgInterface,
      checkV1ContractIdSuffixes = false,
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
  }
  final case class Submit(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace

    override def description = "submit"

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
              Converter
                .fillCommandResults(
                  env.compiledPackages,
                  env.lookupChoice,
                  env.valueTranslator,
                  data.freeAp,
                  results,
                )
            )
          case Left(statusEx) =>
            // This branch is superseded by SubmitMustFail below,
            // however, it is maintained for backwards
            // compatibility with Daml script DARs generated by
            // older SDK versions that didn't distinguish Submit
            // and SubmitMustFail.
            data.continue match {
              // Separated submit and submitMustFail, fail in Scala land
              // instead of going back to Daml.
              case SUnit => Future.failed(statusEx)
              // Fail in Daml land
              case _ =>
                for {
                  res <- Converter.toFuture(
                    Converter
                      .fromStatusException(env.scriptIds, statusEx)
                  )
                } yield SEAppAtomic(SEValue(data.continue), Array(SEValue(res)))
            }
        }
      } yield v
  }

  final case class SubmitMustFail(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace

    override def description = "submitMustFail"

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
            Future.successful(SEAppAtomic(SEValue(data.continue), Array(SEValue(SUnit))))
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
    override def stackTrace = data.stackTrace
    override def description = "submitTree"
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submitTree(
          data.actAs,
          data.readAs,
          data.cmds,
          data.stackTrace.topFrame,
        )
        res <- Converter.toFuture(
          Converter.translateTransactionTree(
            env.lookupChoice,
            env.valueTranslator,
            env.scriptIds,
            submitRes,
          )
        )
      } yield SEAppAtomic(SEValue(data.continue), Array(SEValue(res)))
  }
  final case class Query(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {

    override def description = "query"

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
                .fromCreated(env.valueTranslator, _)
            )
        )
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SList(res))))

  }

  final case class QueryContractId(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      cid: ContractId,
      stackTrace: StackTrace,
      continue: SValue,
      asDisclosure: Boolean, // Return a disclosure blob instead an AnyTemplate when on
  ) extends Cmd {
    override def description = "queryContractId"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractId(parties, tplId, cid)
        optR <- Converter.toFuture(
          if (asDisclosure)
            Right(
              optR.map(c =>
                Converter.makeTuple(
                  Converter.fromTemplateTypeRep(c.templateId),
                  SValue.SText(c.blob.toHexString),
                )
              )
            )
          else
            optR.traverse(Converter.fromContract(env.valueTranslator, _))
        )
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SOptional(optR))))
  }

  final case class QueryInterface(
      parties: OneAnd[Set, Party],
      interfaceId: Identifier,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "queryInterface"

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
                  Right(Converter.makeTuple(SContractId(cid), SOptional(None)))
                case Some(view) =>
                  for {
                    view <- Converter.fromInterfaceView(
                      env.valueTranslator,
                      interfaceId,
                      viewType,
                      view,
                    )
                  } yield {
                    Converter.makeTuple(SContractId(cid), SOptional(Some(view)))
                  }
              }
            }
        )
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SList(list))))
    }
  }

  final case class QueryInterfaceContractId(
      parties: OneAnd[Set, Party],
      interfaceId: Identifier,
      cid: ContractId,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "queryInterfaceContractId"

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
          optR.traverse(Converter.fromInterfaceView(env.valueTranslator, interfaceId, viewType, _))
        )
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SOptional(optR))))
    }
  }

  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "queryContractKey"

    private def translateKey(
        env: Env
    )(id: Identifier, v: Value): Either[String, SValue] =
      for {
        keyTy <- env.lookupKeyTy(id)
        isUpgradable = Converter.upgradable(env.valueTranslator, tplId)
        translated <- env.valueTranslator.translateValue(keyTy, isUpgradable, v).left.map(_.message)
      } yield translated

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractKey(parties, tplId, key.key, translateKey(env))
        optR <- Converter.toFuture(
          optR.traverse(Converter.fromCreated(env.valueTranslator, _))
        )
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SOptional(optR))))
  }
  final case class AllocParty(
      displayName: String,
      idHint: String,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "allocateParty"

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
        SEAppAtomic(SEValue(continue), Array(SEValue(SParty(party))))
      }

  }
  final case class ListKnownParties(
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "listKnownParties"

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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SList(partyDetails_.to(FrontStack)))))

  }
  final case class GetTime(stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "getTime"

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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(STimestamp(time))))

  }
  final case class SetTime(time: Timestamp, stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "setTime"

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
          } yield SEAppAtomic(SEValue(continue), Array(SEValue(SUnit)))
        case ScriptTimeMode.WallClock =>
          Future.failed(
            new RuntimeException("setTime is not supported in wallclock mode")
          )

      }
  }

  final case class Sleep(micros: Long, stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "sleep"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future {
      sleepAtLeast(micros * 1000)
      SEAppAtomic(SEValue(continue), Array(SEValue(SUnit)))
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
      userName: String,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "ValidateUserId"
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
      Future.successful(SEAppAtomic(SEValue(continue), Array(SEValue(SOptional(errorOption)))))
    }
  }

  final case class CreateUser(
      user: User,
      rights: List[UserRight],
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "createUser"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(res)))
  }

  final case class GetUser(
      userId: UserId,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "getUser"
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        user <- client.getUser(userId)
        userValue <- Converter.toFuture(
          Converter.fromOptional(user, Converter.fromUser(env.scriptIds, _))
        )
      } yield {
        (participant, user.flatMap(_.primaryParty)) match {
          case (Some(participant), Some(party)) =>
            env.addPartyParticipantMapping(party, participant)
          case _ =>
        }
        SEAppAtomic(SEValue(continue), Array(SEValue(userValue)))
      }
  }

  final case class DeleteUser(
      userId: UserId,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "deleteUser"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(res)))
  }

  final case class ListAllUsers(
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "listAllUsers"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(SList(users))))
  }

  final case class GrantUserRights(
      userId: UserId,
      rights: List[UserRight],
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "grantUserRights"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(rights)))
  }

  final case class RevokeUserRights(
      userId: UserId,
      rights: List[UserRight],
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "revokeUserRights"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(rights)))
  }

  final case class ListUserRights(
      userId: UserId,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "listUserRights"
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
      } yield SEAppAtomic(SEValue(continue), Array(SEValue(rights)))
  }

  // Shared between Submit, SubmitMustFail and SubmitTree
  final case class SubmitData(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      disclosures: List[Disclosure],
      cmds: List[command.ApiCommand],
      freeAp: SValue,
      stackTrace: StackTrace,
      continue: SValue,
  )

  final case class Ctx(knownPackages: Map[String, PackageId], compiledPackages: CompiledPackages)

  private def toStackTrace(ctx: Ctx, stackTrace: Option[SValue]): Either[String, StackTrace] =
    stackTrace match {
      case None => Right(StackTrace.Empty)
      case Some(stackTrace) => Converter.toStackTrace(ctx.knownPackages, stackTrace)
    }

  private def parseSubmit(
      ctx: Ctx,
      v: SValue,
  ): Either[String, SubmitData] = {
    def convert(
        actAs: OneAnd[List, SValue],
        readAs: List[SValue],
        freeAp: SValue,
        continue: SValue,
        stackTrace: Option[SValue],
        disclosures: List[SValue],
    ) =
      for {
        actAs <- actAs.traverse(Converter.toParty(_)).map(toOneAndSet(_))
        readAs <- readAs.traverse(Converter.toParty(_))
        cmds <- Converter.toCommands(ctx.compiledPackages, freeAp)
        stackTrace <- toStackTrace(ctx, stackTrace)
        disclosures <- disclosures.traverse(Converter.toDisclosure(_))
      } yield SubmitData(actAs, readAs.toSet, disclosures, cmds, freeAp, stackTrace, continue)
    v match {
      // no location
      case SRecord(_, _, ArrayList(sParty, SRecord(_, _, ArrayList(freeAp)), continue)) =>
        convert(OneAnd(sParty, List()), List(), freeAp, continue, None, List.empty)
      // location
      case SRecord(_, _, ArrayList(sParty, SRecord(_, _, ArrayList(freeAp)), continue, loc)) =>
        convert(OneAnd(sParty, List()), List(), freeAp, continue, Some(loc), List.empty)
      // multi-party actAs/readAs + location
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(_, _, ArrayList(hdAct, SList(tlAct))),
              SList(read),
              SRecord(_, _, ArrayList(freeAp)),
              continue,
              loc,
            ),
          ) =>
        convert(OneAnd(hdAct, tlAct.toList), read.toList, freeAp, continue, Some(loc), List.empty)
      // multi-party actAs/readAs + location + disclosures
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(_, _, ArrayList(hdAct, SList(tlAct))),
              SList(read),
              SRecord(_, _, ArrayList(freeAp)),
              continue,
              loc,
              SList(disclosures),
            ),
          ) =>
        convert(
          OneAnd(hdAct, tlAct.toList),
          read.toList,
          freeAp,
          continue,
          Some(loc),
          disclosures.toList,
        )
      case _ => Left(s"Expected Submit payload but got $v")
    }
  }

  private def parseQuery(ctx: Ctx, v: SValue): Either[String, Query] = {
    def convert(readAs: SValue, tplId: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        readAs <- Converter.toParties(readAs)
        tplId <- Converter
          .typeRepToIdentifier(tplId)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield Query(readAs, tplId, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(actAs, tplId, continue)) =>
        convert(actAs, tplId, None, continue)
      case SRecord(_, _, ArrayList(actAs, tplId, continue, stackTrace)) =>
        convert(actAs, tplId, Some(stackTrace), continue)
      case _ => Left(s"Expected Query payload but got $v")
    }
  }

  private def parseQueryContractId(
      ctx: Ctx,
      v: SValue,
      asDisclosure: Boolean,
  ): Either[String, QueryContractId] = {
    def convert(
        actAs: SValue,
        tplId: SValue,
        cid: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        cid <- toContractId(cid)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryContractId(actAs, tplId, cid, stackTrace, continue, asDisclosure)
    v match {
      case SRecord(_, _, ArrayList(actAs, tplId, cid, continue)) =>
        convert(actAs, tplId, cid, None, continue)
      case SRecord(_, _, ArrayList(actAs, tplId, cid, continue, stackTrace)) =>
        convert(actAs, tplId, cid, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryContractId payload but got $v")
    }
  }

  private def parseQueryInterface(
      ctx: Ctx,
      v: SValue,
  ): Either[String, QueryInterface] = {
    def convert(
        actAs: SValue,
        interfaceId: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        interfaceId <- Converter.typeRepToIdentifier(interfaceId)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryInterface(actAs, interfaceId, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(actAs, interfaceId, continue, stackTrace)) =>
        convert(actAs, interfaceId, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryInterface payload but got $v")
    }
  }

  private def parseQueryInterfaceContractId(
      ctx: Ctx,
      v: SValue,
  ): Either[String, QueryInterfaceContractId] = {
    def convert(
        actAs: SValue,
        interfaceId: SValue,
        cid: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        interfaceId <- Converter.typeRepToIdentifier(interfaceId)
        cid <- toContractId(cid)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryInterfaceContractId(actAs, interfaceId, cid, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(actAs, interfaceId, cid, continue, stackTrace)) =>
        convert(actAs, interfaceId, cid, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryInterfaceContractId payload but got $v")
    }
  }

  private def parseQueryContractKey(ctx: Ctx, v: SValue): Either[String, QueryContractKey] = {
    def convert(
        actAs: SValue,
        tplId: SValue,
        key: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        key <- Converter.toAnyContractKey(key)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryContractKey(actAs, tplId, key, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(actAs, tplId, key, continue)) =>
        convert(actAs, tplId, key, None, continue)
      case SRecord(_, _, ArrayList(actAs, tplId, key, continue, stackTrace)) =>
        convert(actAs, tplId, key, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryContractKey payload but got $v")
    }
  }

  private def parseAllocParty(ctx: Ctx, v: SValue): Either[String, AllocParty] = {
    def convert(
        displayName: String,
        idHint: String,
        participantName: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        participantName <- Converter.toParticipantName(participantName)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield AllocParty(displayName, idHint, participantName, stackTrace, continue)
    v match {
      case SRecord(
            _,
            _,
            ArrayList(
              SText(displayName),
              SText(idHint),
              participantName,
              continue,
            ),
          ) =>
        convert(displayName, idHint, participantName, None, continue)
      case SRecord(
            _,
            _,
            ArrayList(
              SText(displayName),
              SText(idHint),
              participantName,
              continue,
              stackTrace,
            ),
          ) =>
        convert(displayName, idHint, participantName, Some(stackTrace), continue)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }
  }

  private def parseListKnownParties(ctx: Ctx, v: SValue): Either[String, ListKnownParties] = {
    def convert(participantName: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        participantName <- Converter.toParticipantName(participantName)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield ListKnownParties(participantName, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(participantName, continue)) =>
        convert(participantName, None, continue)
      case SRecord(_, _, ArrayList(participantName, continue, stackTrace)) =>
        convert(participantName, Some(stackTrace), continue)
      case _ => Left(s"Expected ListKnownParties payload but got $v")
    }
  }

  private def parseGetTime(ctx: Ctx, v: SValue): Either[String, GetTime] = {
    def convert(stackTrace: Option[SValue], continue: SValue) =
      for {
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield GetTime(stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(continue, stackTrace)) => convert(Some(stackTrace), continue)
      case _ => convert(None, v)
    }
  }

  private def parseSetTime(ctx: Ctx, v: SValue): Either[String, SetTime] = {
    def convert(time: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        time <- Converter.toTimestamp(time)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield SetTime(time, stackTrace, continue)
    v match {
      case SRecord(_, _, ArrayList(time, continue)) => convert(time, None, continue)
      case SRecord(_, _, ArrayList(time, continue, stackTrace)) =>
        convert(time, Some(stackTrace), continue)
      case _ => Left(s"Expected SetTime payload but got $v")
    }
  }

  private def parseSleep(ctx: Ctx, v: SValue): Either[String, Sleep] = {
    def convert(micros: Long, stackTrace: Option[SValue], continue: SValue) = {
      for {
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield Sleep(micros, stackTrace, continue)
    }

    v match {
      case SRecord(_, _, ArrayList(SRecord(_, _, ArrayList(SInt64(micros))), continue)) =>
        convert(micros, None, continue)
      case SRecord(
            _,
            _,
            ArrayList(SRecord(_, _, ArrayList(SInt64(micros))), continue, stackTrace),
          ) =>
        convert(micros, Some(stackTrace), continue)
      case _ => Left(s"Expected Sleep payload but got $v")
    }

  }

  private def parseCatch(v: SValue): Either[String, Catch] = {
    v match {
      case SRecord(_, _, ArrayList(act, handle, continue)) =>
        Right(Catch(act, handle, continue))
      case _ => Left(s"Expected Catch payload but got $v")
    }

  }

  private def parseThrow(v: SValue): Either[String, Throw] = {
    v match {
      case SRecord(_, _, ArrayList(exc: SAny)) =>
        Right(Throw(exc))
      case _ => Left(s"Expected Throw payload but got $v")
    }

  }

  private def parseValidateUserId(ctx: Ctx, v: SValue): Either[String, ValidateUserId] =
    v match {
      case SRecord(_, _, ArrayList(userName, continue, stackTrace)) =>
        for {
          userName <- toText(userName)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield ValidateUserId(userName, stackTrace, continue)
      case _ => Left(s"Expected ValidateUserId payload but got $v")
    }

  private def parseCreateUser(ctx: Ctx, v: SValue): Either[String, CreateUser] =
    v match {
      case SRecord(_, _, ArrayList(user, rights, participant, continue, stackTrace)) =>
        for {
          user <- Converter.toUser(user)
          participant <- Converter.toParticipantName(participant)
          rights <- Converter.toList(rights, Converter.toUserRight)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield CreateUser(user, rights, participant, stackTrace, continue)
      case _ => Left(s"Exected CreateUser payload but got $v")
    }

  private def parseGetUser(ctx: Ctx, v: SValue): Either[String, GetUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant, continue, stackTrace)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield GetUser(userId, participant, stackTrace, continue)
      case _ => Left(s"Expected GetUser payload but got $v")
    }

  private def parseDeleteUser(ctx: Ctx, v: SValue): Either[String, DeleteUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant, continue, stackTrace)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield DeleteUser(userId, participant, stackTrace, continue)
      case _ => Left(s"Expected DeleteUser payload but got $v")
    }

  private def parseListAllUsers(ctx: Ctx, v: SValue): Either[String, ListAllUsers] =
    v match {
      case SRecord(_, _, ArrayList(participant, continue, stackTrace)) =>
        for {
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield ListAllUsers(participant, stackTrace, continue)
      case _ => Left(s"Expected ListAllUsers payload but got $v")
    }

  private def parseGrantUserRights(ctx: Ctx, v: SValue): Either[String, GrantUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant, continue, stackTrace)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield GrantUserRights(userId, rights, participant, stackTrace, continue)
      case _ => Left(s"Expected GrantUserRights payload but got $v")
    }

  private def parseRevokeUserRights(ctx: Ctx, v: SValue): Either[String, RevokeUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant, continue, stackTrace)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield RevokeUserRights(userId, rights, participant, stackTrace, continue)
      case _ => Left(s"Expected RevokeUserRights payload but got $v")
    }

  private def parseListUserRights(ctx: Ctx, v: SValue): Either[String, ListUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant, continue, stackTrace)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toParticipantName(participant)
          stackTrace <- toStackTrace(ctx, Some(stackTrace))
        } yield ListUserRights(userId, participant, stackTrace, continue)
      case _ => Left(s"Expected ListUserRights payload but got $v")
    }

  def parse(ctx: Ctx, constr: Ast.VariantConName, v: SValue): Either[String, ScriptF] =
    constr match {
      case "Submit" => parseSubmit(ctx, v).map(Submit(_))
      case "SubmitMustFail" => parseSubmit(ctx, v).map(SubmitMustFail(_))
      case "SubmitTree" => parseSubmit(ctx, v).map(SubmitTree(_))
      case "Query" => parseQuery(ctx, v)
      case "QueryContractId" => parseQueryContractId(ctx, v, asDisclosure = false)
      case "QueryDisclosure" => parseQueryContractId(ctx, v, asDisclosure = true)
      case "QueryInterface" => parseQueryInterface(ctx, v)
      case "QueryInterfaceContractId" => parseQueryInterfaceContractId(ctx, v)
      case "QueryContractKey" => parseQueryContractKey(ctx, v)
      case "AllocParty" => parseAllocParty(ctx, v)
      case "ListKnownParties" => parseListKnownParties(ctx, v)
      case "GetTime" => parseGetTime(ctx, v)
      case "SetTime" => parseSetTime(ctx, v)
      case "Sleep" => parseSleep(ctx, v)
      case "Catch" => parseCatch(v)
      case "Throw" => parseThrow(v)
      case "ValidateUserId" => parseValidateUserId(ctx, v)
      case "CreateUser" => parseCreateUser(ctx, v)
      case "GetUser" => parseGetUser(ctx, v)
      case "DeleteUser" => parseDeleteUser(ctx, v)
      case "ListAllUsers" => parseListAllUsers(ctx, v)
      case "GrantUserRights" => parseGrantUserRights(ctx, v)
      case "RevokeUserRights" => parseRevokeUserRights(ctx, v)
      case "ListUserRights" => parseListUserRights(ctx, v)
      case _ => Left(s"Unknown constructor $constr")
    }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
