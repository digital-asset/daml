// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.data.{Bytes, FrontStack}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.{SBThrow, SBToAny, SBVariantCon}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.{ArrayList, SError, SValue}
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.daml.script.converter.Converter.{makeTuple, toContractId, toText}
import com.digitalasset.canton.ledger.api.{User, UserRight}
import org.apache.pekko.stream.Materializer
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.{Foldable, OneAnd}

import java.security.{KeyFactory, SecureRandom}
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Clock
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ScriptF {

  private val globalRandom = new scala.util.Random(0)

  val left = SEBuiltinFun(
    SBVariantCon(StablePackagesV2.Either, Name.assertFromString("Left"), 0)
  )
  val right = SEBuiltinFun(
    SBVariantCon(StablePackagesV2.Either, Name.assertFromString("Right"), 1)
  )

  sealed trait Cmd {
    private[lf] def executeWithRunner(
        env: Env,
        @annotation.unused runner: v2.Runner,
        @annotation.unused convertLegacyExceptions: Boolean,
    )(implicit
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
      requireContractIdSuffix = false,
      // We need to translate pseudo-exceptions that aren't serializable
      shouldCheckDataSerializable = false,
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

    def lookupVariantConstructorRank(
        tyCon: Identifier,
        consName: Name,
    ): Either[String, Int] =
      compiledPackages.pkgInterface.lookupVariantConstructor(tyCon, consName) match {
        case Right(info) => Right(info.rank)
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
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = {
      def makeFailureStatus(name: Identifier, msg: String) =
        Future.failed(
          free.InterpretationError(
            SError.SErrorDamlException(
              IE.FailureStatus(
                "UNHANDLED_EXCEPTION/" + name.qualifiedName.toString,
                Ast.FCInvalidGivenCurrentSystemStateOther.cantonCategoryId,
                msg,
                Map(),
              )
            )
          )
        )
      def userManagementDef(name: String) =
        env.scriptIds.damlScriptModule("Daml.Script.Internal.Questions.UserManagement", name)
      val invalidUserId = userManagementDef("InvalidUserId")
      val userAlreadyExists = userManagementDef("UserAlreadyExists")
      val userNotFound = userManagementDef("UserNotFound")
      (exc, convertLegacyExceptions) match {
        // Pseudo exceptions defined by daml-script need explicit conversion logic, as compiler won't generate them
        // Can be removed in 3.4, when exceptions will be replaced with FailureStatus (https://github.com/DACH-NY/canton/issues/23881)
        case (SAnyException(SRecord(`invalidUserId`, _, ArrayList(SText(msg)))), true) =>
          makeFailureStatus(invalidUserId, msg)
        case (
              SAnyException(
                SRecord(`userAlreadyExists`, _, ArrayList(SRecord(_, _, ArrayList(SText(userId)))))
              ),
              true,
            ) =>
          makeFailureStatus(userAlreadyExists, "User already exists: " + userId)
        case (
              SAnyException(
                SRecord(`userNotFound`, _, ArrayList(SRecord(_, _, ArrayList(SText(userId)))))
              ),
              true,
            ) =>
          makeFailureStatus(userNotFound, "User not found: " + userId)
        case _ => Future.successful(SBThrow(SEValue(exc)))
      }
    }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future.failed(new NotImplementedError)
  }

  final case class Catch(act: SValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      runner
        .run(SEAppAtomic(SEValue(act), Array(SEValue(SUnit))), convertLegacyExceptions = false)
        .transformWith {
          case Success(v) =>
            Future.successful(SEAppAtomic(right, Array(SEValue(v))))
          case Failure(
                free.InterpretationError(
                  SError.SErrorDamlException(IE.UnhandledException(typ, value))
                )
              ) =>
            env.translateValue(typ, value) match {
              case Right(sVal) =>
                Future.successful(
                  SELet1(
                    SEAppAtomic(SEBuiltinFun(SBToAny(typ)), Array(SEValue(sVal))),
                    SEAppAtomic(left, Array(SELocS(1))),
                  )
                )
              // This shouldn't ever happen, as these can only come from our engine
              case Left(err) =>
                Future.failed(
                  new RuntimeException(s"Daml-script thrown error couldn't be translated: $err")
                )
            }

          case Failure(e) => Future.failed(e)
        }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future.failed(new NotImplementedError)
  }

  final case class TryFailureStatus(act: SValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      runner
        .run(SEAppAtomic(SEValue(act), Array(SEValue(SUnit))), convertLegacyExceptions = true)
        .transformWith {
          case Success(v) =>
            Future.successful(SEAppAtomic(right, Array(SEValue(v))))
          case Failure(
                free.InterpretationError(
                  SError.SErrorDamlException(
                    IE.FailureStatus(errorId, failureCategory, errorMessage, metadata)
                  )
                )
              ) =>
            import com.daml.script.converter.Converter.record
            Future.successful(
              SEAppAtomic(
                left,
                Array(
                  SEValue(
                    record(
                      StablePackagesV2.FailureStatus,
                      ("errorId", SText(errorId)),
                      ("category", SInt64(failureCategory.toLong)),
                      ("message", SText(errorMessage)),
                      (
                        "meta",
                        SMap(true, metadata.map { case (k, v) => (SText(k), SText(v)) }),
                      ),
                    )
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

  final case class Submission(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmds: List[ScriptLedgerClient.CommandWithMeta],
      optPackagePreference: Option[List[PackageId]],
      disclosures: List[Disclosure],
      prefetchKeys: List[AnyContractKey],
      errorBehaviour: ScriptLedgerClient.SubmissionErrorBehaviour,
      optLocation: Option[Location],
  )

  // The one submit to rule them all
  final case class Submit(submissions: List[Submission]) extends Cmd {
    import ScriptLedgerClient.SubmissionErrorBehaviour._

    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      Future
        .traverse(submissions)(singleSubmit(_, env))
        .map(results => SEValue(SList(results.to(FrontStack))))

    def singleSubmit(
        submission: Submission,
        env: Env,
    )(implicit ec: ExecutionContext, mat: Materializer): Future[SValue] =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(submission.actAs)
        )
        submitRes <- client.submit(
          submission.actAs,
          submission.readAs,
          submission.disclosures,
          submission.optPackagePreference,
          submission.cmds,
          submission.prefetchKeys,
          submission.optLocation,
          env.lookupLanguageVersion,
          submission.errorBehaviour,
        )
        res <- (submitRes, submission.errorBehaviour) match {
          case (Right(_), MustFail) =>
            Future.failed(
              SError.SErrorDamlException(
                interpretation.Error.UserError("Expected submit to fail but it succeeded")
              )
            )
          case (Right((commandResults, tree)), _) =>
            Converter.toFuture(
              commandResults
                .to(FrontStack)
                .traverse(
                  Converter.fromCommandResult(
                    env.lookupChoice,
                    env.valueTranslator,
                    env.scriptIds,
                    _,
                  )
                )
                .flatMap { rs =>
                  Converter
                    .translateTransactionTree(
                      env.lookupChoice,
                      env.valueTranslator,
                      env.scriptIds,
                      tree,
                    )
                    .map((rs, _))
                }
                .map { case (rs, tree) =>
                  SVariant(
                    StablePackagesV2.Either,
                    Name.assertFromString("Right"),
                    1,
                    makeTuple(SList(rs), tree),
                  )
                }
            )
          case (Left(ScriptLedgerClient.SubmitFailure(err, _)), MustSucceed) => Future.failed(err)
          case (Left(ScriptLedgerClient.SubmitFailure(_, submitError)), _) =>
            Future.successful(
              SVariant(
                StablePackagesV2.Either,
                Name.assertFromString("Left"),
                0,
                submitError.toDamlSubmitError(env),
              )
            )
        }
      } yield res
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
              Converter.fromCreated(env.valueTranslator, _, tplId)
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
              .fromAnyTemplate(
                env.valueTranslator,
                tplId,
                c.argument,
              )
              .map(
                makeTuple(
                  _,
                  Converter.fromTemplateTypeRep(c.templateId),
                  SText(c.blob.toHexString),
                )
              )
          )
        )
      } yield SEValue(SOptional(optR))
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
                  Right(makeTuple(SContractId(cid), SOptional(None)))
                case Some(view) =>
                  for {
                    view <- Converter.fromInterfaceView(
                      env.valueTranslator,
                      viewType,
                      view,
                    )
                  } yield {
                    makeTuple(SContractId(cid), SOptional(Some(view)))
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
        env: Env
    )(id: Identifier, v: Value): Either[String, SValue] =
      for {
        keyTy <- env.lookupKeyTy(id)
        translated <- env.valueTranslator
          .translateValue(keyTy, v)
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
          translateKey(env),
        )
        optR <- Converter.toFuture(
          optR.traverse(
            Converter.fromCreated(env.valueTranslator, _, tplId)
          )
        )
      } yield SEValue(SOptional(optR))
  }
  final case class AllocParty(
      idHint: String,
      participants: List[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = {
      def replicateParty(
          party: Party,
          fromClient: ScriptLedgerClient,
          toParticipant: Participant,
      ): Future[Unit] = for {
        toClient <- env.clients.getParticipant(Some(toParticipant)) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        _ <- toClient.proposePartyReplication(party, toClient.getParticipantUid)
        _ <- fromClient.proposePartyReplication(party, toClient.getParticipantUid)
        _ <- Future.traverse(env.clients.participants.values)(client =>
          client.waitUntilHostingVisible(party, toClient.getParticipantUid)
        )
      } yield ()

      val mainParticipant = participants.headOption
      val additionalParticipants = if (participants.isEmpty) List.empty else participants.tail
      for {
        mainClient <- env.clients.getParticipant(mainParticipant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        party <- mainClient.allocateParty(idHint)
        _ <- Future.traverse(additionalParticipants)(toParticipant =>
          replicateParty(party, mainClient, toParticipant)
        )
      } yield {
        mainParticipant.foreach(env.addPartyParticipantMapping(party, _))
        SEValue(SParty(party))
      }
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

  final case class Secp256k1Sign(pk: String, msg: String) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future {
      // By using a deterministic PRNG and setting the seed to a fixed value each time we sign a message, we ensure
      // that secp256k1 signing uses a deterministic source of randomness and so behaves deterministically.
      val deterministicRandomSrc: SecureRandom = SecureRandom.getInstance("SHA1PRNG")
      deterministicRandomSrc.setSeed(1)
      val keySpec =
        new PKCS8EncodedKeySpec(HexString.decode(HexString.assertFromString(pk)).toByteArray)
      val privateKey = KeyFactory.getInstance("EC").generatePrivate(keySpec)
      val message = HexString.assertFromString(msg)

      SEValue(SText(MessageSignatureUtil.sign(message, privateKey, deterministicRandomSrc)))
    }
  }

  final case class Secp256k1GenerateKeyPair() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future {
      val keyPair = MessageSignatureUtil.generateKeyPair
      val privateKey = HexString.encode(Bytes.fromByteArray(keyPair.getPrivate.getEncoded))
      val publicKey = HexString.encode(Bytes.fromByteArray(keyPair.getPublic.getEncoded))

      import com.daml.script.converter.Converter.record
      SEValue(
        record(
          env.scriptIds
            .damlScriptModule("Daml.Script.Internal.Questions.Crypto.Text", "Secp256k1KeyPair"),
          "privateKey" -> SText(privateKey),
          "publicKey" -> SText(publicKey),
        )
      )
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
        userValue <- Converter.toFuture(
          Converter.fromOptional(user, Converter.fromUser(env.scriptIds, _))
        )
      } yield {
        (participant, user.flatMap(_.primaryParty)) match {
          case (Some(participant), Some(party)) =>
            env.addPartyParticipantMapping(party, participant)
          case _ =>
        }
        SEValue(userValue)
      }
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
      packages: List[ScriptLedgerClient.ReadablePackageId],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.vetPackages(packages)
        _ <- Future.traverse(env.clients.participants.values)(
          _.waitUntilVettingVisible(packages, client.getParticipantUid)
        )
      } yield SEValue(SUnit)
  }

  final case class UnvetPackages(
      packages: List[ScriptLedgerClient.ReadablePackageId],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.unvetPackages(packages)
        _ <- Future.traverse(env.clients.participants.values)(
          _.waitUntilUnvettingVisible(packages, client.getParticipantUid)
        )
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

  final case class TryCommands(act: SValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      runner.run(SEValue(act), convertLegacyExceptions).transformWith {
        case Success(v) =>
          Future.successful(SEAppAtomic(right, Array(SEValue(v))))
        case Failure(
              Script.FailedCmd(cmdName, _, err)
            ) =>
          import com.digitalasset.daml.lf.script.{Error, Pretty}
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

  final case class FailWithStatus(failureStatus: IE.FailureStatus) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      Future.failed(free.InterpretationError(SError.SErrorDamlException(failureStatus)))
  }

  final case class Ctx(knownPackages: Map[String, PackageId], compiledPackages: CompiledPackages)

  final case class KnownPackages(pkgs: Map[String, PackageId])

  private def parseErrorBehaviour(
      n: Name
  ): Either[String, ScriptLedgerClient.SubmissionErrorBehaviour] =
    n match {
      case "MustSucceed" => Right(ScriptLedgerClient.SubmissionErrorBehaviour.MustSucceed)
      case "MustFail" => Right(ScriptLedgerClient.SubmissionErrorBehaviour.MustFail)
      case "Try" => Right(ScriptLedgerClient.SubmissionErrorBehaviour.Try)
      case _ => Left("Unknown constructor " + n)
    }

  private def parseSubmission(v: SValue, knownPackages: KnownPackages): Either[String, Submission] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(_, _, ArrayList(hdAct, SList(tlAct))),
              SList(readAs),
              SList(disclosures),
              SOptional(optPackagePreference),
              SList(prefetchKeys),
              SEnum(_, name, _),
              SList(cmds),
              SOptional(optLocation),
            ),
          ) =>
        for {
          actAs <- OneAnd(hdAct, tlAct.toList).traverse(Converter.toParty)
          readAs <- readAs.traverse(Converter.toParty)
          disclosures <- disclosures.toImmArray.toList.traverse(Converter.toDisclosure)
          optPackagePreference <- optPackagePreference.traverse(
            Converter.toList(_, Converter.toPackageId)
          )
          prefetchKeys <- prefetchKeys.toImmArray.toList.traverse(Converter.toAnyContractKey)
          errorBehaviour <- parseErrorBehaviour(name)
          cmds <- cmds.toList.traverse(Converter.toCommandWithMeta)
          optLocation <- optLocation.traverse(Converter.toLocation(knownPackages.pkgs, _))
        } yield Submission(
          actAs = toOneAndSet(actAs),
          readAs = readAs.toSet,
          disclosures = disclosures,
          optPackagePreference = optPackagePreference,
          prefetchKeys = prefetchKeys,
          errorBehaviour = errorBehaviour,
          cmds = cmds,
          optLocation = optLocation,
        )
      case _ => Left(s"Expected Submission payload but got $v")
    }

  private def parseSubmit(v: SValue, knownPackages: KnownPackages): Either[String, Submit] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(SList(submissions)),
          ) =>
        for {
          submissions <- submissions.traverse(parseSubmission(_, knownPackages))
        } yield Submit(submissions = submissions.toList)
      case _ => Left(s"Expected Submit payload but got $v")
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

  private def parseAllocPartyV1(v: SValue): Either[String, AllocParty] =
    v match {
      case SRecord(_, _, ArrayList(SText(requestedName), SText(givenHint), participantName)) =>
        for {
          participantName <- Converter.toOptionalParticipantName(participantName)
          idHint <- Converter.toPartyIdHint(givenHint, requestedName, globalRandom)
        } yield AllocParty(idHint, participantName.toList)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }

  private def parseAllocPartyV2(v: SValue): Either[String, AllocParty] =
    v match {
      case SRecord(_, _, ArrayList(SText(requestedName), SText(givenHint), participantNames)) =>
        for {
          participantNames <- Converter.toParticipantNames(participantNames)
          idHint <- Converter.toPartyIdHint(givenHint, requestedName, globalRandom)
        } yield AllocParty(idHint, participantNames)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }

  private def parseListKnownParties(v: SValue): Either[String, ListKnownParties] =
    v match {
      case SRecord(_, _, ArrayList(participantName)) =>
        for {
          participantName <- Converter.toOptionalParticipantName(participantName)
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

  private def parseSecp256k1Sign(v: SValue): Either[String, Secp256k1Sign] =
    v match {
      case SRecord(_, _, ArrayList(pk, msg)) =>
        for {
          pk <- toText(pk)
          msg <- toText(msg)
        } yield Secp256k1Sign(pk, msg)
      case _ => Left(s"Expected Secp256k1Sign payload but got $v")
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

  private def parseTryFailureStatus(v: SValue): Either[String, TryFailureStatus] =
    v match {
      // TryFailureStatus includes a dummy field for old style typeclass LF encoding, we ignore it here.
      case SRecord(_, _, ArrayList(act, _)) => Right(TryFailureStatus(act))
      case _ => Left(s"Expected TryFailureStatus payload but got $v")
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
          participant <- Converter.toOptionalParticipantName(participant)
          rights <- Converter.toList(rights, Converter.toUserRight)
        } yield CreateUser(user, rights, participant)
      case _ => Left(s"Exected CreateUser payload but got $v")
    }

  private def parseGetUser(v: SValue): Either[String, GetUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield GetUser(userId, participant)
      case _ => Left(s"Expected GetUser payload but got $v")
    }

  private def parseDeleteUser(v: SValue): Either[String, DeleteUser] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield DeleteUser(userId, participant)
      case _ => Left(s"Expected DeleteUser payload but got $v")
    }

  private def parseListAllUsers(v: SValue): Either[String, ListAllUsers] =
    v match {
      case SRecord(_, _, ArrayList(participant)) =>
        for {
          participant <- Converter.toOptionalParticipantName(participant)
        } yield ListAllUsers(participant)
      case _ => Left(s"Expected ListAllUsers payload but got $v")
    }

  private def parseGrantUserRights(v: SValue): Either[String, GrantUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield GrantUserRights(userId, rights, participant)
      case _ => Left(s"Expected GrantUserRights payload but got $v")
    }

  private def parseRevokeUserRights(v: SValue): Either[String, RevokeUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, rights, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield RevokeUserRights(userId, rights, participant)
      case _ => Left(s"Expected RevokeUserRights payload but got $v")
    }

  private def parseListUserRights(v: SValue): Either[String, ListUserRights] =
    v match {
      case SRecord(_, _, ArrayList(userId, participant)) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield ListUserRights(userId, participant)
      case _ => Left(s"Expected ListUserRights payload but got $v")
    }

  private def parseChangePackages[A](
      v: SValue,
      wrap: (List[ScriptLedgerClient.ReadablePackageId], Option[Participant]) => A,
  ): Either[String, A] = {
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
      case SRecord(_, _, ArrayList(packages, participant)) =>
        for {
          packageIds <- Converter.toList(packages, toReadablePackageId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield wrap(packageIds, participant)
      case _ => Left(s"Expected (Vet|Unvet)Packages payload but got $v")
    }
  }

  private def parseTryCommands(v: SValue): Either[String, TryCommands] =
    v match {
      case SRecord(_, _, ArrayList(act)) => Right(TryCommands(act))
      case _ => Left(s"Expected TryCommands payload but got $v")
    }

  private def parseFailWithStatus(v: SValue): Either[String, FailWithStatus] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(
              SRecord(
                _,
                _,
                ArrayList(SText(errorId), SInt64(categoryId), SText(message), SMap(true, treeMap)),
              )
            ),
          ) =>
        treeMap.toList
          .traverse {
            case (SText(key), SText(value)) => Right((key, value))
            case v => Left(s"Expected (Text, Text) but got $v")
          }
          .map(meta =>
            FailWithStatus(IE.FailureStatus(errorId, categoryId.toInt, message, Map.from(meta)))
          )
      case _ => Left(s"Expected FailWithStatus payload but got $v")
    }

  def parse(
      commandName: String,
      version: Long,
      v: SValue,
      knownPackages: KnownPackages,
  ): Either[String, Cmd] =
    (commandName, version) match {
      case ("Submit", 1) => parseSubmit(v, knownPackages)
      case ("QueryACS", 1) => parseQueryACS(v)
      case ("QueryContractId", 1) => parseQueryContractId(v)
      case ("QueryInterface", 1) => parseQueryInterface(v)
      case ("QueryInterfaceContractId", 1) => parseQueryInterfaceContractId(v)
      case ("QueryContractKey", 1) => parseQueryContractKey(v)
      case ("AllocateParty", 1) => parseAllocPartyV1(v)
      case ("AllocateParty", 2) => parseAllocPartyV2(v)
      case ("ListKnownParties", 1) => parseListKnownParties(v)
      case ("GetTime", 1) => parseEmpty(GetTime())(v)
      case ("SetTime", 1) => parseSetTime(v)
      case ("Sleep", 1) => parseSleep(v)
      case ("Secp256k1Sign", 1) => parseSecp256k1Sign(v)
      case ("Secp256k1GenerateKeyPair", 1) => parseEmpty(Secp256k1GenerateKeyPair())(v)
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
      case ("VetPackages", 1) => parseChangePackages(v, VetPackages)
      case ("UnvetPackages", 1) => parseChangePackages(v, UnvetPackages)
      case ("ListVettedPackages", 1) => parseEmpty(ListVettedPackages())(v)
      case ("ListAllPackages", 1) => parseEmpty(ListAllPackages())(v)
      case ("TryCommands", 1) => parseTryCommands(v)
      case ("FailWithStatus", 1) => parseFailWithStatus(v)
      case ("TryFailureStatus", 1) => parseTryFailureStatus(v)
      case _ => Left(s"Unknown command $commandName - Version $version")
    }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
