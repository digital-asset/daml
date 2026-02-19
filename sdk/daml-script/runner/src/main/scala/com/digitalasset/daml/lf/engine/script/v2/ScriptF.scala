// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, SortedLookupList, Utf8, ImmArray}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Reference}
import com.digitalasset.daml.lf.speedy.SError
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  ExtendedValue,
  ExtendedValueAny,
  ExtendedValueClosureBlob,
  ExtendedValueComputationMode,
}
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.script.converter.Converter.{makeTuple, toContractId, toText}
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

import annotation.unused

object ScriptF {

  private val globalRandom = new scala.util.Random(0)

  sealed trait Cmd {
    private[lf] def executeWithRunner(
        env: Env,
        @annotation.unused runner: v2.Runner,
        @annotation.unused convertLegacyExceptions: Boolean,
    )(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = execute(env)

    private[lf] def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue]
  }
  // The environment that the `execute` function gets access to.
  final class Env(
      val scriptIds: ScriptIds,
      val timeMode: ScriptTimeMode,
      private var _clients: Participants[ScriptLedgerClient],
      compiledPackages: CompiledPackages,
  ) {
    def clients = _clients
    val utcClock = Clock.systemUTC()

    val enricher = new Enricher(
      compiledPackages = compiledPackages,
      // Cannot load packages in GrpcLedgerClient
      loadPackage = { (_: PackageId, _: Reference) => ResultDone(()) },
      addTypeInfo = true,
      addFieldNames = true,
      addTrailingNoneFields = true,
      forbidLocalContractIds = true,
    )

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

    def doesVariantConstructorExist(
        tyCon: Identifier,
        consName: Name,
    ): Boolean =
      compiledPackages.pkgInterface.lookupVariantConstructor(tyCon, consName).isRight

    def lookupLanguageVersion(packageId: PackageId): Either[String, LanguageVersion] = {
      compiledPackages.pkgInterface.lookupPackageLanguageVersion(packageId) match {
        case Right(lv) => Right(lv)
        case Left(err) => Left(err.pretty)
      }
    }

  }

  final case class Throw(exc: ExtendedValueAny) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = {
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
        case (
              ExtendedValueAny(
                _,
                ValueRecord(Some(`invalidUserId`), ImmArray((_, ValueText(msg)))),
              ),
              true,
            ) =>
          makeFailureStatus(invalidUserId, msg)
        case (
              ExtendedValueAny(
                _,
                ValueRecord(
                  Some(`userAlreadyExists`),
                  ImmArray((_, ValueRecord(_, ImmArray((_, ValueText(userId)))))),
                ),
              ),
              true,
            ) =>
          makeFailureStatus(userAlreadyExists, "User already exists: " + userId)
        case (
              ExtendedValueAny(
                _,
                ValueRecord(
                  Some(`userNotFound`),
                  ImmArray((_, ValueRecord(_, ImmArray((_, ValueText(userId)))))),
                ),
              ),
              true,
            ) =>
          makeFailureStatus(userNotFound, "User not found: " + userId)
        case (ExtendedValueAny(Ast.TTyCon(name), v), true) =>
          // Since we cannot call `SBThrow` from the engine, we must re-implement the legacy exception to FailureStatus conversion logic here
          // This involves calculating the exception message by calling the engine again.
          runner
            .runComputation(
              ExtendedValueComputationMode
                .ByExceptionMessage(name, v),
              false,
            )
            .transformWith {
              case Success(ValueText(message)) => makeFailureStatus(name, message)
              case Success(_) =>
                Future.failed(
                  new RuntimeException(s"Message computation for exception $name did not give Text")
                )
              case Failure(
                    free.InterpretationError(
                      SError.SErrorDamlException(
                        IE.UnhandledException(Ast.TTyCon(messageExceptionName), _)
                      )
                    )
                  ) =>
                makeFailureStatus(
                  name,
                  s"<Failed to calculate message as ${messageExceptionName.qualifiedName.toString} was thrown during conversion>",
                )
              case Failure(e) => Future.failed(e)
            }
        case (ExtendedValueAny(ty, _), true) =>
          Future.failed(
            new RuntimeException(
              s"Tried to convert a non-grounded exception type ${ty.pretty} to Failure Status"
            )
          )
        case (ExtendedValueAny(ty, value), false) =>
          Future.failed(
            free.InterpretationError(
              SError.SErrorDamlException(
                IE.UnhandledException(ty, Converter.castCommandExtendedValue(value).toOption.get)
              )
            )
          )
      }
    }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future.failed(new NotImplementedError)
  }

  final case class Catch(act: ExtendedValueClosureBlob) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      runner
        .run(
          ExtendedValueComputationMode.ByClosure(act, List(ValueUnit)),
          convertLegacyExceptions = false,
        )
        .transformWith {
          case Success(v) =>
            Future.successful(
              ValueVariant(Some(StablePackagesV2.Either), Name.assertFromString("Right"), v)
            )
          case Failure(
                free.InterpretationError(
                  SError.SErrorDamlException(IE.UnhandledException(typ, value))
                )
              ) =>
            Future.successful(
              ValueVariant(
                Some(StablePackagesV2.Either),
                Name.assertFromString("Left"),
                ExtendedValueAny(typ, value),
              )
            )
          case Failure(e) => Future.failed(e)
        }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future.failed(new NotImplementedError)
  }

  final case class TryFailureStatus(act: ExtendedValueClosureBlob) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      runner
        .run(
          ExtendedValueComputationMode.ByClosure(act, List(ValueUnit)),
          convertLegacyExceptions = true,
        )
        .transformWith {
          case Success(v) =>
            Future.successful(
              ValueVariant(Some(StablePackagesV2.Either), Name.assertFromString("Right"), v)
            )
          case Failure(
                free.InterpretationError(
                  SError.SErrorDamlException(
                    IE.FailureStatus(errorId, failureCategory, errorMessage, metadata)
                  )
                )
              ) =>
            import com.digitalasset.daml.lf.script.converter.Converter.record
            Future.successful(
              ValueVariant(
                Some(StablePackagesV2.Either),
                Name.assertFromString("Left"),
                record(
                  StablePackagesV2.FailureStatus,
                  ("errorId", ValueText(errorId)),
                  ("category", ValueInt64(failureCategory.toLong)),
                  ("message", ValueText(errorMessage)),
                  (
                    "meta",
                    ValueTextMap(SortedLookupList(metadata.map { case (k, v) =>
                      (k, ValueText(v))
                    })),
                  ),
                ),
              )
            )
          case Failure(e) => Future.failed(e)
        }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future.failed(new NotImplementedError)
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
        .map(results => ValueList(results.to(FrontStack)))

    def singleSubmit(
        submission: Submission,
        env: Env,
    )(implicit ec: ExecutionContext, mat: Materializer): Future[ExtendedValue] =
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
          case (Right((commandResults, tree)), _) => {
            val rs =
              commandResults.to(FrontStack).map(Converter.fromCommandResult(env.scriptIds, _))
            Converter.toFuture(
              Converter
                .translateTransactionTree(
                  env.lookupChoice,
                  env.scriptIds,
                  tree,
                )
                .map(tree =>
                  ValueVariant(
                    Some(StablePackagesV2.Either),
                    Name.assertFromString("Right"),
                    makeTuple(ValueList(rs), tree),
                  )
                )
            )
          }
          case (Left(ScriptLedgerClient.SubmitFailure(err, _)), MustSucceed) => Future.failed(err)
          case (Left(ScriptLedgerClient.SubmitFailure(_, submitError)), _) =>
            Future.successful(
              ValueVariant(
                Some(StablePackagesV2.Either),
                Name.assertFromString("Left"),
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
      } yield ValueList(acs.to(FrontStack).map(Converter.fromCreated(_, tplId)))
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
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractId(parties, tplId, cid)
        optR_ =
          optR.map(c =>
            makeTuple(
              Converter.fromAnyTemplate(tplId, c.argument),
              Converter.fromTemplateTypeRep(c.templateId),
              ValueText(c.blob.toHexString),
            )
          )
      } yield ValueOptional(optR_)
  }

  final case class QueryInterface(
      parties: OneAnd[Set, Party],
      interfaceId: Identifier,
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = {

      for {
        viewType <- Converter.toFuture(env.lookupInterfaceViewTy(interfaceId))
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        interfaces <- client.queryInterface(parties, interfaceId, viewType)
        interfaceValues =
          interfaces.to(FrontStack).map { case (cid, optView) =>
            makeTuple(ValueContractId(cid), ValueOptional(optView))
          }
      } yield ValueList(interfaceValues)
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
    ): Future[ExtendedValue] = {
      for {
        viewType <- Converter.toFuture(env.lookupInterfaceViewTy(interfaceId))
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryInterfaceContractId(parties, interfaceId, viewType, cid)
      } yield ValueOptional(optR)
    }
  }

  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        keyValue <- Converter.toFuture(Converter.castCommandExtendedValue(key.key))
        optR <- client.queryContractKey(
          parties,
          tplId,
          keyValue,
        )
      } yield ValueOptional(optR.map(Converter.fromCreated(_, tplId)))
  }

  /** Allocate a party on the default, a singular, or multiple participants
    *
    *  @param participants if None we use default_participant. If None or Some
    *  with empty list in second position, we use the old allocateParty logic. If
    *  Some and list in second position not empty, we have the multi participant
    *  workflow.
    */
  final case class AllocParty(
      partyHint: String,
      owningParticipant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = {
      for {
        owningClient <- env.clients.assertGetParticipantFuture(owningParticipant)
        party <- owningClient.allocateParty(partyHint)
      } yield {
        owningParticipant.foreach(env.addPartyParticipantMapping(party, _))
        ValueParty(party)
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
    ): Future[ExtendedValue] =
      for {
        client <- env.clients.getParticipant(participant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        partyDetails <- client.listKnownParties()
        partyDetailsValue = partyDetails.map(details =>
          Converter.fromPartyDetails(env.scriptIds, details)
        )
      } yield ValueList(partyDetailsValue.to(FrontStack))

  }
  final case class GetTime() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
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
      } yield ValueTimestamp(time)
  }
  final case class SetTime(time: Timestamp) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      env.timeMode match {
        case ScriptTimeMode.Static =>
          for {
            // We don’t parametrize this by participant since this
            // is only useful in static time mode and using the time
            // service with multiple participants is very dodgy.
            client <- Converter.toFuture(env.clients.getParticipant(None))
            _ <- client.setStaticTime(time)
          } yield ValueUnit
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
    ): Future[ExtendedValue] = Future {
      sleepAtLeast(micros * 1000)
      ValueUnit
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
    ): Future[ExtendedValue] = Future {
      // By using a deterministic PRNG and setting the seed to a fixed value each time we sign a message, we ensure
      // that secp256k1 signing uses a deterministic source of randomness and so behaves deterministically.
      val deterministicRandomSrc: SecureRandom = SecureRandom.getInstance("SHA1PRNG")
      deterministicRandomSrc.setSeed(1)
      val keySpec =
        new PKCS8EncodedKeySpec(HexString.decode(HexString.assertFromString(pk)).toByteArray)
      val privateKey = KeyFactory.getInstance("EC").generatePrivate(keySpec)
      val message = HexString.decode(HexString.assertFromString(msg))
      val messageDigest = HexString.assertFromString(Utf8.sha256(message))

      ValueText(MessageSignatureUtil.sign(messageDigest, privateKey, deterministicRandomSrc))
    }
  }

  final case class Secp256k1WithEcdsaSign(pk: String, msg: String) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future {
      // By using a deterministic PRNG and setting the seed to a fixed value each time we sign a message, we ensure
      // that secp256k1 signing uses a deterministic source of randomness and so behaves deterministically.
      val deterministicRandomSrc: SecureRandom = SecureRandom.getInstance("SHA1PRNG")
      deterministicRandomSrc.setSeed(1)
      val keySpec =
        new PKCS8EncodedKeySpec(HexString.decode(HexString.assertFromString(pk)).toByteArray)
      val privateKey = KeyFactory.getInstance("EC").generatePrivate(keySpec)
      val message = HexString.assertFromString(msg)

      ValueText(MessageSignatureUtil.sign(message, privateKey, deterministicRandomSrc))
    }
  }

  final case class Secp256k1GenerateKeyPair() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future {
      val keyPair = MessageSignatureUtil.generateKeyPair
      val privateKey = HexString.encode(Bytes.fromByteArray(keyPair.getPrivate.getEncoded))
      val publicKey = HexString.encode(Bytes.fromByteArray(keyPair.getPublic.getEncoded))

      import com.digitalasset.daml.lf.script.converter.Converter.record
      record(
        env.scriptIds
          .damlScriptModule("Daml.Script.Internal.Questions.Crypto.Text", "Secp256k1KeyPair"),
        "privateKey" -> ValueText(privateKey),
        "publicKey" -> ValueText(publicKey),
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
    ): Future[ExtendedValue] = {
      val errorOption =
        UserId.fromString(userName) match {
          case Right(_) => None // valid
          case Left(message) => Some(ValueText(message)) // invalid; with error message
        }
      Future.successful(ValueOptional(errorOption))
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
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        res <- client.createUser(user, rights)
        res <- Converter.toFuture(
          Converter.fromOptional[Unit](res, _ => Right(ValueUnit))
        )
      } yield res
  }

  final case class GetUser(
      userId: UserId,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
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
        userValue
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
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        res <- client.deleteUser(userId)
        res <- Converter.toFuture(
          Converter.fromOptional[Unit](res, _ => Right(ValueUnit))
        )
      } yield res
  }

  final case class ListAllUsers(
      participant: Option[Participant]
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        users <- client.listAllUsers()
        users <- Converter.toFuture(
          users.to(FrontStack).traverse(Converter.fromUser(env.scriptIds, _))
        )
      } yield ValueList(users)
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
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.grantUserRights(userId, rights)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(ValueList(_)),
          )
        )
      } yield rights
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
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.revokeUserRights(userId, rights)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(ValueList(_)),
          )
        )
      } yield rights
  }

  final case class ListUserRights(
      userId: UserId,
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        rights <- client.listUserRights(userId)
        rights <- Converter.toFuture(
          Converter.fromOptional[List[UserRight]](
            rights,
            _.to(FrontStack)
              .traverse(Converter.fromUserRight(env.scriptIds, _))
              .map(ValueList(_)),
          )
        )
      } yield rights
  }

  final case class VetPackages(
      packages: List[ScriptLedgerClient.ReadablePackageId],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.vetPackages(packages)
        participantUid <- client.getParticipantUid()
        _ <- Future.traverse(env.clients.participants.values)(
          _.waitUntilVettingVisible(packages, participantUid)
        )
      } yield ValueUnit
  }

  final case class UnvetPackages(
      packages: List[ScriptLedgerClient.ReadablePackageId],
      participant: Option[Participant],
  ) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(participant))
        _ <- client.unvetPackages(packages)
        participantUid <- client.getParticipantUid()
        _ <- Future.traverse(env.clients.participants.values)(
          _.waitUntilUnvettingVisible(packages, participantUid)
        )
      } yield ValueUnit
  }

  final case class ListVettedPackages() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        packages <- client.listVettedPackages()
      } yield ValueList(
        packages.to(FrontStack).map(Converter.fromReadablePackageId(env.scriptIds, _))
      )
  }

  final case class ListAllPackages() extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      for {
        client <- Converter.toFuture(env.clients.getParticipant(None))
        packages <- client.listAllPackages()
      } yield ValueList(
        packages.to(FrontStack).map(Converter.fromReadablePackageId(env.scriptIds, _))
      )
  }

  final case class TryCommands(act: ExtendedValue) extends Cmd {
    override def executeWithRunner(env: Env, runner: v2.Runner, convertLegacyExceptions: Boolean)(
        implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
      runner.runResolved(act, convertLegacyExceptions).transformWith {
        case Success(v) =>
          Future.successful(
            ValueVariant(Some(StablePackagesV2.Either), Name.assertFromString("Right"), v)
          )
        case Failure(
              Script.FailedCmd(cmdName, _, err)
            ) =>
          import com.digitalasset.daml.lf.script.{Error, Pretty}
          val msg = err match {
            case e: Error => Pretty.prettyError(e).render(10000)
            case e => e.getMessage
          }

          val name = err match {
            case Error.RunnerException(SError.SErrorDamlException(iErr)) =>
              iErr.getClass.getSimpleName
            case e => e.getClass.getSimpleName
          }

          import com.digitalasset.daml.lf.script.converter.Converter.record

          Future.successful(
            ValueVariant(
              Some(StablePackagesV2.Either),
              Name.assertFromString("Left"),
              record(
                StablePackagesV2.Tuple3,
                ("_1", ValueText(cmdName)),
                ("_2", ValueText(name)),
                ("_3", ValueText(msg)),
              ),
            )
          )
        case Failure(e) => Future.failed(e)
      }

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] = Future.failed(new NotImplementedError)
  }

  final case class FailWithStatus(failureStatus: IE.FailureStatus) extends Cmd {
    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[ExtendedValue] =
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

  private def parseSubmission(
      v: ExtendedValue,
      knownPackages: KnownPackages,
  ): Either[String, Submission] =
    v match {
      case ValueRecord(
            _,
            ImmArray(
              (_, ValueRecord(_, ImmArray((_, hdAct), (_, ValueList(tlAct))))),
              (_, ValueList(readAs)),
              (_, ValueList(disclosures)),
              (_, ValueOptional(optPackagePreference)),
              (_, ValueList(prefetchKeys)),
              (_, ValueEnum(_, name)),
              (_, ValueList(cmds)),
              (_, ValueOptional(optLocation)),
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

  private def parseSubmit(v: ExtendedValue, knownPackages: KnownPackages): Either[String, Submit] =
    v match {
      case ValueRecord(
            _,
            ImmArray((_, ValueList(submissions))),
          ) =>
        for {
          submissions <- submissions.traverse(parseSubmission(_, knownPackages))
        } yield Submit(submissions = submissions.toList)
      case _ => Left(s"Expected Submit payload but got $v")
    }

  private def parseQueryACS(v: ExtendedValue): Either[String, QueryACS] =
    v match {
      case ValueRecord(_, ImmArray((_, readAs), (_, tplId))) =>
        for {
          readAs <- Converter.toParties(readAs)
          tplId <- Converter
            .typeRepToIdentifier(tplId)
        } yield QueryACS(readAs, tplId)
      case _ => Left(s"Expected QueryACS payload but got $v")
    }

  private def parseQueryContractId(v: ExtendedValue): Either[String, QueryContractId] =
    v match {
      case ValueRecord(_, ImmArray((_, actAs), (_, tplId), (_, cid))) =>
        for {
          actAs <- Converter.toParties(actAs)
          tplId <- Converter.typeRepToIdentifier(tplId)
          cid <- toContractId(cid)
        } yield QueryContractId(actAs, tplId, cid)
      case _ => Left(s"Expected QueryContractId payload but got $v")
    }

  private def parseQueryInterface(v: ExtendedValue): Either[String, QueryInterface] =
    v match {
      case ValueRecord(_, ImmArray((_, actAs), (_, interfaceId))) =>
        for {
          actAs <- Converter.toParties(actAs)
          interfaceId <- Converter.typeRepToIdentifier(interfaceId)
        } yield QueryInterface(actAs, interfaceId)
      case _ => Left(s"Expected QueryInterface payload but got $v")
    }

  private def parseQueryInterfaceContractId(
      v: ExtendedValue
  ): Either[String, QueryInterfaceContractId] =
    v match {
      case ValueRecord(_, ImmArray((_, actAs), (_, interfaceId), (_, cid))) =>
        for {
          actAs <- Converter.toParties(actAs)
          interfaceId <- Converter.typeRepToIdentifier(interfaceId)
          cid <- toContractId(cid)
        } yield QueryInterfaceContractId(actAs, interfaceId, cid)
      case _ => Left(s"Expected QueryInterfaceContractId payload but got $v")
    }

  private def parseQueryContractKey(v: ExtendedValue): Either[String, QueryContractKey] =
    v match {
      case ValueRecord(_, ImmArray((_, actAs), (_, tplId), (_, key))) =>
        for {
          actAs <- Converter.toParties(actAs)
          tplId <- Converter.typeRepToIdentifier(tplId)
          key <- Converter.toAnyContractKey(key)
        } yield QueryContractKey(actAs, tplId, key)
      case _ => Left(s"Expected QueryContractKey payload but got $v")
    }

  private def parseAllocPartyV1(v: ExtendedValue): Either[String, AllocParty] =
    v match {
      case ValueRecord(
            _,
            ImmArray((_, ValueText(requestedName)), (_, ValueText(givenHint)), (_, participantName)),
          ) =>
        for {
          participantName <- Converter.toOptionalParticipantName(participantName)
          idHint <- Converter.toPartyIdHint(givenHint, requestedName, globalRandom)
        } yield AllocParty(idHint, participantName)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }

  private def parseAllocPartyV2(v: ExtendedValue): Either[String, AllocParty] =
    v match {
      case ValueRecord(
            _,
            ImmArray(
              (_, ValueText(requestedName)),
              (_, ValueText(givenHint)),
              (_, participantNames @ ValueList(vs)),
            ),
          ) if vs.length <= 1 =>
        for {
          participantNames <- Converter.toParticipantNames(participantNames)
          idHint <- Converter.toPartyIdHint(givenHint, requestedName, globalRandom)
        } yield AllocParty(idHint, participantNames.headOption)
      case ValueRecord(
            _,
            ImmArray(
              (_, ValueText(_)),
              (_, ValueText(_)),
              (_, ValueList(_)),
            ),
          ) =>
        Left(s"Expected AllocParty payload with at most one participant name but got $v")
      case _ => Left(s"Expected AllocParty payload but got $v")
    }

  private def parseListKnownParties(v: ExtendedValue): Either[String, ListKnownParties] =
    v match {
      case ValueRecord(_, ImmArray((_, participantName))) =>
        for {
          participantName <- Converter.toOptionalParticipantName(participantName)
        } yield ListKnownParties(participantName)
      case _ => Left(s"Expected ListKnownParties payload but got $v")
    }

  private def parseEmpty[A](result: A)(v: ExtendedValue): Either[String, A] =
    v match {
      case ValueRecord(_, ImmArray()) => Right(result)
      case _ => Left(s"Expected ${result.getClass.getSimpleName} payload but got $v")
    }

  private def parseSetTime(v: ExtendedValue): Either[String, SetTime] =
    v match {
      case ValueRecord(_, ImmArray((_, time))) =>
        for {
          time <- Converter.toTimestamp(time)
        } yield SetTime(time)
      case _ => Left(s"Expected SetTime payload but got $v")
    }

  private def parseSecp256k1Sign(v: ExtendedValue): Either[String, Secp256k1Sign] =
    v match {
      case ValueRecord(_, ImmArray((_, pk), (_, msg))) =>
        for {
          pk <- toText(pk)
          msg <- toText(msg)
        } yield Secp256k1Sign(pk, msg)
      case _ => Left(s"Expected Secp256k1Sign payload but got $v")
    }

  private def parseSecp256k1WithEcdsaSign(
      v: ExtendedValue
  ): Either[String, Secp256k1WithEcdsaSign] =
    v match {
      case ValueRecord(_, ImmArray((_, pk), (_, msg))) =>
        for {
          pk <- toText(pk)
          msg <- toText(msg)
        } yield Secp256k1WithEcdsaSign(pk, msg)
      case _ => Left(s"Expected Secp256k1WithEcdsaSign payload but got $v")
    }

  private def parseSleep(v: ExtendedValue): Either[String, Sleep] =
    v match {
      case ValueRecord(_, ImmArray((_, ValueRecord(_, ImmArray((_, ValueInt64(micros))))))) =>
        Right(Sleep(micros))
      case _ => Left(s"Expected Sleep payload but got $v")
    }

  private def parseCatch(v: ExtendedValue): Either[String, Catch] =
    v match {
      // Catch includes a dummy field for old style typeclass LF encoding, we ignore it here.
      case ValueRecord(_, ImmArray((_, act: ExtendedValueClosureBlob), _)) => Right(Catch(act))
      case _ => Left(s"Expected Catch payload but got $v")
    }

  private def parseThrow(v: ExtendedValue): Either[String, Throw] =
    v match {
      case ValueRecord(_, ImmArray((_, exc: ExtendedValueAny))) => Right(Throw(exc))
      case _ => Left(s"Expected Throw payload but got $v")
    }

  private def parseTryFailureStatus(v: ExtendedValue): Either[String, TryFailureStatus] =
    v match {
      // TryFailureStatus includes a dummy field for old style typeclass LF encoding, we ignore it here.
      case ValueRecord(_, ImmArray((_, act: ExtendedValueClosureBlob), _)) =>
        Right(TryFailureStatus(act))
      case _ => Left(s"Expected TryFailureStatus payload but got $v")
    }

  private def parseValidateUserId(v: ExtendedValue): Either[String, ValidateUserId] =
    v match {
      case ValueRecord(_, ImmArray((_, userName))) =>
        for {
          userName <- toText(userName)
        } yield ValidateUserId(userName)
      case _ => Left(s"Expected ValidateUserId payload but got $v")
    }

  private def parseCreateUser(v: ExtendedValue): Either[String, CreateUser] =
    v match {
      case ValueRecord(_, ImmArray((_, user), (_, rights), (_, participant))) =>
        for {
          user <- Converter.toUser(user)
          participant <- Converter.toOptionalParticipantName(participant)
          rights <- Converter.toList(rights, Converter.toUserRight)
        } yield CreateUser(user, rights, participant)
      case _ => Left(s"Exected CreateUser payload but got $v")
    }

  private def parseGetUser(v: ExtendedValue): Either[String, GetUser] =
    v match {
      case ValueRecord(_, ImmArray((_, userId), (_, participant))) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield GetUser(userId, participant)
      case _ => Left(s"Expected GetUser payload but got $v")
    }

  private def parseDeleteUser(v: ExtendedValue): Either[String, DeleteUser] =
    v match {
      case ValueRecord(_, ImmArray((_, userId), (_, participant))) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield DeleteUser(userId, participant)
      case _ => Left(s"Expected DeleteUser payload but got $v")
    }

  private def parseListAllUsers(v: ExtendedValue): Either[String, ListAllUsers] =
    v match {
      case ValueRecord(_, ImmArray((_, participant))) =>
        for {
          participant <- Converter.toOptionalParticipantName(participant)
        } yield ListAllUsers(participant)
      case _ => Left(s"Expected ListAllUsers payload but got $v")
    }

  private def parseGrantUserRights(v: ExtendedValue): Either[String, GrantUserRights] =
    v match {
      case ValueRecord(_, ImmArray((_, userId), (_, rights), (_, participant))) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield GrantUserRights(userId, rights, participant)
      case _ => Left(s"Expected GrantUserRights payload but got $v")
    }

  private def parseRevokeUserRights(v: ExtendedValue): Either[String, RevokeUserRights] =
    v match {
      case ValueRecord(_, ImmArray((_, userId), (_, rights), (_, participant))) =>
        for {
          userId <- Converter.toUserId(userId)
          rights <- Converter.toList(rights, Converter.toUserRight)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield RevokeUserRights(userId, rights, participant)
      case _ => Left(s"Expected RevokeUserRights payload but got $v")
    }

  private def parseListUserRights(v: ExtendedValue): Either[String, ListUserRights] =
    v match {
      case ValueRecord(_, ImmArray((_, userId), (_, participant))) =>
        for {
          userId <- Converter.toUserId(userId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield ListUserRights(userId, participant)
      case _ => Left(s"Expected ListUserRights payload but got $v")
    }

  private def parseChangePackages[A](
      v: ExtendedValue,
      wrap: (List[ScriptLedgerClient.ReadablePackageId], Option[Participant]) => A,
  ): Either[String, A] = {
    def toReadablePackageId(
        s: ExtendedValue
    ): Either[String, ScriptLedgerClient.ReadablePackageId] =
      s match {
        case ValueRecord(_, ImmArray((_, ValueText(name)), (_, ValueText(version)))) =>
          for {
            pname <- PackageName.fromString(name)
            pversion <- PackageVersion.fromString(version)
          } yield ScriptLedgerClient.ReadablePackageId(pname, pversion)
        case _ => Left(s"Expected PackageName but got $s")
      }
    v match {
      case ValueRecord(_, ImmArray((_, packages), (_, participant))) =>
        for {
          packageIds <- Converter.toList(packages, toReadablePackageId)
          participant <- Converter.toOptionalParticipantName(participant)
        } yield wrap(packageIds, participant)
      case _ => Left(s"Expected (Vet|Unvet)Packages payload but got $v")
    }
  }

  private def parseTryCommands(v: ExtendedValue): Either[String, TryCommands] =
    v match {
      case ValueRecord(_, ImmArray((_, act))) => Right(TryCommands(act))
      case _ => Left(s"Expected TryCommands payload but got $v")
    }

  private def parseFailWithStatus(v: ExtendedValue): Either[String, FailWithStatus] =
    v match {
      case ValueRecord(
            _,
            ImmArray(
              (
                _,
                ValueRecord(
                  _,
                  ImmArray(
                    (_, ValueText(errorId)),
                    (_, ValueInt64(categoryId)),
                    (_, ValueText(message)),
                    (_, ValueTextMap(treeMap)),
                  ),
                ),
              )
            ),
          ) =>
        treeMap.toImmArray.toList
          .traverse {
            case (key, ValueText(value)) => Right((key, value))
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
      v: ExtendedValue,
      @unused knownPackages: KnownPackages,
  ): Either[String, Cmd] = {
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
      case ("Secp256k1WithEcdsaSign", 1) => parseSecp256k1WithEcdsaSign(v)
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
  }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
