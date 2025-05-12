// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.command.{ApiCommand, ApiCommands}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.transaction.{
  SubmittedTransaction,
  Transaction,
  TransactionCoder,
  VersionedTransaction,
}
import com.daml.lf.value.{Value, ValueCoder}
import com.daml.logging.LoggingContext
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MixLFUpgradeTest extends AnyFreeSpec with Matchers with Inside {

  import Ordering.Implicits._

  def toPkgVersion(langVersion: LanguageVersion) =
    langVersion match {
      case LanguageVersion.v1_dev => "9999"
      case otherwise => otherwise.minor.identifier
    }

  def buildIfacePkg(langVersion: LanguageVersion) = {
    val pkgId = Ref.PackageId.assertFromString("ifacePkgId-" + langVersion.minor.identifier)
    implicit val parserParameters: ParserParameters[this.type] = ParserParameters(
      defaultPackageId = pkgId,
      languageVersion = langVersion,
    )

    pkgId ->
      p"""metadata ( '-interface-' : '${toPkgVersion(langVersion)}' )

        module Mod {
          record @serializable R = { p: Party, data: Text, extraData: Option Text };
          record @serializable MyUnit = {};
          interface (this: Iface) = {
            viewtype Mod:MyUnit;
            method data: Text;
            choice @nonConsuming Choice (self) (arg: Mod:R) : Mod:R
              , controllers (Cons @Party [Mod:R {p} arg] (Nil @Party))
              to upure @Mod:R Mod:R {arg with data = APPEND_TEXT (Mod:R {data} arg) (call_method @Mod:Iface data this)};
          } ;
        }
     """
  }

  def buildImplPkg(ifacePkgId: Ref.PackageId, langVersion: LanguageVersion, version: Int) = {
    val pkgId = Ref.PackageId.assertFromString(
      "implPkgId-" + version.toString + "-" + langVersion.minor.identifier
    )
    implicit val parserParameters: ParserParameters[this.type] = ParserParameters(
      defaultPackageId = pkgId,
      languageVersion = langVersion,
    )

    val ifaceId = s"'$ifacePkgId':Mod:Iface"
    val ifaceRId = s"'$ifacePkgId':Mod:R"
    val ifaceMyUnitId = s"'$ifacePkgId':Mod:MyUnit"

    pkgId ->
      p"""metadata ( '-implementation-' : '$version.${toPkgVersion(langVersion)}' )

        module Mod {
          record @serializable T = { p: Party, data: Text, extraData: Option Text };
          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {p} this] (Nil @Party);
            observers Nil @Party;
            agreement "";

            choice @nonConsuming Choice (self) (arg: $ifaceRId) : $ifaceRId
              , controllers (Cons @Party [$ifaceRId {p} arg] (Nil @Party))
              to exercise_interface @$ifaceId Choice (COERCE_CONTRACT_ID @Mod:T @$ifaceId self) arg;

            implements $ifaceId {
              view = $ifaceMyUnitId {};
              method data = Mod:T {data} this;
            };
          };
        }
     """
  }

  def checkSerializable(tx: VersionedTransaction): Unit = {
    val serialized = TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      .fold(x => sys.error(x.errorMessage), identity)
    val deserialized = TransactionCoder
      .decodeTransaction(TransactionCoder.NidDecoder, ValueCoder.CidDecoder, serialized)
      .fold(x => sys.error(x.errorMessage), identity)
    require(deserialized == tx)
  }

  val langVersions = LanguageVersion.AllV1.filter(LanguageVersion.v1_15 <= _)

  assert(
    langVersions.size == 3,
    "carefully double check the versioning of node if a new LF is introduced.",
  )

  val alice = Ref.Party.assertFromString("Alice")
  val participantId = Ref.ParticipantId.assertFromString("particitipant")
  val seed = crypto.Hash.hashPrivateKey("seed")
  implicit val logContext: LoggingContext = LoggingContext.ForTesting
  val engineConfig = EngineConfig(
    allowedLanguageVersions = LanguageVersion.AllVersions(LanguageMajorVersion.V1)
  )

  for (ifaceVersion <- langVersions) s"interface version = ${ifaceVersion.pretty}" - {
    val entryIface @ (ifaceId, ifacePkg) = buildIfacePkg(ifaceVersion)

    for (tmplVersion1 <- langVersions if ifaceVersion <= tmplVersion1)
      s"implementation version = ${tmplVersion1.pretty}" - {

        val broken =
          (ifaceVersion < LanguageVersion.Features.smartContractUpgrade) && (LanguageVersion.Features.smartContractUpgrade <= tmplVersion1)

        val entryImpl1 @ (implPkgId, implPkg) = buildImplPkg(ifaceId, tmplVersion1, 1)

        val pkgs = Map(entryIface, entryImpl1)

        s"exercise-by-interface succeeds" in {
          val engine = new Engine(engineConfig)
          val pkgMap = List(
            ifaceId -> ifacePkg.metadata,
            implPkgId -> implPkg.metadata,
          ).collect { case (id, Some(m)) => id -> (m.name -> m.version) }.toMap
          val pkgPref = Set(ifaceId, implPkgId)

          val cmd = ApiCommand.CreateAndExercise(
            Ref.Identifier(implPkgId, Ref.QualifiedName.assertFromString("Mod:T")),
            Value.ValueRecord(
              None,
              ImmArray(
                None -> Value.ValueParty(alice),
                None -> Value.ValueText("implementation"),
                None -> Value.ValueOptional(None),
              ),
            ),
            Ref.Name.assertFromString("Choice"),
            Value.ValueRecord(
              None,
              ImmArray(
                None -> Value.ValueParty(alice),
                None -> Value.ValueText("implementation"),
                None -> Value.ValueOptional(None),
              ),
            ),
          )

          val result: Either[String, (SubmittedTransaction, Transaction.Metadata)] = scala.util
            .Try {
              engine
                .submit(
                  packageMap = pkgMap,
                  packagePreference = pkgPref,
                  submitters = Set(alice),
                  readAs = Set.empty,
                  cmds = ApiCommands(ImmArray(cmd), Time.Timestamp.Epoch, "cmdsRef"),
                  disclosures = ImmArray.Empty,
                  participantId = participantId,
                  submissionSeed = seed,
                  prefetchKeys = Seq.empty,
                  engineLogger = None,
                )
                .consume(pkgs = pkgs)
                .left
                .map(_.toString)
            }
            .toEither
            .left
            .map(_.toString)
            .flatten

          inside(result) {
            case Right((tx, _)) if !broken =>
              checkSerializable(tx)
              succeed
            case Left(_) if broken =>
              succeed
          }
        }
      }
  }

}
