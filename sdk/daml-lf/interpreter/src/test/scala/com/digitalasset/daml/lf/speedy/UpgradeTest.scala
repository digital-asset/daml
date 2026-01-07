// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, TypeConId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.SValue.SContractId
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.GlobalKeyWithMaintainers
import com.digitalasset.daml.lf.transaction.SerializationVersion.VDev
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ArraySeq

class UpgradeTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks with Inside {

  implicit val pkgId: Ref.PackageId = Ref.PackageId.assertFromString("-no-pkg-")

  private[this] implicit def parserParameters(implicit
      pkgId: Ref.PackageId
  ): ParserParameters[this.type] =
    ParserParameters(pkgId, languageVersion = LanguageVersion.devLfVersion)

  private[this] val compilerConfig = Compiler.Config.Dev

  val ifacePkgId = Ref.PackageId.assertFromString("-iface-")
  private lazy val ifacePkg = {
    implicit def pkgId: Ref.PackageId = ifacePkgId
    p""" metadata ( '-iface-' : '1.0.0' )
    module M {

      record @serializable MyUnit = {};
      interface (this : Iface) = {
        viewtype M:MyUnit;
        method myChoice : Text;

        choice @nonConsuming MyChoice (self) (ctl: Party): Text
          , controllers (Cons @Party [ctl] Nil @Party)
          , observers (Nil @Party)
          to upure @Text (call_method @M:Iface myChoice this);
      };
    }
    """
  }

  val variantPkgIdV1: Ref.PackageId = Ref.PackageId.assertFromString("-variant-v1-")
  private lazy val variantPkgV1 = {
    // unknown variant type constructor
    implicit def pkgId: Ref.PackageId = variantPkgIdV1
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      variant @serializable D = tag : Int64 | label : Text;

      record @serializable T = { sig: Party, obs: Party, data: M:D };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  val variantPkgIdV2: Ref.PackageId = Ref.PackageId.assertFromString("-variant-v2-")
  private lazy val variantPkgV2 = {
    // unknown variant type constructor
    implicit def pkgId: Ref.PackageId = variantPkgIdV2
    p""" metadata ( '-upgrade-test-' : '2.0.0' )
    module M {

      variant @serializable D = label : Text;

      record @serializable T = { sig: Party, obs: Party, data: M:D };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  val enumPkgIdV1: Ref.PackageId = Ref.PackageId.assertFromString("-enum-v1-")
  private lazy val enumPkgV1 = {
    // unknown enum type constructor
    implicit def pkgId: Ref.PackageId = enumPkgIdV1
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      enum @serializable D = Black | White;

      record @serializable T = { sig: Party, obs: Party, data: M:D };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  val enumPkgIdV2: Ref.PackageId = Ref.PackageId.assertFromString("-enum-v2-")
  private lazy val enumPkgV2 = {
    // unknown enum type constructor
    implicit def pkgId: Ref.PackageId = enumPkgIdV2
    p""" metadata ( '-upgrade-test-' : '2.0.0' )
    module M {

      enum @serializable D = White;

      record @serializable T = { sig: Party, obs: Party, data: M:D };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  lazy val utilPkgId = Ref.PackageId.assertFromString("-util-")
  private lazy val utilPkg = {
    implicit def pkgId: Ref.PackageId = pkgId0
    p""" metadata ( '-util-' : '1.0.0' )
    module M {
      val mkParty : Text -> Party =
        \(t:Text) ->
          case TEXT_TO_PARTY t of
            None -> ERROR @Party "none"
          | Some x -> x;

      val mkList: Party -> Option Party -> List Party =
        \(sig: Party) -> \(optSig: Option Party) ->
          case optSig of
            None -> Cons @Party [sig] Nil @Party
          | Some extraSig -> Cons @Party [sig, extraSig] Nil @Party;
    }
    """
  }

  lazy val pkgId0 = Ref.PackageId.assertFromString("-pkg0-")
  private lazy val pkg0 = {
    implicit def pkgId: Ref.PackageId = pkgId0
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  val pkgId1 = Ref.PackageId.assertFromString("-pkg1-")
  private lazy val pkg1 = {
    implicit def pkgId: Ref.PackageId = pkgId1
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v1";
        };
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_create: Text -> Text -> Int64 -> Update (ContractId M:T) =
        \(sig: Text) -> \(obs: Text) -> \(n: Int64) ->
          create @M:T M:T { sig = '-util-':M:mkParty sig, obs = '-util-':M:mkParty obs, aNumber = n };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val mkList: Party -> Option Party -> List Party =
        \(sig: Party) -> \(optSig: Option Party) ->
          case optSig of
            None -> Cons @Party [sig] Nil @Party
          | Some extraSig -> Cons @Party [sig, extraSig] Nil @Party;

    }
    """
  }

  val pkgId2: Ref.PackageId = Ref.PackageId.assertFromString("-pkg2-")
  private lazy val pkg2 = {
    // adds a choice to T
    implicit def pkgId: Ref.PackageId = pkgId2
    p""" metadata ( '-upgrade-test-' : '2.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
        choice NoOp (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {sig} this] Nil @Party,
          observers Nil @Party
          to upure @Unit ();
        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v2";
        };
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val do_exercise: ContractId M:T -> Update Unit  =
        \(cId: ContractId M:T) -> exercise @M:T NoOp cId ();
      }
    """
  }

  val pkgId3: Ref.PackageId = Ref.PackageId.assertFromString("-pkg3-")
  private lazy val pkg3 = {
    // add an optional additional signatory
    implicit def pkgId: Ref.PackageId = pkgId3
    p""" metadata ( '-upgrade-test-' : '3.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64, optSig: Option Party };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (M:T {optSig} this);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
        choice NoOp (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {sig} this] Nil @Party,
          observers Nil @Party
          to upure @Unit ();
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val do_exercise: ContractId M:T -> Update Unit  =
        \(cId: ContractId M:T) -> exercise @M:T NoOp cId ();
      }
    """
  }

  val pkgId4: Ref.PackageId = Ref.PackageId.assertFromString("-pkg4-")
  private lazy val pkg4 = {
    // swap signatories and observers with respect to pkg2
    implicit def pkgId: Ref.PackageId = pkgId4
    p""" metadata ( '-upgrade-test-' : '4.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64, optSig: Option Party };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {obs} this) (None @Party);
        observers '-util-':M:mkList (M:T {sig} this) (None @Party);
        choice NoOp (self) (u: Unit) : Unit,
          controllers '-util-':M:mkList (M:T {sig} this) (None @Party),
          observers Nil @Party
          to upure @Unit ();
        key @Party (M:T {obs} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
      }
    """
  }

  lazy val pkgId5 = Ref.PackageId.assertFromString("-pkg5-")
  private lazy val pkg5 = {
    // renames "aNumber" in pkg0 to "aNumberRenamed"
    implicit def pkgId: Ref.PackageId = pkgId5
    p""" metadata ( '-upgrade-test-' : '5.0.0' )
    module M {

      record @serializable T = { sig: Party, obs: Party, aNumberRenamed: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers '-util-':M:mkList (M:T {obs} this) (None @Party);
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
    }
    """
  }

  lazy val localUpgradePkgId = Ref.PackageId.assertFromString("-local-upgrade-pkg-")
  private lazy val pkgLocalUpgrade = {
    implicit def pkgId: Ref.PackageId = localUpgradePkgId
    p""" metadata ( '-local-upgrade-' : '1.0.0' )
    module M {

      record @serializable T = { sig: Party };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers Nil @Party;

        choice @nonConsuming FetchLocal (self) (u: Unit): '-pkg5-':M:T
          , controllers (Cons @Party [M:T {sig} this] (Nil @Party))
          , observers (Nil @Party)
          to ubind cid: ContractId '-pkg0-':M:T <- create
                 @'-pkg0-':M:T
                 ('-pkg0-':M:T { sig = M:T {sig} this, obs = M:T {sig} this, aNumber = 42 })
             in fetch_template
                 @'-pkg5-':M:T
                 (COERCE_CONTRACT_ID @'-pkg0-':M:T @'-pkg5-':M:T cid);
      };

      val do_fetch_local: ContractId M:T -> Update '-pkg5-':M:T =
        \(cid: ContractId M:T) ->
          exercise @M:T FetchLocal cid ();
    }
    """
  }

  // A package defining a template which has a contract ID in the create argument.
  lazy val cidInCreateArgPkgId1 = Ref.PackageId.assertFromString("-cid-in-create-arg-pkg1-")
  private lazy val cidInCreateArgPkg1 = {
    implicit def pkgId: Ref.PackageId = cidInCreateArgPkgId1
    p""" metadata ( '-upgrade-test-cid-in-create-arg-' : '1.0.0' )
    module M {

      record @serializable U = { sig: Party };
      template (this: U) = {
        precondition True;
        signatories '-util-':M:mkList (M:U {sig} this) (None @Party);
        observers Nil @Party;
      };

      record @serializable T = { sig: Party, cid: ContractId M:U };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers Nil @Party;

        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v1";
        };
      };
    }
    """
  }

  // A valid upgrade of cidInCreateArgPkg2
  lazy val cidInCreateArgPkgId2 = Ref.PackageId.assertFromString("-cid-in-create-arg-pkg2-")
  private lazy val cidInCreateArgPkg2 = {
    implicit def pkgId: Ref.PackageId = cidInCreateArgPkgId2
    p""" metadata ( '-upgrade-test-cid-in-create-arg-' : '2.0.0' )
    module M {

      record @serializable U = { sig: Party };
      template (this: U) = {
        precondition True;
        signatories '-util-':M:mkList (M:U {sig} this) (None @Party);
        observers Nil @Party;
      };

      record @serializable T = { sig: Party, cid: ContractId M:U, extra : Option Int64 };
      template (this: T) = {
        precondition True;
        signatories '-util-':M:mkList (M:T {sig} this) (None @Party);
        observers Nil @Party;

        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v2";
        };
      };
    }
    """
  }

  val pkgName = {
    assert(
      pkg1.pkgName == pkg2.pkgName && pkg2.pkgName == pkg3.pkgName && pkg3.pkgName == pkg4.pkgName
    )
    pkg1.pkgName
  }

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")

  val theCid = ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  type Success = (SValue, Value)

  def go(
      e: Expr,
      availablePackages: Map[Ref.PackageId, Package],
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
  ): Either[SError, Success] = {
    val machine = buildMachine(e, availablePackages, packageResolution)
    go(machine)
  }

  // The given contractValue is wrapped as a contract available for ledger-fetch
  def go(
      e: Expr,
      availablePackages: Map[Ref.PackageId, Package],
      globalContractPackageName: Ref.PackageName,
      globalContractTemplateId: Identifier,
      globalContractArg: Value,
      globalContractSignatories: Iterable[Ref.Party],
      globalContractObservers: Iterable[Ref.Party],
      globalContractKeyWithMaintainers: Option[GlobalKeyWithMaintainers],
      hashingMethod: ContractId => Hash.HashingMethod,
  ): Either[SError, Success] = {

    val pkgs = PureCompiledPackages.assertBuild(availablePackages, compilerConfig)

    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args = ArraySeq[SValue](SContractId(theCid))
    val sexprToEval: SExpr = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    SpeedyTestLib
      .run(
        machine,
        getContract = Map(
          theCid -> TransactionBuilder
            .fatContractInstanceWithDummyDefaults(
              VDev,
              globalContractPackageName,
              globalContractTemplateId,
              globalContractArg,
              globalContractSignatories,
              globalContractObservers,
              globalContractKeyWithMaintainers,
            )
        ),
        hashingMethod = hashingMethod,
      )
      .map(sv => (sv, sv.toNormalizedValue))
  }

  def go(machine: Speedy.UpdateMachine): Either[SError, Success] =
    SpeedyTestLib
      .run(machine)
      .map(sv => (sv, sv.toNormalizedValue))

  def buildMachine(
      e: Expr,
      availablePackages: Map[Ref.PackageId, Package],
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
  ): Speedy.UpdateMachine = {
    val pkgs = PureCompiledPackages.assertBuild(availablePackages, compilerConfig)
    val sexprToEval: SExpr = pkgs.compiler.unsafeCompile(e)
    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    Speedy.Machine.fromUpdateSExpr(
      pkgs,
      seed,
      sexprToEval,
      Set(alice, bob),
      packageResolution = packageResolution,
    )
  }

  def makeRecord(fields: Value*): Value = {
    ValueRecord(
      None,
      fields.map { v => (None, v) }.to(ImmArray),
    )
  }

  val v1_base =
    makeRecord(
      ValueParty(alice),
      ValueParty(bob),
      ValueInt64(100),
    )

  val v1_key =
    GlobalKeyWithMaintainers.assertBuild(i"'-pkg1-':M:T", ValueParty(alice), Set(alice), pkgName)

  val v2_key =
    GlobalKeyWithMaintainers.assertBuild(i"'-pkg2-':M:T", ValueParty(alice), Set(alice), pkgName)

  "upgrade attempted" - {

    "missing optional field -- None is manufactured" in {

      val v_missingField =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
        )
      val v_missingFieldKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg2-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val sv_extendedWithNone =
        SValue.SRecord(
          i"'-pkg3-':M:T",
          ImmArray(n"sig", n"obs", n"aNumber", n"optSig"),
          ArraySeq(
            SValue.SParty(alice),
            SValue.SParty(bob),
            SValue.SInt64(100),
            SValue.SOptional(None),
          ),
        )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId3 -> pkg3,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        inside(
          go(
            e"'-pkg3-':M:do_fetch",
            availablePackages = availablePackages,
            globalContractPackageName = pkgName,
            globalContractTemplateId = i"'-pkg2-':M:T",
            globalContractArg = v_missingField,
            globalContractSignatories = List(alice),
            globalContractObservers = List(bob),
            globalContractKeyWithMaintainers = Some(v_missingFieldKey),
            hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
          )
        ) { case Right((sv, v)) =>
          sv shouldBe sv_extendedWithNone
          v shouldBe v_missingField
        }
      }
    }

    "missing non-optional field -- should be rejected" in {
      // should be caught by package upgradability check
      val v_missingField = makeRecord(ValueParty(alice))
      val v_missingFieldKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg1-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      inside(
        go(
          e"'-pkg1-':M:do_fetch",
          // We cannot test the case where the creation package is unavailable because this test case tests the case
          // when we try to read it back using the same package version.
          availablePackages = Map(
            utilPkgId -> utilPkg,
            ifacePkgId -> ifacePkg,
            pkgId1 -> pkg1,
          ),
          globalContractPackageName = pkgName,
          globalContractTemplateId = i"'-pkg1-':M:T",
          globalContractArg = v_missingField,
          globalContractSignatories = List(alice),
          globalContractObservers = List.empty,
          globalContractKeyWithMaintainers = Some(v_missingFieldKey),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )
      ) {
        case Left(
              SError.SErrorDamlException(
                IE.Upgrade(
                  IE.Upgrade.TranslationFailed(
                    Some(coid),
                    srcTemplateId,
                    dstTemplateId,
                    createArg,
                    IE.Upgrade.TranslationFailed.TypeMismatch(expectedType, actualValue, message),
                  )
                )
              )
            ) =>
          coid shouldBe theCid
          srcTemplateId shouldBe i"'-pkg1-':M:T"
          dstTemplateId shouldBe i"'-pkg1-':M:T"
          createArg shouldBe v_missingField
          expectedType shouldBe TTyCon(i"'-pkg1-':M:T")
          actualValue shouldBe v_missingField
          message should include(
            "Unexpected non-optional extra template field type encountered during upgrading."
          )
      }
    }

    "mismatching qualified name -- should be rejected" in {
      val v =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
        )
      def key(templateId: Ref.TypeConId) = GlobalKeyWithMaintainers.assertBuild(
        templateId,
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId3 -> pkg3,
        ),
      )

      val negativeTestCase = i"'-pkg2-':M:T"
      forEvery(availablePackagesCases) { availablePackages =>
        go(
          e"'-pkg3-':M:do_fetch",
          availablePackages = availablePackages,
          globalContractPackageName = pkgName,
          globalContractTemplateId = negativeTestCase,
          globalContractArg = v,
          globalContractSignatories = List(alice),
          globalContractObservers = List(bob),
          globalContractKeyWithMaintainers = Some(key(i"'-pkg2-':M:T")),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        ) shouldBe a[
          Right[_, _]
        ]
      }

      val positiveTestCases = Table("tyCon", i"'-pkg2-':M1:T", i"'-pkg2-':M2:T")
      forEvery(positiveTestCases) { tyCon =>
        forEvery(availablePackagesCases) { availablePackages =>
          inside(
            go(
              e"'-pkg3-':M:do_fetch",
              availablePackages = availablePackages,
              globalContractPackageName = pkgName,
              globalContractTemplateId = tyCon,
              globalContractArg = v,
              globalContractSignatories = List(alice),
              globalContractObservers = List(bob),
              globalContractKeyWithMaintainers = Some(key(tyCon)),
              hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
            )
          ) { case Left(SError.SErrorDamlException(error)) =>
            error shouldBe a[IE.WronglyTypedContract]
          }
        }
      }
    }

    "renamed template field in local contract -- should be rejected" in {
      inside(
        go(
          e"'-local-upgrade-pkg-':M:do_fetch_local",
          // We cannot test the case where the creation package is unavailable because this test case tests the upgrade
          // of a local contract, which requires the creation package in order to be created.
          availablePackages = Map(
            utilPkgId -> utilPkg,
            pkgId0 -> pkg0,
            pkgId5 -> pkg5,
            localUpgradePkgId -> pkgLocalUpgrade,
          ),
          globalContractPackageName = pkgLocalUpgrade.pkgName,
          globalContractTemplateId = i"'-local-upgrade-pkg-':M:T",
          globalContractArg = makeRecord(ValueParty(alice)),
          globalContractSignatories = List(alice),
          globalContractObservers = List.empty,
          globalContractKeyWithMaintainers = None,
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )
      ) {
        case Left(
              SError.SErrorDamlException(
                IE.Upgrade(
                  IE.Upgrade.AuthenticationFailed(_, srcTemplateId, dstTemplateId, createArg, _)
                )
              )
            ) =>
          srcTemplateId shouldBe i"'-pkg0-':M:T"
          dstTemplateId shouldBe i"'-pkg5-':M:T"
          createArg shouldBe makeRecord(ValueParty(alice), ValueParty(alice), ValueInt64(42))
      }
    }

    "inconsistent packageName in FatContractInstance -- should be rejected" in {
      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId0 -> pkg0,
          pkgId1 -> pkg1,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
        ),
      )

      val wrongPackageName = Ref.PackageName.assertFromString("wrong")

      forEvery(availablePackagesCases) { availablePackages =>
        inside(
          go(
            e"'-pkg1-':M:do_fetch",
            // We cannot test the case where the creation package is unavailable because this test case tests the upgrade
            // of a local contract, which requires the creation package in order to be created.
            availablePackages = availablePackages,
            globalContractPackageName = wrongPackageName,
            globalContractTemplateId = i"'-pkg0-':M:T",
            globalContractArg = v1_base,
            globalContractSignatories = List(alice),
            globalContractObservers = List.empty,
            globalContractKeyWithMaintainers = None,
            hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
          )
        ) {
          case Left(
                SError.SErrorDamlException(
                  IE.Upgrade(
                    IE.Upgrade.ValidationFailed(
                      coid,
                      srcTemplateId,
                      dstTemplateId,
                      srcPackageName,
                      dstPackageName,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                    )
                  )
                )
              ) =>
            coid shouldBe theCid
            srcTemplateId shouldBe i"'-pkg0-':M:T"
            dstTemplateId shouldBe i"'-pkg1-':M:T"
            srcPackageName shouldBe wrongPackageName
            dstPackageName shouldBe pkg1.pkgName
        }
      }
    }
  }

  "downgrade attempted" - {

    "correct fields" in {

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        val res =
          go(
            e"'-pkg1-':M:do_fetch",
            availablePackages = availablePackages,
            globalContractPackageName = pkgName,
            globalContractTemplateId = i"'-pkg2-':M:T",
            globalContractArg = v1_base,
            globalContractSignatories = List(alice),
            globalContractObservers = List(bob),
            globalContractKeyWithMaintainers = Some(v2_key),
            hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
          )

        inside(res) { case Right((_, v)) =>
          v shouldBe v1_base
        }
      }
    }

    "extra field (text)" in {
      // should be caught by package upgradability check

      val v1_extraText =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueText("extra"),
        )
      val v1_extraTextKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg1-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val res =
        go(
          e"'-pkg1-':M:do_fetch",
          // We cannot test the case where the creation package is unavailable because this test cases tests the case
          // when we try to read it back using the same package version.
          availablePackages = Map(
            utilPkgId -> utilPkg,
            ifacePkgId -> ifacePkg,
            pkgId1 -> pkg1,
          ),
          globalContractPackageName = pkgName,
          globalContractTemplateId = i"'-pkg1-':M:T",
          globalContractArg = v1_extraText,
          globalContractSignatories = List(alice),
          globalContractObservers = List(bob),
          globalContractKeyWithMaintainers = Some(v1_extraTextKey),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )

      inside(res) {
        case Left(
              SError.SErrorDamlException(
                IE.Upgrade(
                  IE.Upgrade.TranslationFailed(
                    Some(coid),
                    srcTemplateId,
                    dstTemplateId,
                    createArg,
                    IE.Upgrade.TranslationFailed.TypeMismatch(expectedType, actualValue, message),
                  )
                )
              )
            ) =>
          coid shouldBe theCid
          srcTemplateId shouldBe i"'-pkg1-':M:T"
          dstTemplateId shouldBe i"'-pkg1-':M:T"
          createArg shouldBe v1_extraText
          expectedType shouldBe TTyCon(i"'-pkg1-':M:T")
          actualValue shouldBe v1_extraText
          message should include(
            "Found non-optional extra field at index 3, cannot remove for downgrading."
          )
      }
    }

    "extra field (Some) - cannot be dropped" in {

      val v1_extraSome =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueOptional(Some(ValueParty(bob))),
        )
      val v1_extraSomeKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg3-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        val res = go(
          e"'-pkg2-':M:do_fetch",
          availablePackages = availablePackages,
          globalContractPackageName = pkgName,
          globalContractTemplateId = i"'-pkg3-':M:T",
          globalContractArg = v1_extraSome,
          globalContractSignatories = List(alice),
          globalContractObservers = List(bob),
          globalContractKeyWithMaintainers = Some(v1_extraSomeKey),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )

        inside(res) {
          case Left(
                SError.SErrorDamlException(
                  IE.Upgrade(
                    IE.Upgrade.TranslationFailed(
                      Some(coid),
                      srcTemplateId,
                      dstTemplateId,
                      createArg,
                      IE.Upgrade.TranslationFailed.TypeMismatch(expectedType, actualValue, message),
                    )
                  )
                )
              ) =>
            coid shouldBe theCid
            srcTemplateId shouldBe i"'-pkg3-':M:T"
            dstTemplateId shouldBe i"'-pkg2-':M:T"
            createArg shouldBe v1_extraSome
            expectedType shouldBe TTyCon(i"'-pkg2-':M:T")
            actualValue shouldBe v1_extraSome
            message should include(
              "Found an optional contract field with a value of Some at index 3, may not be dropped during downgrading."
            )
        }
      }
    }

    "extra field (None) - OK, downgrade allowed with legacy contracts" in {

      val v1_extraNone =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          Value.ValueOptional(None),
        )
      val v1_extraNoneKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg3-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        val res =
          go(
            e"'-pkg1-':M:do_fetch",
            availablePackages = availablePackages,
            globalContractPackageName = pkgName,
            globalContractTemplateId = i"'-pkg3-':M:T",
            globalContractArg = v1_extraNone,
            globalContractSignatories = List(alice),
            globalContractObservers = List(bob),
            globalContractKeyWithMaintainers = Some(v1_extraNoneKey),
            hashingMethod = _ => Hash.HashingMethod.Legacy,
          )

        inside(res) { case Right((_, v)) =>
          v shouldBe v1_base
        }
      }
    }

    "extra field (None) -- should be rejected for >v10 contracts" in {

      val v1_extraNone =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          Value.ValueOptional(None),
        )
      val v1_extraNoneKey = GlobalKeyWithMaintainers.assertBuild(
        i"'-pkg3-':M:T",
        ValueParty(alice),
        Set(alice),
        pkgName,
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
        ),
      )

      val gtV10HahsingMethods = Table(
        "hashingMethod",
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(availablePackagesCases) { availablePackages =>
        forEvery(gtV10HahsingMethods) { hashingMethod =>
          val res =
            go(
              e"'-pkg1-':M:do_fetch",
              availablePackages = availablePackages,
              globalContractPackageName = pkgName,
              globalContractTemplateId = i"'-pkg3-':M:T",
              globalContractArg = v1_extraNone,
              globalContractSignatories = List(alice),
              globalContractObservers = List(bob),
              globalContractKeyWithMaintainers = Some(v1_extraNoneKey),
              hashingMethod = _ => hashingMethod,
            )

          inside(res) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Upgrade(
                      IE.Upgrade.TranslationFailed(
                        Some(coid),
                        srcTemplateId,
                        dstTemplateId,
                        createArg,
                        error,
                      )
                    )
                  )
                ) =>
              coid shouldBe theCid
              srcTemplateId shouldBe i"'-pkg3-':M:T"
              dstTemplateId shouldBe i"'-pkg1-':M:T"
              createArg shouldBe v1_extraNone
              error shouldBe a[IE.Upgrade.TranslationFailed.InvalidValue]
          }
        }
      }
    }

    "unknown variant constructor -- should be rejected" in {
      val tag = ValueInt64(42)
      val v1Arg = makeRecord(
        ValueParty(alice),
        ValueParty(bob),
        ValueVariant(None, Ref.Name.assertFromString("tag"), tag),
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          variantPkgIdV1 -> variantPkgV1,
          variantPkgIdV2 -> variantPkgV2,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          variantPkgIdV2 -> variantPkgV2,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        inside(
          go(
            e"'-variant-v2-':M:do_fetch",
            availablePackages = availablePackages,
            globalContractPackageName = pkgName,
            globalContractTemplateId = i"'-variant-v1-':M:T",
            globalContractArg = v1Arg,
            globalContractSignatories = List(alice),
            globalContractObservers = List(bob),
            globalContractKeyWithMaintainers = None,
            hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
          )
        ) {
          case Left(
                SError.SErrorDamlException(
                  IE.Upgrade(
                    IE.Upgrade.TranslationFailed(
                      Some(coid),
                      srcTemplateId,
                      dstTemplateId,
                      createArg,
                      error,
                    )
                  )
                )
              ) =>
            coid shouldBe theCid
            srcTemplateId shouldBe i"'-variant-v1-':M:T"
            dstTemplateId shouldBe i"'-variant-v2-':M:T"
            createArg shouldBe v1Arg
            error shouldBe a[IE.Upgrade.TranslationFailed.LookupError]
        }
      }
    }

    "unknown enum constructor -- should be rejected" in {
      val black = ValueEnum(None, Ref.Name.assertFromString("Black"))
      val v1Arg = makeRecord(
        ValueParty(alice),
        ValueParty(bob),
        black,
      )

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          enumPkgIdV1 -> enumPkgV1,
          enumPkgIdV2 -> enumPkgV2,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          enumPkgIdV2 -> enumPkgV2,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        inside(
          go(
            e"'-enum-v2-':M:do_fetch",
            availablePackages = availablePackages,
            globalContractPackageName = pkgName,
            globalContractTemplateId = i"'-enum-v1-':M:T",
            globalContractArg = v1Arg,
            globalContractSignatories = List(alice),
            globalContractObservers = List(bob),
            globalContractKeyWithMaintainers = None,
            hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
          )
        ) {
          case Left(
                SError.SErrorDamlException(
                  IE.Upgrade(
                    IE.Upgrade.TranslationFailed(
                      Some(coid),
                      srcTemplateId,
                      dstTemplateId,
                      createArg,
                      error,
                    )
                  )
                )
              ) =>
            coid shouldBe theCid
            srcTemplateId shouldBe i"'-enum-v1-':M:T"
            dstTemplateId shouldBe i"'-enum-v2-':M:T"
            createArg shouldBe v1Arg
            error shouldBe a[IE.Upgrade.TranslationFailed.LookupError]
        }
      }
    }
  }

  "upgrade" - {
    "be able to fetch a same contract using different versions" in {

      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        // The following code is not properly typed, but emulates two commands that fetch the same contract using different versions.
        val res = go(
          e"""\(cid: ContractId '-pkg2-':M:T) ->
               ubind
                 x1: Unit <- '-pkg2-':M:do_fetch cid;
                 x2: Unit <- '-pkg3-':M:do_fetch cid
               in upure @Unit ()
          """,
          availablePackages = availablePackages,
          globalContractPackageName = pkgName,
          globalContractTemplateId = i"'-pkg1-':M:T",
          globalContractArg = v1_base,
          globalContractSignatories = List(alice),
          globalContractObservers = List(bob),
          globalContractKeyWithMaintainers = Some(v1_key),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )
        res shouldBe a[Right[_, _]]
      }
    }

    "be able to fetch a locally created contract using different versions" in {
      val res = go(
        e"""ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: '-pkg2-':M:T <- fetch_template @'-pkg2-':M:T cid
            in upure @(ContractId '-pkg1-':M:T) cid
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
      )
      inside(res) { case Right((_, v)) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to fetch a locally created contract with a contract ID in its create argument using different versions" in {
      val res = go(
        e"""ubind
              ucid: ContractId '-cid-in-create-arg-pkg1-':M:U <- create
                 @'-cid-in-create-arg-pkg1-':M:U
                 ('-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice" });
              cid: ContractId '-cid-in-create-arg-pkg1-':M:T <- create
                 @'-cid-in-create-arg-pkg1-':M:T
                 ('-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice", cid = ucid })
            in fetch_template
                 @'-cid-in-create-arg-pkg2-':M:T
                 (COERCE_CONTRACT_ID @'-cid-in-create-arg-pkg1-':M:T @'-cid-in-create-arg-pkg2-':M:T cid)
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          cidInCreateArgPkgId1 -> cidInCreateArgPkg1,
          cidInCreateArgPkgId2 -> cidInCreateArgPkg2,
        ),
      )
      res shouldBe a[Right[_, _]]
    }

    "be able to fetch by interface a locally created contract with a contract ID in its create argument using different versions" in {
      val res = go(
        e"""ubind
              ucid: ContractId '-cid-in-create-arg-pkg1-':M:U <- create
                 @'-cid-in-create-arg-pkg1-':M:U
                 ('-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice" });
              cid: ContractId '-cid-in-create-arg-pkg1-':M:T <- create
                 @'-cid-in-create-arg-pkg1-':M:T
                 ('-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice", cid = ucid });
              _: '-iface-':M:Iface <- fetch_interface
                 @'-iface-':M:Iface
                 (COERCE_CONTRACT_ID @'-cid-in-create-arg-pkg1-':M:T @'-iface-':M:Iface cid)
            in upure @Unit ()
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          cidInCreateArgPkgId1 -> cidInCreateArgPkg1,
          cidInCreateArgPkgId2 -> cidInCreateArgPkg2,
        ),
        packageResolution = Map(
          cidInCreateArgPkg2.pkgName -> cidInCreateArgPkgId2
        ),
      )
      res shouldBe a[Right[_, _]]
    }

    "be able to fetch by key a locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-util-':M:mkParty "alice"
            in ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: '-pkg2-':M:T <- fetch_by_key @'-pkg2-':M:T alice
            in upure @(ContractId '-pkg1-':M:T) cid
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
      )
      inside(res) { case Right((_, v)) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise a locally created contract using different versions" in {
      val res = go(
        e"""ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: Unit <- exercise @'-pkg2-':M:T NoOp cid ()
            in upure @(ContractId '-pkg1-':M:T) cid
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
      )
      inside(res) { case Right((_, v)) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise by key a locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-util-':M:mkParty "alice"
            in ubind
                 cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
                 _: Unit <- exercise_by_key @'-pkg2-':M:T NoOp alice ()
               in upure @(ContractId '-pkg1-':M:T) cid
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
      )
      inside(res) { case Right((_, v)) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise by interface locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-util-':M:mkParty "alice"
            in ubind
                 cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
                 res: Text <- exercise_interface @'-iface-':M:Iface
                                MyChoice
                                (COERCE_CONTRACT_ID @'-pkg1-':M:T @'-iface-':M:Iface cid)
                                alice
               in upure @(ContractId '-pkg1-':M:T) cid
          """,
        // We cannot test the case where the creation package is unavailable because this the LF code under test creates
        // the contract itself and thus needs the creation package.
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
        packageResolution = Map(Ref.PackageName.assertFromString("-upgrade-test-") -> pkgId2),
      )
      inside(res) { case Right((_, v)) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to upgrade a template using from_interface" in {
      val res = go(
        e"""let t: '-pkg1-':M:T = '-pkg1-':M:T { sig = '-util-':M:mkParty "alice", obs = '-util-':M:mkParty "bob", aNumber = 100 }
            in let i : '-iface-':M:Iface = to_interface @'-pkg1-':M:T @'-iface-':M:Iface t
            in upure @(Option '-pkg2-':M:T) (from_interface @'-iface-':M:Iface@'-pkg2-':M:T i)
        """,
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
        packageResolution = Map(Ref.PackageName.assertFromString("-upgrade-test-") -> pkgId2),
      )
      inside(res) { case Right((_, ValueOptional(v))) =>
        v shouldBe defined
      }
    }

    "be able to upgrade a local template with a contract ID in its create arg using from_interface" in {
      val res = go(
        e"""ubind
              ucid: ContractId '-cid-in-create-arg-pkg1-':M:U <- create
                 @'-cid-in-create-arg-pkg1-':M:U
                 ('-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice" })
            in let t: '-cid-in-create-arg-pkg1-':M:T =
                         '-cid-in-create-arg-pkg1-':M:T { sig = '-util-':M:mkParty "alice", cid = ucid }
               in upure @(Option '-cid-in-create-arg-pkg2-':M:T)
                        (from_interface @'-iface-':M:Iface@'-cid-in-create-arg-pkg2-':M:T
                           (to_interface @'-cid-in-create-arg-pkg1-':M:T @'-iface-':M:Iface t))
          """,
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          cidInCreateArgPkgId1 -> cidInCreateArgPkg1,
          cidInCreateArgPkgId2 -> cidInCreateArgPkg2,
        ),
        packageResolution = Map(
          cidInCreateArgPkg2.pkgName -> cidInCreateArgPkgId2
        ),
      )
      inside(res) { case Right((_, ValueOptional(v))) =>
        v shouldBe defined
      }
    }

    "fail to upgrade template using unsafe_from_interface" in {
      val machine = buildMachine(
        e"""let alice : Party = '-util-':M:mkParty "alice"
            in ubind
              cidT : ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              t : '-pkg1-':M:T <- '-pkg1-':M:do_fetch cidT
            in let cidI : ContractId '-iface-':M:Iface = COERCE_CONTRACT_ID @'-pkg0-':M:T @'-iface-':M:Iface cidT
            in let i : '-iface-':M:Iface = to_interface @'-pkg1-':M:T @'-iface-':M:Iface t
            in let _ : '-pkg1-':M:T = unsafe_from_interface @'-iface-':M:Iface @'-pkg1-':M:T cidI i
            in let _ : '-pkg2-':M:T = unsafe_from_interface @'-iface-':M:Iface @'-pkg2-':M:T cidI i
            in upure @Unit ()
        """,
        availablePackages = Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
        ),
        packageResolution = Map(Ref.PackageName.assertFromString("-upgrade-test-") -> pkgId2),
      )
      inside(go(machine)) {
        case Left(SErrorDamlException(IE.WronglyTypedContract(_, expected, actual))) =>
          expected shouldBe TypeConId.assertFromString("-pkg2-:M:T")
          actual shouldBe TypeConId.assertFromString("-pkg1-:M:T")
      }
      val warnings = machine.warningLog.iterator.toSeq.filter(warning =>
        warning.message.contains("unsafeFromInterface is deprecated")
      )
      warnings.size shouldBe 2
    }

    "do recompute and check immutability of meta data when using different versions" in {
      val availablePackagesCases = Table(
        "availablePackages",
        // with creation package available
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId1 -> pkg1,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
        // with creation package unavailable
        Map(
          utilPkgId -> utilPkg,
          ifacePkgId -> ifacePkg,
          pkgId2 -> pkg2,
          pkgId3 -> pkg3,
        ),
      )

      forEvery(availablePackagesCases) { availablePackages =>
        // The following code is not properly typed, but emulates two commands that fetch a same contract using different versions.
        val res: Either[SError, (SValue, Value)] = go(
          e"""\(cid: ContractId '-pkg2-':M:T) ->
               ubind
                 x1: Unit <- '-pkg2-':M:do_fetch cid;
                 x2: Unit <- '-pkg3-':M:do_fetch cid
               in upure @Unit ()
          """,
          availablePackages = availablePackages,
          globalContractPackageName = pkgName,
          globalContractTemplateId = i"'-pkg1-':M:T",
          globalContractArg = v1_base,
          globalContractSignatories = List(alice),
          globalContractObservers = List(bob),
          globalContractKeyWithMaintainers = Some(v1_key),
          hashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
        )
        res shouldBe a[Right[_, _]]
      }
    }
  }
}
