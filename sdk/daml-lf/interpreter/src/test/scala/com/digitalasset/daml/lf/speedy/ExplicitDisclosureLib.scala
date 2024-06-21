// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref.{IdString, Party}
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Struct}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion}
import com.digitalasset.daml.lf.speedy.SExpr.SEMakeClo
import com.digitalasset.daml.lf.speedy.SValue.SToken
import com.digitalasset.daml.lf.speedy.Speedy.{CachedKey, ContractInfo}
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  TransactionVersion,
  Versioned,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractId, ContractInstance}
import org.scalatest.matchers.{MatchResult, Matcher}

/** Shared test data and functions for testing explicit disclosure.
  */
private[lf] class ExplicitDisclosureLib(majorLanguageVersion: LanguageMajorVersion) {

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  val testKeyName: String = "test-key"
  private val pkg =
    p""" metadata ( '-package-' : '1.0.0' )
       module TestMod {

         record @serializable Key = { label: Text, maintainers: List Party };

         record @serializable House = { owner: Party, key_maintainer: Party };
         template(this: House) = {
           precondition True;
           signatories (TestMod:listOf @Party (TestMod:House {owner} this));
           observers (Nil @Party);

           choice Destroy (self) (arg: Unit): Unit,
             controllers (TestMod:listOf @Party (TestMod:House {owner} this)),
             observers Nil @Party
             to upure @Unit ();

           key @TestMod:Key
              (TestMod:Key { label = "test-key", maintainers = (TestMod:listOf @Party (TestMod:House {key_maintainer} this)) })
              (\(key: TestMod:Key) -> (TestMod:Key {maintainers} key));
         };

         record @serializable Cave = { owner: Party };
         template(this: Cave) = {
           precondition True;
           signatories (TestMod:listOf @Party (TestMod:Cave {owner} this));
           observers (Nil @Party);

           choice Destroy (self) (arg: Unit): Unit,
             controllers (TestMod:listOf @Party (TestMod:Cave {owner} this)),
             observers Nil @Party
             to upure @Unit ();
         };

         val destroyHouse: ContractId TestMod:House -> Update Unit =
           \(contractId: ContractId TestMod:House) ->
             exercise @TestMod:House Destroy contractId ();

         val destroyCave: ContractId TestMod:Cave -> Update Unit =
           \(contractId: ContractId TestMod:Cave) ->
             exercise @TestMod:Cave Destroy contractId ();

         val listOf: forall(t:*). t -> List t =
           /\(t:*). \(x: t) ->
             Cons @t [x] (Nil @t);

         val optToList: forall(t:*). Option t -> List t  =
           /\(t:*). \(opt: Option t) ->
             case opt of
                 None -> Nil @t
               | Some x -> Cons @t [x] (Nil @t);
       }
       """
  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(pkg)
  val maintainerParty: IdString.Party = Ref.Party.assertFromString("maintainerParty")
  val ledgerParty: IdString.Party = Ref.Party.assertFromString("ledgerParty")
  val disclosureParty: IdString.Party = Ref.Party.assertFromString("disclosureParty")
  val contractId: ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val ledgerContractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-ledger-contract-id"))
  val disclosureContractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-disclosure-contract-id"))
  val altDisclosureContractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-alternative-disclosure-contract-id"))
  val invalidTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Invalid")
  val somePackageName: Ref.PackageName = Ref.PackageName.assertFromString("package-name")
  val houseTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val houseTemplateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val caveTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Cave")
  val caveTemplateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Cave")
  val keyType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Key")
  val contractKey: GlobalKey = buildContractKey(maintainerParty, pkg.pkgName)
  val contractSStructKey: SValue =
    SValue.SStruct(
      fieldNames =
        Struct.assertFromNameSeq(Seq("globalKey", "maintainers").map(Ref.Name.assertFromString)),
      values = ArrayList(
        buildContractSKey(maintainerParty),
        SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainerParty)))),
      ),
    )
  val ledgerHouseContract: Value.VersionedContractInstance =
    buildContract(ledgerParty, maintainerParty)
  val ledgerCaveContract: Value.VersionedContractInstance =
    buildContract(ledgerParty, maintainerParty, templateId = caveTemplateId)
  val disclosedCaveContractNoHash: (Value.ContractId, Speedy.ContractInfo) =
    contractId -> buildDisclosedCaveContract(disclosureParty)
  val disclosedHouseContract: (Value.ContractId, Speedy.ContractInfo) =
    disclosureContractId -> buildDisclosedHouseContract(disclosureParty, maintainerParty)
  val disclosedCaveContract: (Value.ContractId, Speedy.ContractInfo) =
    contractId -> buildDisclosedCaveContract(disclosureParty)

  def buildDisclosedHouseContract(
      owner: Party,
      maintainer: Party,
      packageName: Ref.PackageName = pkg.pkgName,
      packageVersion: Option[Ref.PackageVersion] = pkg.pkgVersion,
      templateId: Ref.Identifier = houseTemplateId,
      withKey: Boolean = true,
      label: String = testKeyName,
  ): Speedy.ContractInfo = {
    val cachedKey: Option[Speedy.CachedKey] =
      if (withKey)
        Some(
          Speedy.CachedKey(
            packageName,
            globalKeyWithMaintainers = GlobalKeyWithMaintainers(
              buildContractKey(maintainer, packageName, label),
              Set(maintainer),
            ),
            key = buildContractSKey(maintainer),
          )
        )
      else
        None
    Speedy.ContractInfo(
      version = TransactionVersion.maxVersion,
      packageName = packageName,
      packageVersion = packageVersion,
      templateId = templateId,
      value = SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner"), Ref.Name.assertFromString("key_maintainer")),
        ArrayList(SValue.SParty(owner), SValue.SParty(maintainer)),
      ),
      signatories = Set(owner, maintainer),
      observers = Set.empty,
      keyOpt = cachedKey,
    )
  }

  def buildDisclosedCaveContract(
      owner: Party,
      packageName: Ref.PackageName = pkg.pkgName,
      packageVersion: Option[Ref.PackageVersion] = pkg.pkgVersion,
      templateId: Ref.Identifier = caveTemplateId,
  ): Speedy.ContractInfo = {
    Speedy.ContractInfo(
      version = TransactionVersion.maxVersion,
      packageName = packageName,
      packageVersion = packageVersion,
      templateId = templateId,
      value = SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner")),
        ArrayList(SValue.SParty(owner)),
      ),
      signatories = Set(owner),
      observers = Set.empty,
      keyOpt = None,
    )
  }

  def buildContractKeyValue(maintainer: Party, label: String = testKeyName) =
    Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueText(label),
        None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainer)))),
      ),
    )

  def buildContractKey(
      maintainer: Party,
      packageName: Ref.PackageName,
      label: String = testKeyName,
  ): GlobalKey =
    GlobalKey.assertBuild(houseTemplateType, buildContractKeyValue(maintainer, label), packageName)

  def buildContractSKey(maintainer: Party, label: String = testKeyName): SValue =
    SValue.SRecord(
      keyType,
      ImmArray("label", "maintainers").map(Ref.Name.assertFromString),
      ArrayList(
        SValue.SText(label),
        SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainer)))),
      ),
    )

  def buildContract(
      owner: Party,
      maintainer: Party,
      packageName: Ref.PackageName = pkg.pkgName,
      packageVersion: Option[Ref.PackageVersion] = pkg.pkgVersion,
      templateId: Ref.Identifier = houseTemplateId,
  ): Versioned[ContractInstance] = {
    val contractFields = templateId match {
      case `caveTemplateId` =>
        ImmArray(
          None -> Value.ValueParty(owner)
        )

      case `houseTemplateId` =>
        ImmArray(
          None -> Value.ValueParty(owner),
          None -> Value.ValueParty(maintainer),
        )

      case _ =>
        throw new RuntimeException(
          s"Unknown template ID $templateId - unable to determine the contract fields"
        )
    }

    Versioned(
      TransactionVersion.minVersion,
      Value.ContractInstance(
        packageName = packageName,
        packageVersion = packageVersion,
        template = templateId,
        arg = Value.ValueRecord(None, contractFields),
      ),
    )
  }

  def buildHouseContractInfo(
      signatory: Party,
      maintainer: Party,
      packageName: Ref.PackageName = pkg.pkgName,
      packageVersion: Option[Ref.PackageVersion] = pkg.pkgVersion,
      templateId: Ref.Identifier = houseTemplateId,
      withKey: Boolean = true,
      label: String = testKeyName,
  ): ContractInfo = {
    val contract = SValue.SRecord(
      templateId,
      ImmArray("label", "maintainers").map(Ref.Name.assertFromString),
      ArrayList(
        SValue.SText(label),
        SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainer)))),
      ),
    )
    val mbKey =
      if (withKey)
        Some(
          CachedKey(
            packageName = packageName,
            GlobalKeyWithMaintainers
              .assertBuild(templateId, contract.toUnnormalizedValue, Set(maintainer), packageName),
            contract,
          )
        )
      else None

    ContractInfo(
      version = TransactionVersion.minVersion,
      packageName = packageName,
      packageVersion = packageVersion,
      templateId = templateId,
      value = contract,
      signatories = Set(signatory),
      observers = Set.empty,
      keyOpt = mbKey,
    )
  }

  val getOwner: Value => Option[Party] = {
    case Value.ValueRecord(_, ImmArray(_ -> Value.ValueParty(owner), _)) =>
      Some(owner)

    case Value.ValueRecord(_, ImmArray(_ -> Value.ValueParty(owner))) =>
      Some(owner)

    case _ =>
      None
  }

  val getMaintainer: Value => Option[Party] = {
    case Value.ValueRecord(_, ImmArray(_, _ -> Value.ValueParty(maintainer))) =>
      Some(maintainer)

    case _ =>
      None
  }

  def runUpdateSExpr(sexpr: SExpr.SExpr): SExpr.SExpr = {
    SEMakeClo(Array(), 1, sexpr)
  }

  def evaluateSExprWithSetup(
      setupExpr: Ast.Expr,
      setupArgs: Array[SValue],
  )(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosures: Iterable[(Value.ContractId, ContractInfo)] = Iterable.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Either[SError.SError, SValue], Speedy.UpdateMachine) = {
    import SpeedyTestLib.loggingContext

    // A token function closure is added as part of compiling the Expr
    val contextSExpr = pkgs.compiler.unsafeCompile(setupExpr)
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
        updateSE =
          if (setupArgs.isEmpty) contextSExpr
          else SExpr.SEApp(contextSExpr, setupArgs),
        committers = committers,
      )
    disclosures.foreach { case (cid, contract) => machine.addDisclosedContracts(cid, contract) }
    val setupResult = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    setupResult match {
      case Right(_) => ()
      case Left(SError.SErrorCrash(loc, err)) =>
        throw new Exception(s"$loc: $err")
      case Left(SError.SErrorDamlException(error)) =>
        throw new Exception(s"$error")
    }
    machine.setExpressionToEvaluate(SExpr.SEApp(runUpdateSExpr(sexpr), Array(SToken)))

    val result = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    (result, machine)
  }

  def evaluateSExpr(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosures: Iterable[(Value.ContractId, ContractInfo)] = Iterable.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Either[SError.SError, SValue], Speedy.UpdateMachine) = {
    import SpeedyTestLib.loggingContext

    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureLib"),
        updateSE = runUpdateSExpr(sexpr),
        committers = committers,
      )
    disclosures.foreach { case (cid, contract) => machine.addDisclosedContracts(cid, contract) }
    val result = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    (result, machine)
  }

  def haveInactiveContractIds(contractIds: ContractId*): Matcher[Speedy.UpdateMachine] = Matcher {
    machine =>
      val expectedResult = contractIds.toSet
      val actualResult = machine.ptx.contractState.activeState.consumedBy.keySet
      val debugMessage = {
        val diff1 = expectedResult -- actualResult
        val diff2 = actualResult -- expectedResult
        if (diff1.nonEmpty)
          s"expected but missing contract IDs: $diff1"
        else
          s"unexpected but found contract IDs: $diff2"
      }

      MatchResult(
        expectedResult == actualResult,
        s"Failed with unexpected inactive contracts: $expectedResult != $actualResult $debugMessage",
        s"Failed with unexpected inactive contracts: $expectedResult == $actualResult",
      )
  }

  def haveDisclosedContracts(
      disclosures: (Value.ContractId, ContractInfo)*
  ): Matcher[Speedy.UpdateMachine] =
    Matcher { machine =>
      val expectedResult = disclosures.iterator.map { case (coid, contract) =>
        coid -> contract.arg
      }.toMap
      val actualResult = machine.disclosedContracts.transform((_, c) => c.arg)

      val debugMessage = {
        val diff1 = expectedResult.keySet -- actualResult.keySet
        val diff2 = actualResult.keySet -- expectedResult.keySet
        if (diff1.nonEmpty)
          s"expected but missing contract IDs: $diff1"
        else if (diff2.nonEmpty)
          s"unexpected but found contract IDs: $diff2"
        else
          ""
      }

      MatchResult(
        expectedResult == actualResult,
        s"Failed with unexpected disclosed contracts: $expectedResult != $actualResult $debugMessage",
        s"Failed with unexpected disclosed contracts: $expectedResult == $actualResult",
      )

    }
}
