// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref, Struct, Time}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SExpr.SEMakeClo
import com.daml.lf.speedy.SValue.{SContractId, SToken}
import com.daml.lf.speedy.Speedy.{CachedContract, CachedKey}
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import org.scalatest.matchers.{MatchResult, Matcher}
import com.daml.lf.testing.parser.Implicits._

/** Shared test data and functions for testing explicit disclosure.
  */
object ExplicitDisclosureLib {

  val testKeyName: String = "test-key"
  val pkg: PureCompiledPackages = SpeedyTestLib.typeAndCompile(
    p"""
       module TestMod {

         record @serializable Key = { label: Text, maintainers: List Party };

         record @serializable House = { owner: Party, key_maintainer: Party };
         template(this: House) = {
           precondition True;
           signatories (TestMod:listOf @Party (TestMod:House {owner} this));
           observers (Nil @Party);
           agreement "Agreement for TestMod:House";

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
           agreement "Agreement for TestMod:Cave";

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
  )
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
  val houseTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val houseTemplateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val caveTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Cave")
  val caveTemplateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Cave")
  val keyType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Key")
  val contractKey: GlobalKey = buildContractKey(maintainerParty)
  val contractSKey: SValue = buildContractSKey(maintainerParty)
  val ledgerContractKey: GlobalKey = buildContractKey(ledgerParty)
  val ledgerHouseContract: Value.VersionedContractInstance =
    buildContract(ledgerParty, maintainerParty)
  val ledgerCaveContract: Value.VersionedContractInstance =
    buildContract(ledgerParty, maintainerParty, caveTemplateId)
  val disclosedCaveContractNoHash: DisclosedContract =
    buildDisclosedCaveContract(contractId, disclosureParty)
  val disclosedHouseContract: DisclosedContract =
    buildDisclosedHouseContract(disclosureContractId, disclosureParty, maintainerParty)
  val disclosedCaveContract: DisclosedContract =
    buildDisclosedCaveContract(contractId, disclosureParty)

  def buildDisclosedHouseContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier = houseTemplateId,
      withHash: Boolean = true,
      label: String = testKeyName,
  ): DisclosedContract = {
    val key = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueText(label),
        None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainer)))),
      ),
    )
    val keyHash: Option[Hash] =
      if (withHash) Some(crypto.Hash.assertHashContractKey(houseTemplateType, key)) else None

    DisclosedContract(
      templateId,
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner"), Ref.Name.assertFromString("key_maintainer")),
        ArrayList(SValue.SParty(owner), SValue.SParty(maintainer)),
      ),
      ContractMetadata(Time.Timestamp.now(), keyHash, Bytes.Empty),
    )
  }

  def buildDisclosedCaveContract(
      contractId: ContractId,
      owner: Party,
      templateId: Ref.Identifier = caveTemplateId,
  ): DisclosedContract = {
    DisclosedContract(
      templateId,
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner")),
        ArrayList(SValue.SParty(owner)),
      ),
      ContractMetadata(Time.Timestamp.now(), None, Bytes.Empty),
    )
  }

  def buildContractKey(maintainer: Party, label: String = testKeyName): GlobalKey =
    GlobalKey.assertBuild(
      houseTemplateType,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueText(label),
          None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainer)))),
        ),
      ),
    )

  def buildContractSKey(maintainer: Party): SValue =
    SValue.SStruct(
      fieldNames =
        Struct.assertFromNameSeq(Seq("globalKey", "maintainers").map(Ref.Name.assertFromString)),
      values = ArrayList(
        SValue.SRecord(
          keyType,
          ImmArray("label", "maintainers").map(Ref.Name.assertFromString),
          ArrayList(
            SValue.SText(testKeyName),
            SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainer)))),
          ),
        ),
        SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainer)))),
      ),
    )

  def buildContract(
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier = houseTemplateId,
  ): Versioned[ContractInstance] = Versioned(
    TransactionVersion.minExplicitDisclosure,
    Value.ContractInstance(
      templateId,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(owner),
          None -> Value.ValueParty(maintainer),
        ),
      ),
    ),
  )

  def buildHouseCachedContract(
      signatory: Party,
      maintainer: Party,
      templateId: Ref.Identifier = houseTemplateId,
      withKey: Boolean = true,
      label: String = testKeyName,
  ): CachedContract = {
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
            GlobalKeyWithMaintainers
              .assertBuild(templateId, contract.toUnnormalizedValue, Set(maintainer)),
            contract,
          )
        )
      else None

    CachedContract(
      TransactionVersion.minExplicitDisclosure,
      templateId,
      contract,
      agreementText = "",
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
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Either[SError.SError, SValue], Speedy.UpdateMachine) = {
    import SpeedyTestLib.loggingContext

    // A token function closure is added as part of compiling the Expr
    val contextSExpr = pkg.compiler.unsafeCompile(setupExpr)
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkg,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
        updateSE =
          if (setupArgs.isEmpty) contextSExpr
          else SExpr.SEApp(contextSExpr, setupArgs),
        committers = committers,
        disclosedContracts = disclosedContracts,
      )
    val setupResult = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    assert(setupResult.isRight)

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
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Either[SError.SError, SValue], Speedy.UpdateMachine) = {
    import SpeedyTestLib.loggingContext

    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkg,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureLib"),
        updateSE = runUpdateSExpr(sexpr),
        committers = committers,
        disclosedContracts = disclosedContracts,
      )
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
      disclosedContracts: DisclosedContract*
  ): Matcher[Speedy.UpdateMachine] =
    Matcher { machine =>
      val expectedResult = disclosedContracts.iterator
        .map(c =>
          c.contractId.value -> c.argument.toNormalizedValue(machine.tmplId2TxVersion(c.templateId))
        )
        .toMap
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
