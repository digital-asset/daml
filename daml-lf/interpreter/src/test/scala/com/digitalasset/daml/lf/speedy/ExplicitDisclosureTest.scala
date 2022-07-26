// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Struct, Time}
import com.daml.lf.interpretation.Error.{
  ContractKeyNotFound,
  ContractNotActive,
  DisclosurePreprocessing,
}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SExpr.{SEMakeClo, SEValue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.speedy.SBuiltin.{SBFetchAny, SBUFetchKey, SBULookupKey}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.matchers.{MatchResult, Matcher}

class ExplicitDisclosureTest extends ExplicitDisclosureTestMethods {

  import ExplicitDisclosureTest._

  "disclosed contract behaviour" - {
    "fetching contracts" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        getOwner(ledgerContract.unversioned.arg) shouldBe Some(ledgerParty)
        getMaintainer(ledgerContract.unversioned.arg) shouldBe Some(maintainerParty)
        disclosedContract1.contractId shouldBe SContractId(contractId)
        getOwner(disclosedContract1.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
        getMaintainer(disclosedContract1.argument.toUnnormalizedValue) shouldBe Some(
          maintainerParty
        )
      }

      "disclosure preprocessing" - {
        "template does not exist" in {
          templateDoesNotExist(SBFetchAny(SEValue(SContractId(contractId)), SEValue.None))
        }

        "disclosed contract key has no hash" in {
          disclosedContractKeyHasNoHash(SBFetchAny(SEValue(SContractId(contractId)), SEValue.None))
        }
      }

      "ledger queried when contract ID is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
          contractId,
          getContract = Map(contractId -> ledgerContract),
        )(result =>
          inside(result) {
            case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
              getOwner(contract.toUnnormalizedValue) shouldBe Some(ledgerParty)
              getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
          }
        )
      }

      "disclosure table queried when contract ID is disclosed" - {
        "contract ID in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            disclosedContract1,
            disclosedContracts = ImmArray(disclosedContract1),
          )(result =>
            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
                getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
            }
          )
        }

        "contract ID in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            disclosedContract1,
            // contract owned by ledgerParty
            getContract = Map(contractId -> ledgerContract),
            // contract owned by disclosureParty
            disclosedContracts = ImmArray(disclosedContract1),
          )(result =>
            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
                getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
            }
          )
        }
      }

      "contract IDs that are inactive" - {
        "ledger query fails when contract ID is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            contractId,
            committers = Set(ledgerParty, maintainerParty),
            getContract = Map(contractId -> ledgerContract),
          )(result =>
            inside(result) {
              case Left(
                    SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                  ) =>
                succeed
            }
          )
        }

        "disclosure table query fails when contract ID is disclosed" - {
          "contract ID in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
              disclosedContract1,
              contractId,
              committers = Set(disclosureParty, maintainerParty),
              disclosedContracts = ImmArray(disclosedContract1),
            )(result =>
              inside(result) {
                case Left(
                      SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                    ) =>
                  succeed
              }
            )
          }

          "contract ID in ledger and disclosure table" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
              disclosedContract1,
              contractId,
              committers = Set(disclosureParty, ledgerParty, maintainerParty),
              getContract = Map(contractId -> ledgerContract),
              disclosedContracts = ImmArray(disclosedContract1),
            )(result =>
              inside(result) {
                case Left(
                      SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                    ) =>
                  succeed
              }
            )
          }
        }
      }
    }

    "fetching contract keys" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        ledgerContractId should not be disclosureContractId
        disclosedContract2.contractId shouldBe SContractId(disclosureContractId)
        getOwner(disclosedContract2.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
        getMaintainer(disclosedContract2.argument.toUnnormalizedValue) shouldBe Some(
          maintainerParty
        )
      }

      "disclosure preprocessing" - {
        "template does not exist" in {
          templateDoesNotExist(SBUFetchKey(templateId)(SEValue(contractSKey)))
        }

        "disclosed contract key has no hash" in {
          disclosedContractKeyHasNoHash(SBUFetchKey(templateId)(SEValue(contractSKey)))
        }
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBUFetchKey(templateId)(SEValue(contractSKey)),
          ledgerContractId,
          committers = Set(ledgerParty),
          getKey = Map(
            GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
          ),
          getContract = Map(ledgerContractId -> ledgerContract),
        )(_ shouldBe Right(SValue.SContractId(ledgerContractId)))
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBUFetchKey(templateId)(SEValue(contractSKey)),
            disclosedContract2,
            committers = Set(disclosureParty),
            disclosedContracts = ImmArray(disclosedContract2),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBUFetchKey(templateId)(SEValue(contractSKey)),
            disclosedContract2,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
            disclosedContracts = ImmArray(disclosedContract2),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBUFetchKey(templateId)(SEValue(contractSKey)),
            ledgerContractId,
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
          )(result =>
            inside(result) {
              case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                succeed
            }
          )
        }

        "disclosure table query fails when contract key is disclosed" - {
          "contract key in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBUFetchKey(templateId)(SEValue(contractSKey)),
              disclosedContract2,
              disclosureContractId,
              committers = Set(disclosureParty, maintainerParty),
              disclosedContracts = ImmArray(disclosedContract2),
            )(result =>
              inside(result) {
                case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                  succeed
              }
            )
          }

          "contract key in ledger and disclosure table" in {
            for (contractIdToBurn <- Set(ledgerContractId, disclosureContractId)) {
              // Exercising a single contract ID is sufficient to make the key inactive
              disclosureTableQueryFailsWhenContractDisclosed(
                SBUFetchKey(templateId)(SEValue(contractSKey)),
                disclosedContract2,
                contractIdToBurn,
                committers = Set(disclosureParty, ledgerParty, maintainerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(
                    contractKey,
                    Set(maintainerParty),
                  ) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerContract),
                disclosedContracts = ImmArray(disclosedContract2),
              )(result =>
                inside(result) {
                  case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                    succeed
                }
              )
            }
          }
        }
      }
    }

    "looking up contract keys" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        ledgerContractId should not be disclosureContractId
        disclosedContract2.contractId shouldBe SContractId(disclosureContractId)
        getOwner(disclosedContract2.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
        getMaintainer(disclosedContract2.argument.toUnnormalizedValue) shouldBe Some(
          maintainerParty
        )
      }

      "disclosure preprocessing" - {
        "template does not exist" in {
          templateDoesNotExist(SBULookupKey(templateId)(SEValue(contractSKey)))
        }

        "disclosed contract key has no hash" in {
          disclosedContractKeyHasNoHash(SBULookupKey(templateId)(SEValue(contractSKey)))
        }
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBULookupKey(templateId)(SEValue(contractSKey)),
          ledgerContractId,
          committers = Set(ledgerParty),
          getKey = Map(
            GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
          ),
          getContract = Map(ledgerContractId -> ledgerContract),
        )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(ledgerContractId)))))
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBULookupKey(templateId)(SEValue(contractSKey)),
            disclosedContract2,
            committers = Set(disclosureParty),
            disclosedContracts = ImmArray(disclosedContract2),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBULookupKey(templateId)(SEValue(contractSKey)),
            disclosedContract2,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
            disclosedContracts = ImmArray(disclosedContract2),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBULookupKey(templateId)(SEValue(contractSKey)),
            ledgerContractId,
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
          )(result =>
            inside(result) { case Right(SValue.SOptional(None)) =>
              succeed
            }
          )
        }

        "disclosure table query fails when contract key is disclosed" - {
          "contract key in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBULookupKey(templateId)(SEValue(contractSKey)),
              disclosedContract2,
              disclosureContractId,
              committers = Set(disclosureParty, maintainerParty),
              disclosedContracts = ImmArray(disclosedContract2),
            )(result =>
              inside(result) { case Right(SValue.SOptional(None)) =>
                succeed
              }
            )
          }

          "contract key in ledger and disclosure table" in {
            for (contractIdToBurn <- Set(ledgerContractId, disclosureContractId)) {
              // Exercising a single contract ID is sufficient to make the key inactive
              disclosureTableQueryFailsWhenContractDisclosed(
                SBULookupKey(templateId)(SEValue(contractSKey)),
                disclosedContract2,
                contractIdToBurn,
                committers = Set(disclosureParty, ledgerParty, maintainerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(
                    contractKey,
                    Set(maintainerParty),
                  ) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerContract),
                disclosedContracts = ImmArray(disclosedContract2),
              )(result =>
                inside(result) { case Right(SValue.SOptional(None)) =>
                  succeed
                }
              )
            }
          }
        }
      }
    }
  }
}

object ExplicitDisclosureTest {

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
  val invalidTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Invalid")
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val templateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val keyType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Key")
  val contractKey: GlobalKey = buildContractKey(maintainerParty)
  val contractSKey: SValue = buildContractSKey(maintainerParty)
  val ledgerContractKey: GlobalKey = buildContractKey(ledgerParty)
  val ledgerContract: Value.VersionedContractInstance = buildContract(ledgerParty, maintainerParty)
  val disclosedContractNoHash: DisclosedContract =
    buildDisclosedContract(contractId, disclosureParty, maintainerParty, withHash = false)
  val disclosedContractInvalidTemplate: DisclosedContract = buildDisclosedContract(
    contractId,
    disclosureParty,
    maintainerParty,
    templateId = invalidTemplateId,
    withHash = false,
  )
  val disclosedContract1: DisclosedContract =
    buildDisclosedContract(contractId, disclosureParty, maintainerParty)
  val disclosedContract2: DisclosedContract =
    buildDisclosedContract(disclosureContractId, disclosureParty, maintainerParty)

  def buildDisclosedContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier = templateId,
      withHash: Boolean = true,
  ): DisclosedContract = {
    val key = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueText(testKeyName),
        None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainer)))),
      ),
    )
    val keyHash: Option[Hash] =
      if (withHash) Some(crypto.Hash.assertHashContractKey(templateType, key)) else None

    DisclosedContract(
      templateId,
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner"), Ref.Name.assertFromString("key_maintainer")),
        ArrayList(SValue.SParty(owner), SValue.SParty(maintainer)),
      ),
      ContractMetadata(Time.Timestamp.now(), keyHash, ImmArray.Empty),
    )
  }

  def buildContractKey(maintainer: Party): GlobalKey =
    GlobalKey.assertBuild(
      templateType,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueText(testKeyName),
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

  def buildContract(owner: Party, maintainer: Party): Versioned[ContractInstance] = Versioned(
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
      "test",
    ),
  )

  val getOwner: Value => Option[Party] = {
    case Value.ValueRecord(_, ImmArray(_ -> Value.ValueParty(owner), _)) =>
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
  ): (Either[SError.SError, SValue], Speedy.OnLedger) = {
    import SpeedyTestLib.loggingContext

    // A token function closure is added as part of compiling the Expr
    val contextSExpr = pkg.compiler.unsafeCompile(setupExpr)
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkg,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
        updateSE =
          if (setupArgs.isEmpty) contextSExpr
          else SExpr.SEApp(contextSExpr, setupArgs.map(SEValue(_))),
        committers = committers,
        disclosedContracts = disclosedContracts,
      )
    val setupResult = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    assert(setupResult.isRight)

    machine.setExpressionToEvaluate(SExpr.SEApp(runUpdateSExpr(sexpr), Array(SEValue.Token)))

    val result = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    (result, machine.ledgerMode.asInstanceOf[Speedy.OnLedger])
  }

  def evaluateSExpr(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Either[SError.SError, SValue], Speedy.OnLedger) = {
    import SpeedyTestLib.loggingContext

    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkg,
        transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
        updateSE = runUpdateSExpr(sexpr),
        committers = committers,
        disclosedContracts = disclosedContracts,
      )
    val result = SpeedyTestLib.run(
      machine = machine,
      getContract = getContract,
      getKey = getKey,
    )

    (result, machine.ledgerMode.asInstanceOf[Speedy.OnLedger])
  }

  def haveInactiveContractIds(contractIds: ContractId*): Matcher[Speedy.OnLedger] = Matcher {
    ledger =>
      MatchResult(
        ledger.ptx.contractState.activeState.consumedBy.keySet == contractIds.toSet,
        s"Failed with unexpected inactive contracts: ${ledger.ptx.contractState.activeState.consumedBy.keySet} != $contractIds",
        s"Failed with unexpected inactive contracts: ${ledger.ptx.contractState.activeState.consumedBy.keySet} == $contractIds",
      )
  }

  def haveCachedContractIds(contractIds: ContractId*): Matcher[Speedy.OnLedger] = Matcher {
    ledger =>
      MatchResult(
        ledger.cachedContracts.keySet == contractIds.toSet,
        s"Failed with unexpected cached contracts: ${ledger.cachedContracts.keySet} != $contractIds",
        s"Failed with unexpected cached contracts: ${ledger.cachedContracts.keySet} == $contractIds",
      )
  }

  def haveDisclosedContracts(contractIds: DisclosedContract*): Matcher[Speedy.OnLedger] = Matcher {
    ledger =>
      MatchResult(
        ledger.ptx.disclosedContracts == ImmArray(contractIds: _*),
        s"Failed with unexpected disclosed contracts: ${ledger.ptx.disclosedContracts} != $contractIds",
        s"Failed with unexpected disclosed contracts: ${ledger.ptx.disclosedContracts} == $contractIds",
      )
  }
}

trait ExplicitDisclosureTestMethods extends AnyFreeSpec with Inside with Matchers {

  import ExplicitDisclosureTest._

  def templateDoesNotExist(sexpr: SExpr.SExpr): Assertion = {
    val error = intercept[SError.SErrorDamlException] {
      evaluateSExpr(
        sexpr,
        disclosedContracts = ImmArray(disclosedContractInvalidTemplate),
      )
    }

    error shouldBe SError.SErrorDamlException(
      DisclosurePreprocessing(DisclosurePreprocessing.NonExistentTemplate(invalidTemplateId))
    )
  }

  def disclosedContractKeyHasNoHash(sexpr: SExpr.SExpr): Assertion = {
    val error = intercept[SError.SErrorDamlException] {
      evaluateSExpr(
        sexpr,
        disclosedContracts = ImmArray(disclosedContractNoHash),
      )
    }

    error shouldBe SError.SErrorDamlException(
      DisclosurePreprocessing(
        DisclosurePreprocessing.NonExistentDisclosedContractKeyHash(contractId, templateId)
      )
    )
  }

  def ledgerQueriedWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
      contractId: ContractId,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExpr(
        sexpr,
        committers = committers,
        disclosedContracts = disclosedContracts,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    ledger should haveDisclosedContracts()
    ledger should haveCachedContractIds(contractId)
    ledger should haveInactiveContractIds()
  }

  def disclosureTableQueriedWhenContractDisclosed(
      sexpr: SExpr.SExpr,
      disclosedContract: DisclosedContract,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExpr(
        sexpr,
        committers = committers,
        disclosedContracts = disclosedContracts,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    ledger should haveDisclosedContracts(disclosedContract)
    ledger should haveCachedContractIds(disclosedContract.contractId.value)
    ledger should haveInactiveContractIds()
  }

  def ledgerQueryFailsWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
      contractId: ContractId,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:destroyHouse contractId
                  """,
        Array(SContractId(contractId)),
      )(
        sexpr,
        committers = committers,
        disclosedContracts = disclosedContracts,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    ledger should haveDisclosedContracts()
    ledger should haveCachedContractIds(contractId)
    ledger should haveInactiveContractIds(contractId)
  }

  def disclosureTableQueryFailsWhenContractDisclosed(
      sexpr: SExpr.SExpr,
      disclosedContract: DisclosedContract,
      contractToDestroy: ContractId,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:destroyHouse contractId
                    """,
        Array(SContractId(contractToDestroy)),
      )(
        sexpr,
        committers = committers,
        disclosedContracts = disclosedContracts,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    ledger should haveDisclosedContracts(disclosedContract)
    ledger should haveCachedContractIds(contractToDestroy)
    ledger should haveInactiveContractIds(contractToDestroy)
  }
}
