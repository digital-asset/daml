// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.Party
import com.daml.lf.interpretation.Error.{ContractKeyNotFound, ContractNotActive}
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SExpr.SEValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.speedy.SBuiltinFun.{SBFetchTemplate, SBUFetchKey, SBULookupKey}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.speedy.Speedy.ContractInfo
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.testing.parser.Implicits._

class ExplicitDisclosureTestV2 extends ExplicitDisclosureTest(LanguageMajorVersion.V2)

private[lf] class ExplicitDisclosureTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Inside
    with Matchers {

  val explicitDisclosureLib = new ExplicitDisclosureLib(majorLanguageVersion)

  import explicitDisclosureLib._

  "disclosed contract behaviour" - {
    "fetching contracts" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        getOwner(ledgerCaveContract.unversioned.arg) shouldBe Some(ledgerParty)
        inside(disclosedCaveContract) { case (`contractId`, contract) =>
          getOwner(contract.arg) shouldBe Some(disclosureParty)
        }
      }

      "disclosure table queried when contract ID is disclosed" - {
        "contract ID in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchTemplate(caveTemplateId)(SEValue(SContractId(contractId)), SEValue.None),
            disclosedCaveContract,
            disclosures = List(disclosedCaveContract),
          )(result =>
            inside(result) { case Right(contract @ SValue.SRecord(`caveTemplateId`, _, _)) =>
              getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
            }
          )
        }

        "contract ID in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchTemplate(caveTemplateId)(SEValue(SContractId(contractId)), SEValue.None),
            disclosedCaveContract,
            getContract = Map(contractId -> ledgerCaveContract),
            disclosures = List(disclosedCaveContract),
          )(result =>
            inside(result) { case Right(contract @ SValue.SRecord(`caveTemplateId`, _, _)) =>
              getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
            }
          )
        }
      }

      "contract IDs that are inactive" - {
        "ledger query fails when contract ID is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBFetchTemplate(caveTemplateId)(SEValue(SContractId(contractId)), SEValue.None),
            contractId,
            "TestMod:destroyCave",
            committers = Set(ledgerParty),
            getContract = Map(contractId -> ledgerCaveContract),
          ) { result =>
            inside(result) {
              case Left(
                    SError.SErrorDamlException(
                      ContractNotActive(`contractId`, `caveTemplateId`, _)
                    )
                  ) =>
                succeed
            }
          }
        }

        "disclosure table query fails when contract ID is disclosed" - {
          "contract ID in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBFetchTemplate(caveTemplateId)(SEValue(SContractId(contractId)), SEValue.None),
              disclosedCaveContract,
              contractId,
              "TestMod:destroyCave",
              committers = Set(disclosureParty),
              disclosures = List(disclosedCaveContract),
            )(result =>
              inside(result) {
                case Left(
                      SError.SErrorDamlException(
                        ContractNotActive(`contractId`, `caveTemplateId`, _)
                      )
                    ) =>
                  succeed
              }
            )
          }

          "contract ID in ledger and disclosure table" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBFetchTemplate(caveTemplateId)(SEValue(SContractId(contractId)), SEValue.None),
              disclosedCaveContract,
              contractId,
              "TestMod:destroyCave",
              committers = Set(disclosureParty, ledgerParty),
              getContract = Map(contractId -> ledgerCaveContract),
              disclosures = List(disclosedCaveContract),
            )(result =>
              inside(result) {
                case Left(
                      SError.SErrorDamlException(
                        ContractNotActive(`contractId`, `caveTemplateId`, _)
                      )
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
        inside(disclosedHouseContract) { case (`disclosureContractId`, contract) =>
          getOwner(contract.arg) shouldBe Some(disclosureParty)
          getMaintainer(contract.arg) shouldBe Some(maintainerParty)
        }
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
          committers = Set(ledgerParty),
          getKey = Map(
            GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
          ),
          getContract = Map(ledgerContractId -> ledgerHouseContract),
        )(_ shouldBe Right(SValue.SContractId(ledgerContractId)))
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty),
            disclosures = List(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
            disclosures = List(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
            ledgerContractId,
            "TestMod:destroyHouse",
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
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
              SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
              disclosedHouseContract,
              disclosureContractId,
              "TestMod:destroyHouse",
              committers = Set(disclosureParty, maintainerParty),
              disclosures = List(disclosedHouseContract),
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
                SBUFetchKey(houseTemplateId)(SEValue(contractSStructKey)),
                disclosedHouseContract,
                contractIdToBurn,
                "TestMod:destroyHouse",
                committers = Set(disclosureParty, ledgerParty, maintainerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(
                    contractKey,
                    Set(maintainerParty),
                  ) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerHouseContract),
                disclosures = List(disclosedHouseContract),
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
        inside(disclosedHouseContract) { case (`disclosureContractId`, contract) =>
          getOwner(contract.arg) shouldBe Some(disclosureParty)
          getMaintainer(contract.arg) shouldBe Some(maintainerParty)
        }
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
          committers = Set(ledgerParty),
          getKey = Map(
            GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
          ),
          getContract = Map(ledgerContractId -> ledgerHouseContract),
        )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(ledgerContractId)))))
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty),
            disclosures = List(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
            disclosures = List(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
            ledgerContractId,
            "TestMod:destroyHouse",
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
          )(result =>
            inside(result) { case Right(SValue.SOptional(None)) =>
              succeed
            }
          )
        }

        "disclosure table query fails when contract key is disclosed" - {
          "contract key in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
              disclosedHouseContract,
              disclosureContractId,
              "TestMod:destroyHouse",
              committers = Set(disclosureParty, maintainerParty),
              disclosures = List(disclosedHouseContract),
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
                SBULookupKey(houseTemplateId)(SEValue(contractSStructKey)),
                disclosedHouseContract,
                contractIdToBurn,
                "TestMod:destroyHouse",
                committers = Set(disclosureParty, ledgerParty, maintainerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(
                    contractKey,
                    Set(maintainerParty),
                  ) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerHouseContract),
                disclosures = List(disclosedHouseContract),
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

  def ledgerQueriedWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosures: Iterable[(Value.ContractId, ContractInfo)] = Iterable.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, machine) =
      evaluateSExpr(
        sexpr,
        committers = committers,
        disclosures = disclosures,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    machine should haveDisclosedContracts()
    machine should haveInactiveContractIds()
  }

  def disclosureTableQueriedWhenContractDisclosed(
      sexpr: SExpr.SExpr,
      disclosedContract: (Value.ContractId, ContractInfo),
      committers: Set[Party] = Set.empty,
      disclosures: Iterable[(Value.ContractId, ContractInfo)],
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, machine) =
      evaluateSExpr(
        sexpr,
        committers = committers,
        disclosures = disclosures,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    machine should haveDisclosedContracts(disclosedContract)
    machine should haveInactiveContractIds()
  }

  def ledgerQueryFailsWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
      contractId: ContractId,
      action: String,
      committers: Set[Party],
      disclosures: Iterable[(Value.ContractId, ContractInfo)] = Iterable.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, machine) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                      $action contractId
              """,
        Array(SContractId(contractId)),
      )(
        sexpr,
        committers = committers,
        disclosures = disclosures,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    machine should haveDisclosedContracts()
    machine should haveInactiveContractIds(contractId)
  }

  def disclosureTableQueryFailsWhenContractDisclosed(
      sexpr: SExpr.SExpr,
      disclosedContract: (Value.ContractId, ContractInfo),
      contractToDestroy: ContractId,
      action: String,
      committers: Set[Party],
      disclosures: Iterable[(Value.ContractId, ContractInfo)],
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, machine) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                      $action contractId
                """,
        Array(SContractId(contractToDestroy)),
      )(
        sexpr,
        committers = committers,
        disclosures = disclosures,
        getContract = getContract,
        getKey = getKey,
      )

    assertResult(result)
    machine should haveDisclosedContracts(disclosedContract)
    machine should haveInactiveContractIds(contractToDestroy)
  }
}
