// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.interpretation.Error.{ContractKeyNotFound, ContractNotActive}
import com.daml.lf.speedy.SExpr.SEValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.speedy.SBuiltin.{SBFetchAny, SBUFetchKey, SBULookupKey}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.daml.lf.testing.parser.Implicits._

class ExplicitDisclosureTest extends ExplicitDisclosureTestMethods {

  import ExplicitDisclosureLib._

  "disclosed contract behaviour" - {
    "fetching contracts" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        getOwner(ledgerCaveContract.unversioned.arg) shouldBe Some(ledgerParty)
        disclosedCaveContract.contractId shouldBe SContractId(contractId)
        getOwner(disclosedCaveContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
      }

      "ledger queried when contract ID is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
          getContract = Map(contractId -> ledgerCaveContract),
        )(result =>
          inside(result) {
            case Right(SValue.SAny(_, contract @ SValue.SRecord(`caveTemplateId`, _, _))) =>
              getOwner(contract.toUnnormalizedValue) shouldBe Some(ledgerParty)
          }
        )
      }

      "disclosure table queried when contract ID is disclosed" - {
        "contract ID in disclosure table only" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            disclosedCaveContract,
            disclosedContracts = ImmArray(disclosedCaveContract),
          )(result =>
            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`caveTemplateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
            }
          )
        }

        "contract ID in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            disclosedCaveContract,
            getContract = Map(contractId -> ledgerCaveContract),
            disclosedContracts = ImmArray(disclosedCaveContract),
          )(result =>
            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`caveTemplateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
            }
          )
        }
      }

      "contract IDs that are inactive" - {
        "ledger query fails when contract ID is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            contractId,
            "TestMod:destroyCave",
            committers = Set(ledgerParty),
            getContract = Map(contractId -> ledgerCaveContract),
          )(result =>
            inside(result) {
              case Left(
                    SError.SErrorDamlException(ContractNotActive(`contractId`, `caveTemplateId`, _))
                  ) =>
                succeed
            }
          )
        }

        "disclosure table query fails when contract ID is disclosed" - {
          "contract ID in disclosure table only" in {
            disclosureTableQueryFailsWhenContractDisclosed(
              SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
              disclosedCaveContract,
              contractId,
              "TestMod:destroyCave",
              committers = Set(disclosureParty),
              disclosedContracts = ImmArray(disclosedCaveContract),
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
              SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
              disclosedCaveContract,
              contractId,
              "TestMod:destroyCave",
              committers = Set(disclosureParty, ledgerParty),
              getContract = Map(contractId -> ledgerCaveContract),
              disclosedContracts = ImmArray(disclosedCaveContract),
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
        disclosedHouseContract.contractId shouldBe SContractId(disclosureContractId)
        getOwner(disclosedHouseContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
        getMaintainer(disclosedHouseContract.argument.toUnnormalizedValue) shouldBe Some(
          maintainerParty
        )
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
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
            SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty),
            disclosedContracts = ImmArray(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
            disclosedContracts = ImmArray(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SContractId(disclosureContractId)))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
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
              SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
              disclosedHouseContract,
              disclosureContractId,
              "TestMod:destroyHouse",
              committers = Set(disclosureParty, maintainerParty),
              disclosedContracts = ImmArray(disclosedHouseContract),
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
                SBUFetchKey(houseTemplateId)(SEValue(contractSKey)),
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
                disclosedContracts = ImmArray(disclosedHouseContract),
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

      "wrongly typed contract disclosures are not found" in {
        wronglyTypedDisclosedContractsRejected(
          SBUFetchKey(houseTemplateType)(SEValue(contractSKey))
        ) { result =>
          inside(result) { case Left(SError.SErrorDamlException(ContractKeyNotFound(_))) =>
            succeed
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
        disclosedHouseContract.contractId shouldBe SContractId(disclosureContractId)
        getOwner(disclosedHouseContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
        getMaintainer(disclosedHouseContract.argument.toUnnormalizedValue) shouldBe Some(
          maintainerParty
        )
      }

      "ledger queried when contract key is not disclosed" in {
        ledgerQueriedWhenContractNotDisclosed(
          SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
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
            SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty),
            disclosedContracts = ImmArray(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }

        "contract key in ledger and disclosure table" in {
          disclosureTableQueriedWhenContractDisclosed(
            SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
            disclosedHouseContract,
            committers = Set(disclosureParty, ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerHouseContract),
            disclosedContracts = ImmArray(disclosedHouseContract),
          )(_ shouldBe Right(SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))))
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          ledgerQueryFailsWhenContractNotDisclosed(
            SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
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
              SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
              disclosedHouseContract,
              disclosureContractId,
              "TestMod:destroyHouse",
              committers = Set(disclosureParty, maintainerParty),
              disclosedContracts = ImmArray(disclosedHouseContract),
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
                SBULookupKey(houseTemplateId)(SEValue(contractSKey)),
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
                disclosedContracts = ImmArray(disclosedHouseContract),
              )(result =>
                inside(result) { case Right(SValue.SOptional(None)) =>
                  succeed
                }
              )
            }
          }
        }
      }

      "wrongly typed contract disclosures are not found" in {
        wronglyTypedDisclosedContractsRejected(
          SBULookupKey(houseTemplateType)(SEValue(contractSKey))
        )(_ shouldBe Right(SValue.SOptional(None)))
      }
    }
  }
}

private[lf] trait ExplicitDisclosureTestMethods extends AnyFreeSpec with Inside with Matchers {

  import ExplicitDisclosureLib._

  def ledgerQueriedWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
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
    ledger should haveInactiveContractIds()
  }

  def ledgerQueryFailsWhenContractNotDisclosed(
      sexpr: SExpr.SExpr,
      contractId: ContractId,
      action: String,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                          $action contractId
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
    ledger should haveInactiveContractIds(contractId)
  }

  def disclosureTableQueryFailsWhenContractDisclosed(
      sexpr: SExpr.SExpr,
      disclosedContract: DisclosedContract,
      contractToDestroy: ContractId,
      action: String,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val (result, ledger) =
      evaluateSExprWithSetup(
        e"""\(contractId: ContractId TestMod:House) ->
                          $action contractId
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
    ledger should haveInactiveContractIds(contractToDestroy)
  }

  def wronglyTypedDisclosedContractsRejected(
      sexpr: SExpr.SExpr
  )(assertResult: Either[SError.SError, SValue] => Assertion): Assertion = {
    val houseContractKey: GlobalKey = buildContractKey(maintainerParty)
    // Here the disclosed contract has a caveTemplateType, but its key has a houseTemplateType
    val malformedDisclosedContract: DisclosedContract =
      DisclosedContract(
        caveTemplateType,
        SContractId(disclosureContractId),
        SValue.SRecord(
          caveTemplateType,
          ImmArray(
            Ref.Name.assertFromString("owner"),
            Ref.Name.assertFromString("key_maintainer"),
          ),
          ArrayList(SValue.SParty(disclosureParty), SValue.SParty(maintainerParty)),
        ),
        ContractMetadata(Time.Timestamp.now(), Some(houseContractKey.hash), ImmArray.Empty),
      )
    val (result, ledger) =
      evaluateSExpr(
        sexpr,
        committers = Set(disclosureParty),
        disclosedContracts = ImmArray(malformedDisclosedContract),
      )

    assertResult(result)
    ledger should haveDisclosedContracts(malformedDisclosedContract)
    ledger should haveInactiveContractIds()
  }
}
