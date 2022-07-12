// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Struct, Time}
import com.daml.lf.interpretation.Error.ContractKeyNotFound
import com.daml.lf.speedy.SExpr.{SEMakeClo, SEValue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.speedy.SBuiltin.{SBFetchAny, SBUFetchKey, SBULookupKey}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.prop.TableDrivenPropertyChecks

class ExplicitDisclosureTest extends AnyFreeSpec with Inside with Matchers with TableDrivenPropertyChecks {

  import ExplicitDisclosureTest._

  // TODO: add in testing for inactive contracts

  "disclosed contract behaviour" - {
    "with contracts stored in a single location" - {
      "off ledger" - {
        "fetching contracts" - {
          "fail to evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                getContract = Map(contractId -> ledgerContract),
                onLedger = false,
              )

            inside(result) {
              case Right(
                    SValue.SPAP(SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame), _, 1)
                  ) =>
                function shouldBe SExpr.SEBuiltin(SBFetchAny)
                args shouldBe Array(
                  SEValue(SContractId(contractId)),
                  SEValue(SValue.SOptional(None)),
                )
                frame shouldBe empty
            }
          }

          "fail to evaluate disclosed contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, _) =>
              val result =
                evaluateSExpr(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  disclosedContracts = ImmArray(disclosedContract),
                  onLedger = false,
                )

              inside(result) {
                case Right(
                SValue.SPAP(SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame), _, 1)
                ) =>
                  function shouldBe SExpr.SEBuiltin(SBFetchAny)
                  args shouldBe Array(
                    SEValue(SContractId(contractId)),
                    SEValue(SValue.SOptional(None)),
                  )
                  frame shouldBe empty
              }
            }
          }
        }

        "fetching contract keys" - {
          "fail to evaluate disclosed contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, _) =>
              val result =
                evaluateSExpr(
                  SBUFetchKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                  committers = Set(disclosureParty),
                  disclosedContracts = ImmArray(disclosedContract),
                  onLedger = false,
                )

              inside(result) {
                case Right(
                SValue.SPAP(SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame), _, 1)
                ) =>
                  function shouldBe SExpr.SEBuiltin(SBUFetchKey(templateId))
                  args shouldBe Array(SEValue(buildContractSKey(disclosureParty)))
                  frame shouldBe empty
              }
            }
          }
        }

        "looking up contract keys" - {
          "fail to evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBULookupKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                committers = Set(ledgerParty),
                getKey = Map(GlobalKeyWithMaintainers(contractKey, Set(ledgerParty)) -> contractId),
                getContract = Map(contractId -> ledgerContract),
                onLedger = false,
              )

            inside(result) {
              case Right(
                    SValue.SPAP(SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame), _, 1)
                  ) =>
                function shouldBe SExpr.SEBuiltin(SBULookupKey(templateId))
                args shouldBe Array(SEValue(buildContractSKey(ledgerParty)))
                frame shouldBe empty
            }
          }
        }
      }

      "on ledger" - {
        "fetching contracts" - {
          "evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                getContract = Map(contractId -> ledgerContract),
              )

            inside(result) {
              case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                values shouldBe ArrayList(SValue.SParty(ledgerParty))
            }
          }

          "evaluate disclosed contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, _) =>
              val result =
                evaluateSExpr(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                  fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                  values shouldBe ArrayList(SValue.SParty(disclosureParty))
              }
            }
          }
        }

        "fetching contract keys" - {
          "evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBUFetchKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                committers = Set(ledgerParty),
                getKey = Map(GlobalKeyWithMaintainers(contractKey, Set(ledgerParty)) -> contractId),
                getContract = Map(contractId -> ledgerContract),
              )

            result shouldBe Right(SValue.SContractId(contractId))
          }

          "evaluate disclosed contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, label) =>
              val result =
                evaluateSExpr(
                  SBUFetchKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                  committers = Set(disclosureParty),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                  label shouldBe "case-1"

                case Right(SValue.SContractId(`contractId`)) =>
                  label shouldBe "case-2"
              }
            }
          }
        }

        "looking up contract keys" - {
          "evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBULookupKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                committers = Set(ledgerParty),
                getKey = Map(GlobalKeyWithMaintainers(contractKey, Set(ledgerParty)) -> contractId),
                getContract = Map(contractId -> ledgerContract),
              )

            result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(contractId))))
          }

          "evaluate disclosed contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, label) =>
              val result =
                evaluateSExpr(
                  SBULookupKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                  committers = Set(disclosureParty),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Right(SValue.SOptional(None)) =>
                  label shouldBe "case-1"

                case Right(SValue.SOptional(Some(SValue.SContractId(`contractId`)))) =>
                  label shouldBe "case-2"
              }
            }
          }
        }
      }
    }

    "with contracts stored in multiple locations" - {
      "off ledger" - {
        "fetching contracts" - {
          "fail to evaluate known contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, _) =>
              val result =
                evaluateSExpr(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  disclosedContracts = ImmArray(disclosedContract),
                  onLedger = false,
                )

              inside(result) {
                case Right(
                SValue.SPAP(SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame), _, 1)
                ) =>
                  function shouldBe SExpr.SEBuiltin(SBFetchAny)
                  args shouldBe Array(
                    SEValue(SContractId(contractId)),
                    SEValue(SValue.SOptional(None)),
                  )
                  frame shouldBe empty
              }
            }
          }
        }

        "fetching contract keys" - {
          // TODO:
        }

        "looking up contract keys" - {
          // TODO:
        }
      }

      "on ledger" - {
        "fetching contracts" - {
          "evaluate known contract IDs" in {
            forAll(disclosedContracts) { case (disclosedContract, _) =>
              val result =
                evaluateSExpr(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  disclosedContracts = ImmArray(disclosedContract),
                  getContract = Map(contractId -> ledgerContract),
                )

              // Ledger contract is not cached, so we always return the disclosed contract
              inside(result) {
                case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                  fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                  values shouldBe ArrayList(SValue.SParty(disclosureParty))
              }
            }
          }
        }

        "fetching contract keys" - {
          // TODO:
        }

        "looking up contract keys" - {
          "evaluate known contract IDs" in {
            val result =
              evaluateSExpr(
                SBULookupKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                committers = Set(ledgerParty),
                getKey = Map(GlobalKeyWithMaintainers(contractKey, Set(ledgerParty)) -> contractId),
                getContract = Map(contractId -> ledgerContract),
              )

            result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(contractId))))
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

         record @serializable House = { owner: Party };

         template(this: House) = {
           precondition True;
           signatories (TestMod:listOf (TestMod:House {owner} this));
           observers (Nil @Party);
           agreement "Agreement for TestMod:House";

           key @TestMod:Key
              (TestMod:Key { label = "test-key", maintainers = (TestMod:listOf (TestMod:House {owner} this)) })
              (\(key: TestMod:Key) -> (TestMod:Key {maintainers} key));
         };

         val listOf: Party -> List Party =
           \(person: Party) -> Cons @Party [person] (Nil @Party);

       }
       """
  )
  val ledgerParty: IdString.Party = Ref.Party.assertFromString("cachedParty")
  val disclosureParty: IdString.Party = Ref.Party.assertFromString("disclosureParty")
  val contractId: ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val templateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val testKeyHash: Hash = crypto.Hash.assertHashContractKey(templateType, Value.ValueText(testKeyName))
  val contractKey: GlobalKey = GlobalKey.assertBuild(templateType, Value.ValueText(testKeyName))
  val ledgerContract: Value.VersionedContractInstance = buildContract(ledgerParty)
  val disclosedContractNoHash: DisclosedContract = DisclosedContract(
    templateId,
    SContractId(contractId),
    SValue.SRecord(
      `templateId`,
      ImmArray(Ref.Name.assertFromString("owner")),
      ArrayList(SValue.SParty(disclosureParty)),
    ),
    ContractMetadata(Time.Timestamp.now(), None, ImmArray.Empty),
  )
  val disclosedContractWithHash: DisclosedContract = DisclosedContract(
    templateId,
    SContractId(contractId),
    SValue.SRecord(
      `templateId`,
      ImmArray(Ref.Name.assertFromString("owner")),
      ArrayList(SValue.SParty(disclosureParty)),
    ),
    ContractMetadata(Time.Timestamp.now(), Some(testKeyHash), ImmArray.Empty),
  )
  val disclosedContracts =
    TableDrivenPropertyChecks.Table(
      ("disclosedContract", "specialCaseLabel"),
      (disclosedContractNoHash, "case-1"),
      (disclosedContractWithHash, "case-2"),
    )

  def buildContractSKey(maintainer: Party): SValue =
    SValue.SStruct(
      fieldNames =
        Struct.assertFromNameSeq(Seq("globalKey", "maintainers").map(Ref.Name.assertFromString)),
      values = ArrayList(
        SValue.SText(testKeyName),
        SValue.SList(FrontStack.from(ImmArray(SValue.SParty(maintainer)))),
      ),
    )

  def buildContract(owner: Party): Versioned[ContractInstance] = Versioned(
    TransactionVersion.minExplicitDisclosure,
    Value.ContractInstance(
      templateId,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(owner)
        ),
      ),
      "test",
    ),
  )

  def runUpdateSExpr(sexpr: SExpr.SExpr): SExpr.SExpr = {
    SEMakeClo(Array(), 1, sexpr)
  }

  def evaluateSExpr(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      onLedger: Boolean = true,
  ): Either[SError.SError, SValue] = {
    import SpeedyTestLib.loggingContext

    if (onLedger) {
      SpeedyTestLib.run(
        machine = Speedy.Machine.fromUpdateSExpr(
          pkg,
          transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
          updateSE = runUpdateSExpr(sexpr),
          committers = committers,
          disclosedContracts = disclosedContracts,
        ),
        getContract = getContract,
        getKey = getKey,
      )
    } else {
      SpeedyTestLib.run(
        machine = Speedy.Machine.fromPureSExpr(
          pkg,
          expr = runUpdateSExpr(sexpr),
          disclosedContracts = disclosedContracts,
        ),
        getContract = getContract,
        getKey = getKey,
      )
    }
  }
}
