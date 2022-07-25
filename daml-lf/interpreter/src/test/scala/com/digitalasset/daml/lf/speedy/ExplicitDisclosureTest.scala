// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Struct, Time}
import com.daml.lf.interpretation.Error.{ContractKeyNotFound, ContractNotActive}
import com.daml.lf.language.Ast
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
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.prop.TableDrivenPropertyChecks

class ExplicitDisclosureTest
    extends AnyFreeSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import ExplicitDisclosureTest._

  "disclosed contract behaviour" - {
    "fetching contracts" - {
      "test data validation" in {
        ledgerParty should not be disclosureParty
        ledgerParty should not be maintainerParty
        disclosureParty should not be maintainerParty
        getOwner(ledgerContract.unversioned.arg) shouldBe Some(ledgerParty)
        getMaintainer(ledgerContract.unversioned.arg) shouldBe Some(maintainerParty)
        forAll(disclosedContractIds) { case (disclosedContract, _) =>
          disclosedContract.contractId shouldBe SContractId(contractId)
          getOwner(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
          getMaintainer(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(
            maintainerParty
          )
        }
      }

      "ledger queried when contract ID is not disclosed" in {
        val (result, ledger) =
          evaluateSExpr(
            SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
            getContract = Map(contractId -> ledgerContract),
          )

        inside(result) {
          case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
            getOwner(contract.toUnnormalizedValue) shouldBe Some(ledgerParty)
            getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
            ledger should haveDisclosedContracts()
            ledger should haveCachedContractIds(contractId)
            ledger should haveInactiveContractIds()
        }
      }

      "disclosure table queried when contract ID is disclosed" - {
        "contract ID in disclosure table only" in {
          forAll(disclosedContractIds) { case (disclosedContract, _) =>
            val (result, ledger) =
              evaluateSExpr(
                SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                disclosedContracts = ImmArray(disclosedContract),
              )

            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
                getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(contractId)
                ledger should haveInactiveContractIds()
            }
          }
        }

        "contract ID in ledger and disclosure table" in {
          forAll(disclosedContractIds) { case (disclosedContract, _) =>
            val (result, ledger) =
              evaluateSExpr(
                SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                // contract owned by ledgerParty
                getContract = Map(contractId -> ledgerContract),
                // contract owned by disclosureParty
                disclosedContracts = ImmArray(disclosedContract),
              )

            inside(result) {
              case Right(SValue.SAny(_, contract @ SValue.SRecord(`templateId`, _, _))) =>
                getOwner(contract.toUnnormalizedValue) shouldBe Some(disclosureParty)
                getMaintainer(contract.toUnnormalizedValue) shouldBe Some(maintainerParty)
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(contractId)
                ledger should haveInactiveContractIds()
            }
          }
        }
      }

      "contract IDs that are inactive" - {
        "ledger query fails when contract ID is not disclosed" in {
          val (result, ledger) =
            evaluateSExprWithSetup(
              e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                  """,
              Array(SContractId(contractId)),
            )(
              SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
              committers = Set(ledgerParty, maintainerParty),
              getContract = Map(contractId -> ledgerContract),
            )

          inside(result) {
            case Left(
                  SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                ) =>
              ledger should haveDisclosedContracts()
              ledger should haveCachedContractIds(contractId)
              ledger should haveInactiveContractIds(contractId)
          }
        }

        "disclosure table query fails when contract ID is disclosed" - {
          "contract ID in disclosure table only" in {
            forAll(disclosedContractIds) { case (disclosedContract, _) =>
              val (result, ledger) =
                evaluateSExprWithSetup(
                  e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                  Array(SContractId(contractId)),
                )(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  committers = Set(disclosureParty, maintainerParty),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Left(
                      SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                    ) =>
                  ledger should haveDisclosedContracts(disclosedContract)
                  ledger should haveCachedContractIds(contractId)
                  ledger should haveInactiveContractIds(contractId)
              }
            }
          }

          "contract ID in ledger and disclosure table" in {
            forAll(disclosedContractIds) { case (disclosedContract, _) =>
              val (result, ledger) =
                evaluateSExprWithSetup(
                  e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                  Array(SContractId(contractId)),
                )(
                  SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                  committers = Set(disclosureParty, ledgerParty, maintainerParty),
                  getContract = Map(contractId -> ledgerContract),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Left(
                      SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                    ) =>
                  ledger should haveDisclosedContracts(disclosedContract)
                  ledger should haveCachedContractIds(contractId)
                  ledger should haveInactiveContractIds(contractId)
              }
            }
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
        forAll(disclosedContractKeys) { case (disclosedContract, _) =>
          disclosedContract.contractId shouldBe SContractId(disclosureContractId)
          getOwner(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
          getMaintainer(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(
            maintainerParty
          )
        }
      }

      "ledger queried when contract key is not disclosed" in {
        val (result, ledger) =
          evaluateSExpr(
            SBUFetchKey(templateId)(SEValue(contractSKey)),
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
          )

        result shouldBe Right(SValue.SContractId(ledgerContractId))
        ledger should haveDisclosedContracts()
        ledger should haveCachedContractIds(ledgerContractId)
        ledger should haveInactiveContractIds()
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          forAll(disclosedContractKeys) { case (disclosedContract, label) =>
            val (result, ledger) =
              evaluateSExpr(
                SBUFetchKey(templateId)(SEValue(contractSKey)),
                committers = Set(disclosureParty),
                disclosedContracts = ImmArray(disclosedContract),
              )

            label match {
              case "disclosedContractNoHash" =>
                // Contract ID has no hash, so we serve key using the ledger (where key is non-existent)
                result shouldBe Left(SError.SErrorDamlException(ContractKeyNotFound(contractKey)))
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds()
                ledger should haveInactiveContractIds()

              case "disclosedContractWithHash" =>
                // Contract ID has a hash, so we serve key using the disclosure table
                result shouldBe Right(SValue.SContractId(disclosureContractId))
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(disclosureContractId)
                ledger should haveInactiveContractIds()
            }
          }
        }

        "contract key in ledger and disclosure table" in {
          forAll(disclosedContractKeys) { case (disclosedContract, label) =>
            val (result, ledger) =
              evaluateSExpr(
                SBUFetchKey(templateId)(SEValue(contractSKey)),
                committers = Set(disclosureParty, ledgerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerContract),
                disclosedContracts = ImmArray(disclosedContract),
              )

            label match {
              case "disclosedContractNoHash" =>
                // Contract ID has no hash, so we serve key using the ledger
                result shouldBe Right(SValue.SContractId(ledgerContractId))
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(ledgerContractId)
                ledger should haveInactiveContractIds()

              case "disclosedContractWithHash" =>
                // Contract ID has a hash, so we serve key using the disclosure table
                result shouldBe Right(SValue.SContractId(disclosureContractId))
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(disclosureContractId)
                ledger should haveInactiveContractIds()
            }
          }
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          val (result, ledger) =
            evaluateSExprWithSetup(
              e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                  """,
              Array(SContractId(ledgerContractId)),
            )(
              SBUFetchKey(templateId)(SEValue(contractSKey)),
              committers = Set(ledgerParty),
              getKey = Map(
                GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
              ),
              getContract = Map(ledgerContractId -> ledgerContract),
            )

          inside(result) {
            case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
              ledger should haveDisclosedContracts()
              ledger should haveCachedContractIds(ledgerContractId)
              ledger should haveInactiveContractIds(ledgerContractId)
          }
        }

        "disclosure table query fails when contract key is disclosed" - {
          "contract key in disclosure table only" in {
            forAll(disclosedContractKeys) { case (disclosedContract, _) =>
              val (result, ledger) =
                evaluateSExprWithSetup(
                  e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                  Array(SContractId(disclosureContractId)),
                )(
                  SBUFetchKey(templateId)(SEValue(contractSKey)),
                  committers = Set(disclosureParty, maintainerParty),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) {
                case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                  ledger should haveDisclosedContracts(disclosedContract)
                  ledger should haveCachedContractIds(disclosureContractId)
                  ledger should haveInactiveContractIds(disclosureContractId)
              }
            }
          }

          "contract key in ledger and disclosure table" in {
            for (contractIdToBurn <- Set(ledgerContractId, disclosureContractId)) {
              forAll(disclosedContractKeys) { case (disclosedContract, _) =>
                // Exercising a single contract ID is sufficient to make the key inactive
                val (result, ledger) =
                  evaluateSExprWithSetup(
                    e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                    Array(SContractId(contractIdToBurn)),
                  )(
                    SBUFetchKey(templateId)(SEValue(contractSKey)),
                    committers = Set(disclosureParty, ledgerParty, maintainerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(
                        contractKey,
                        Set(maintainerParty),
                      ) -> ledgerContractId
                    ),
                    getContract = Map(ledgerContractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) {
                  case Left(SError.SErrorDamlException(ContractKeyNotFound(`contractKey`))) =>
                    ledger should haveDisclosedContracts(disclosedContract)
                    ledger should haveCachedContractIds(contractIdToBurn)
                    ledger should haveInactiveContractIds(contractIdToBurn)
                }
              }
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
        forAll(disclosedContractKeys) { case (disclosedContract, _) =>
          disclosedContract.contractId shouldBe SContractId(disclosureContractId)
          getOwner(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(disclosureParty)
          getMaintainer(disclosedContract.argument.toUnnormalizedValue) shouldBe Some(
            maintainerParty
          )
        }
      }

      "ledger queried when contract key is not disclosed" in {
        val (result, ledger) =
          evaluateSExpr(
            SBULookupKey(templateId)(SEValue(contractSKey)),
            committers = Set(ledgerParty),
            getKey = Map(
              GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
            ),
            getContract = Map(ledgerContractId -> ledgerContract),
          )

        result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(ledgerContractId))))
        ledger should haveDisclosedContracts()
        ledger should haveCachedContractIds(ledgerContractId)
        ledger should haveInactiveContractIds()
      }

      "disclosure table queried when contract key is disclosed" - {
        "contract key in disclosure table only" in {
          forAll(disclosedContractKeys) { case (disclosedContract, label) =>
            val (result, ledger) =
              evaluateSExpr(
                SBULookupKey(templateId)(SEValue(contractSKey)),
                committers = Set(disclosureParty),
                disclosedContracts = ImmArray(disclosedContract),
              )

            label match {
              case "disclosedContractNoHash" =>
                // Contract ID has no hash, so we serve key using the ledger (where key is non-existent)
                result shouldBe Right(SValue.SOptional(None))
                ledger.cachedContracts.keySet shouldBe Set.empty
                ledger.ptx.disclosedContracts shouldBe ImmArray(disclosedContract)
                ledger.ptx.contractState.activeState.consumedBy.keySet shouldBe Set.empty

              case "disclosedContractWithHash" =>
                // Contract ID has a hash, so we serve key using the disclosure table
                result shouldBe Right(
                  SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))
                )
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(disclosureContractId)
                ledger should haveInactiveContractIds()
            }
          }
        }

        "contract key in ledger and disclosure table" in {
          forAll(disclosedContractKeys) { case (disclosedContract, label) =>
            val (result, ledger) =
              evaluateSExpr(
                SBULookupKey(templateId)(SEValue(contractSKey)),
                committers = Set(disclosureParty, ledgerParty),
                getKey = Map(
                  GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
                ),
                getContract = Map(ledgerContractId -> ledgerContract),
                disclosedContracts = ImmArray(disclosedContract),
              )

            label match {
              case "disclosedContractNoHash" =>
                // Contract ID has no hash, so we serve key using the ledger
                result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(ledgerContractId))))
                ledger.cachedContracts.keySet shouldBe Set(ledgerContractId)
                ledger.ptx.disclosedContracts shouldBe ImmArray(disclosedContract)
                ledger.ptx.contractState.activeState.consumedBy.keySet shouldBe Set.empty

              case "disclosedContractWithHash" =>
                // Contract ID has a hash, so we serve key using the disclosure table
                result shouldBe Right(
                  SValue.SOptional(Some(SValue.SContractId(disclosureContractId)))
                )
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(disclosureContractId)
                ledger should haveInactiveContractIds()
            }
          }
        }
      }

      "disclosed contract keys that are inactive" - {
        "ledger query fails when contract key is not disclosed" in {
          val (result, ledger) =
            evaluateSExprWithSetup(
              e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                  """,
              Array(SContractId(ledgerContractId)),
            )(
              SBULookupKey(templateId)(SEValue(contractSKey)),
              committers = Set(ledgerParty),
              getKey = Map(
                GlobalKeyWithMaintainers(contractKey, Set(maintainerParty)) -> ledgerContractId
              ),
              getContract = Map(ledgerContractId -> ledgerContract),
            )

          inside(result) { case Right(SValue.SOptional(None)) =>
            ledger should haveDisclosedContracts()
            ledger should haveCachedContractIds(ledgerContractId)
            ledger should haveInactiveContractIds(ledgerContractId)
          }
        }

        "disclosure table query fails when contract key is disclosed" - {
          "contract key in disclosure table only" in {
            forAll(disclosedContractKeys) { case (disclosedContract, _) =>
              val (result, ledger) =
                evaluateSExprWithSetup(
                  e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                  Array(SContractId(disclosureContractId)),
                )(
                  SBULookupKey(templateId)(SEValue(contractSKey)),
                  committers = Set(disclosureParty, maintainerParty),
                  disclosedContracts = ImmArray(disclosedContract),
                )

              inside(result) { case Right(SValue.SOptional(None)) =>
                ledger should haveDisclosedContracts(disclosedContract)
                ledger should haveCachedContractIds(disclosureContractId)
                ledger should haveInactiveContractIds(disclosureContractId)
              }
            }
          }

          "contract key in ledger and disclosure table" in {
            for (contractIdToBurn <- Set(ledgerContractId, disclosureContractId)) {
              // Exercising a single contract ID is sufficient to make the key inactive
              forAll(disclosedContractKeys) { case (disclosedContract, _) =>
                val (result, ledger) =
                  evaluateSExprWithSetup(
                    e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:exercise contractId
                    """,
                    Array(SContractId(contractIdToBurn)),
                  )(
                    SBULookupKey(templateId)(SEValue(contractSKey)),
                    committers = Set(disclosureParty, ledgerParty, maintainerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(
                        contractKey,
                        Set(maintainerParty),
                      ) -> ledgerContractId
                    ),
                    getContract = Map(ledgerContractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) { case Right(SValue.SOptional(None)) =>
                  ledger should haveDisclosedContracts(disclosedContract)
                  ledger should haveCachedContractIds(contractIdToBurn)
                  ledger should haveInactiveContractIds(contractIdToBurn)
                }
              }
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

         val exercise: ContractId TestMod:House -> Update Unit =
           \(contractId: ContractId TestMod:House) ->
             exercise @TestMod:House Destroy contractId ();

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
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val templateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val keyType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Key")
  val contractKey: GlobalKey = buildContractKey(maintainerParty)
  val contractSKey: SValue = buildContractSKey(maintainerParty)
  val ledgerContractKey: GlobalKey = buildContractKey(ledgerParty)
  val ledgerContract: Value.VersionedContractInstance = buildContract(ledgerParty, maintainerParty)
  val disclosedContractIds =
    TableDrivenPropertyChecks.Table(
      ("disclosedContract", "caseLabel"),
      (
        buildDisclosedContract(contractId, disclosureParty, maintainerParty, withHash = false),
        "disclosedContractNoHash",
      ),
      (
        buildDisclosedContract(contractId, disclosureParty, maintainerParty),
        "disclosedContractWithHash",
      ),
    )
  val disclosedContractKeys =
    TableDrivenPropertyChecks.Table(
      ("disclosedContract", "caseLabel"),
      (
        buildDisclosedContract(
          disclosureContractId,
          disclosureParty,
          maintainerParty,
          withHash = false,
        ),
        "disclosedContractNoHash",
      ),
      (
        buildDisclosedContract(disclosureContractId, disclosureParty, maintainerParty),
        "disclosedContractWithHash",
      ),
    )

  def buildDisclosedContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
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
        `templateId`,
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
