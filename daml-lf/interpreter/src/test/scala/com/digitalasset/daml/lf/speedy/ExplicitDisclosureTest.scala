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
    "with contracts stored in a single location" - {
      "off ledger" - {
        "active contracts" - {
          "fetching contracts" - {
            "fail to evaluate known contract IDs" - {
              "using SBuiltin" in {
                val (result, events) =
                  evaluateSExpr(
                    SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                    getContract = Map(contractId -> ledgerContract),
                    onLedger = false,
                  )
                val expectedFunction = SExpr.SEBuiltin(SBFetchAny)
                val expectedArgs = Array(
                  SEValue(SContractId(contractId)),
                  SEValue(SValue.SOptional(None)),
                )

                result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                events shouldBe Seq.empty
              }

              "using AST.Expr" in {
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    getContract = Map(contractId -> ledgerContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }

            "fail to evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBFetchAny)
                  val expectedArgs = Array(
                    SEValue(SContractId(contractId)),
                    SEValue(SValue.SOptional(None)),
                  )

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                      Array(SContractId(contractId)),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }
          }

          "fetching contract keys" - {
            "fail to evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBUFetchKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBUFetchKey(templateId))
                  val expectedArgs = Array(SEValue(buildContractSKey(disclosureParty)))

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate known contract IDs" - {
              "using SBuiltin" in {
                val (result, events) =
                  evaluateSExpr(
                    SBULookupKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    onLedger = false,
                  )
                val expectedFunction = SExpr.SEBuiltin(SBULookupKey(templateId))
                val expectedArgs = Array(SEValue(buildContractSKey(ledgerParty)))

                result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                events shouldBe Seq.empty
              }

              "using AST.Expr" in {
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }
          }
        }

        "inactive contracts" - {
          "fetching contracts" - {
            "fail to fetch known contract IDs" in {
              val (result, events) =
                evaluateExprApp(
                  e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                  Array(SContractId(contractId)),
                  committers = Set(ledgerParty),
                  getKey = Map(
                    GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                  ),
                  getContract = Map(contractId -> ledgerContract),
                  onLedger = false,
                )

              result should beAnEvaluationFailure
              events shouldBe Array.empty
            }

            "fail to fetch disclosed contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    committers = Set(disclosureParty),
                    disclosedContracts = ImmArray(disclosedContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Array.empty
              }
            }
          }

          "fetching contracts keys" - {
            "fail to evaluate disclosed contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(disclosureParty),
                    disclosedContracts = ImmArray(disclosedContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate known contract IDs" in {
              val (result, events) =
                evaluateExprApp(
                  e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                  Array(
                    SContractId(contractId),
                    SValue.SText(testKeyName),
                    SValue.SParty(ledgerParty),
                  ),
                  committers = Set(ledgerParty),
                  getKey = Map(
                    GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                  ),
                  getContract = Map(contractId -> ledgerContract),
                  onLedger = false,
                )

              result should beAnEvaluationFailure
              events shouldBe Seq.empty
            }
          }
        }
      }

      "on ledger" - {
        "active contracts" - {
          "fetching contracts" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                val (result, events) =
                  evaluateSExpr(
                    SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                    getContract = Map(contractId -> ledgerContract),
                  )

                inside(result) {
                  case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                    fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                    values shouldBe ArrayList(SValue.SParty(ledgerParty))
                    events shouldBe Seq("contractById queried", "getContract queried")
                }
              }

              "using AST.Expr" in {
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    committers = Set(ledgerParty),
                    getContract = Map(contractId -> ledgerContract),
                  )

                inside(result) { case Right(SValue.SRecord(`templateId`, fields, values)) =>
                  fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                  values shouldBe ArrayList(SValue.SParty(ledgerParty))
                  events shouldBe Seq("contractById queried", "getContract queried")
                }
              }
            }

            "evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                      fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                      values shouldBe ArrayList(SValue.SParty(disclosureParty))
                      events shouldBe Seq("contractById queried")
                  }
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                      Array(SContractId(contractId)),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) { case Right(SValue.SRecord(`templateId`, fields, values)) =>
                    fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                    values shouldBe ArrayList(SValue.SParty(disclosureParty))
                    events shouldBe Seq("contractById queried")
                  }
                }
              }
            }
          }

          "fetching contract keys" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                val (result, events) =
                  evaluateSExpr(
                    SBUFetchKey(templateId)(SEValue(ledgerContractSKey)),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                  )

                result shouldBe Right(SValue.SContractId(contractId))
                events shouldBe Seq(
                  "contractIdByKey queried",
                  "getKey queried",
                  "contractById queried",
                  "getContract queried",
                )
              }

              "using AST.Expr" in {
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(
                        buildContractKey(ledgerParty),
                        Set(ledgerParty),
                      ) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                  )

                inside(result) {
                  case Right(SValue.SStruct(_, ArrayList(_, SValue.SContractId(`contractId`)))) =>
                    events shouldBe Seq(
                      "contractIdByKey queried",
                      "getKey queried",
                      "contractById queried",
                      "getContract queried",
                    )
                }
              }
            }

            "evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, label) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBUFetchKey(templateId)(SEValue(disclosureContractSKey)),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Left(
                          SError.SErrorDamlException(ContractKeyNotFound(`disclosureContractKey`))
                        ) =>
                      // Contract ID has no hash, so we serve key using the ledger
                      label shouldBe "disclosedContractNoHash"
                      events shouldBe Seq("contractIdByKey queried", "getKey queried")

                    case Right(SValue.SContractId(`contractId`)) =>
                      // Contract ID has a hash, so we serve key using the disclosure table
                      label shouldBe "disclosedContractWithHash"
                      events shouldBe Seq("contractIdByKey queried", "contractById queried")
                  }
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, label) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(disclosureParty),
                      ),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Left(
                          SError.SErrorDamlException(ContractKeyNotFound(`disclosureContractKey`))
                        ) =>
                      // Contract ID has no hash, so we serve key using the ledger
                      label shouldBe "disclosedContractNoHash"
                      events shouldBe Seq("contractIdByKey queried", "getKey queried")

                    case Right(SValue.SStruct(_, ArrayList(_, SValue.SContractId(`contractId`)))) =>
                      // Contract ID has a hash, so we serve key using the disclosure table
                      label shouldBe "disclosedContractWithHash"
                      events shouldBe Seq("contractIdByKey queried", "contractById queried")
                  }
                }
              }
            }
          }

          "looking up contract keys" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                val (result, events) =
                  evaluateSExpr(
                    SBULookupKey(templateId)(SEValue(ledgerContractSKey)),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                  )

                result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(contractId))))
                events shouldBe Seq(
                  "contractIdByKey queried",
                  "getKey queried",
                  "contractById queried",
                  "getContract queried",
                )
              }

              "using AST.Expr" in {
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(
                        buildContractKey(ledgerParty),
                        Set(ledgerParty),
                      ) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                  )

                result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(contractId))))
                events shouldBe Seq(
                  "contractIdByKey queried",
                  "getKey queried",
                  "contractById queried",
                  "getContract queried",
                )
              }
            }

            "evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, label) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBULookupKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Right(SValue.SOptional(None)) =>
                      // Contract ID has no hash, so we serve key using the ledger
                      label shouldBe "disclosedContractNoHash"
                      events shouldBe Seq("contractIdByKey queried", "getKey queried")

                    case Right(SValue.SOptional(Some(SValue.SContractId(`contractId`)))) =>
                      // Contract ID has a hash, so we serve key using the disclosure table
                      label shouldBe "disclosedContractWithHash"
                      events shouldBe Seq("contractIdByKey queried", "contractById queried")
                  }
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, label) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(disclosureParty),
                      ),
                      committers = Set(disclosureParty),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Right(SValue.SOptional(None)) =>
                      // Contract ID has no hash, so we serve key using the ledger
                      label shouldBe "disclosedContractNoHash"
                      events shouldBe Seq("contractIdByKey queried", "getKey queried")

                    case Right(SValue.SOptional(Some(SValue.SContractId(`contractId`)))) =>
                      // Contract ID has a hash, so we serve key using the disclosure table
                      label shouldBe "disclosedContractWithHash"
                      events shouldBe Seq("contractIdByKey queried", "contractById queried")
                  }
                }
              }
            }
          }
        }

        "inactive contracts" - {
          "fetching contracts" - {
            "fail to fetch known contract IDs" in {
              val (result, events) =
                evaluateExprApp(
                  e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                  Array(SContractId(contractId)),
                  committers = Set(ledgerParty),
                  getKey = Map(
                    GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                  ),
                  getContract = Map(contractId -> ledgerContract),
                )

              inside(result) {
                case Left(
                      SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                    ) =>
                  events shouldBe Array("contractById queried", "getContract queried")
              }
            }

            "fail to fetch disclosed contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    committers = Set(disclosureParty),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) {
                  case Left(
                        SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                      ) =>
                    events shouldBe Array("contractById queried")
                }
              }
            }
          }

          "fetching contract keys" - {
            "fail to fetch known contract keys" in {
              val (result, events) =
                evaluateExprApp(
                  e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                  Array(
                    SContractId(contractId),
                    SValue.SText(testKeyName),
                    SValue.SParty(ledgerParty),
                  ),
                  committers = Set(ledgerParty),
                  getKey = Map(
                    GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                  ),
                  getContract = Map(contractId -> ledgerContract),
                )

              inside(result) { case Left(SError.SErrorDamlException(ContractKeyNotFound(key))) =>
                key shouldBe buildContractKey(ledgerParty)
                events shouldBe Array("contractById queried", "getContract queried")
              }
            }

            "fail to fetch disclosed contract keys" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(disclosureParty),
                    ),
                    committers = Set(disclosureParty),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) { case Left(SError.SErrorDamlException(ContractKeyNotFound(key))) =>
                  key shouldBe buildContractKey(disclosureParty)
                  events shouldBe Array("contractById queried")
                }
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate lookup contract keys" in {
              val (result, events) =
                evaluateExprApp(
                  e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                  Array(
                    SContractId(contractId),
                    SValue.SText(testKeyName),
                    SValue.SParty(ledgerParty),
                  ),
                  committers = Set(ledgerParty),
                  getKey = Map(
                    GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                  ),
                  getContract = Map(contractId -> ledgerContract),
                )

              inside(result) { case Right(SValue.SOptional(None)) =>
                events shouldBe Array("contractById queried", "getContract queried")
              }
            }

            "fail to lookup disclosed contract keys" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(disclosureParty),
                    ),
                    committers = Set(disclosureParty),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) { case Right(SValue.SOptional(None)) =>
                  events shouldBe Array("contractById queried")
                }
              }
            }
          }
        }
      }
    }

    "with contracts stored in multiple locations" - {
      "off ledger" - {
        "active contracts" - {
          "fetching contracts" - {
            "fail to evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBFetchAny)
                  val expectedArgs = Array(
                    SEValue(SContractId(contractId)),
                    SEValue(SValue.SOptional(None)),
                  )

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                      Array(SContractId(contractId)),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }
          }

          "fetching contract keys" - {
            "fail to evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBUFetchKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                      committers = Set(ledgerParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBUFetchKey(templateId))
                  val expectedArgs = Array(SEValue(buildContractSKey(ledgerParty)))

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(ledgerParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }

            "fail to evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBUFetchKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBUFetchKey(templateId))
                  val expectedArgs = Array(SEValue(buildContractSKey(disclosureParty)))

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBULookupKey(templateId)(SEValue(buildContractSKey(ledgerParty))),
                      committers = Set(ledgerParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBULookupKey(templateId))
                  val expectedArgs = Array(SEValue(buildContractSKey(ledgerParty)))

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(ledgerParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }

            "fail to evaluate disclosed contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBULookupKey(templateId)(SEValue(buildContractSKey(disclosureParty))),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )
                  val expectedFunction = SExpr.SEBuiltin(SBULookupKey(templateId))
                  val expectedArgs = Array(SEValue(buildContractSKey(disclosureParty)))

                  result should beAnFunctionEvalFailure(expectedFunction, expectedArgs)
                  events shouldBe Seq.empty
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                      onLedger = false,
                    )

                  result should beAnEvaluationFailure
                  events shouldBe Seq.empty
                }
              }
            }
          }
        }

        "inactive contracts" - {
          "fetching contracts" - {
            "fail to evaluate known contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }
          }

          "fetching contracts keys" - {
            "fail to evaluate disclosed contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(disclosureParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate known contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(ledgerParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                    onLedger = false,
                  )

                result should beAnEvaluationFailure
                events shouldBe Seq.empty
              }
            }
          }
        }
      }

      "on ledger" - {
        "active contracts" - {
          "fetching contracts" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBFetchAny(SEValue(SContractId(contractId)), SEValue.None),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  // Ledger contract is not cached, so we always return the disclosed contract
                  inside(result) {
                    case Right(SValue.SAny(_, SValue.SRecord(`templateId`, fields, values))) =>
                      fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                      values shouldBe ArrayList(SValue.SParty(disclosureParty))
                      events shouldBe Seq("contractById queried")
                  }
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) ->
                          TestMod:fetch_by_id contractId
                    """,
                      Array(SContractId(contractId)),
                      committers = Set(disclosureParty),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  // Ledger contract is not cached, so we always return the disclosed contract
                  inside(result) { case Right(SValue.SRecord(`templateId`, fields, values)) =>
                    fields shouldBe ImmArray(Ref.Name.assertFromString("owner"))
                    values shouldBe ArrayList(SValue.SParty(disclosureParty))
                    events shouldBe Seq("contractById queried")
                  }
                }
              }
            }
          }

          "fetching contract keys" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBUFetchKey(templateId)(SEValue(ledgerContractSKey)),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  result shouldBe Right(SValue.SContractId(`contractId`))
                  events shouldBe Seq(
                    "contractIdByKey queried",
                    "getKey queried",
                    "contractById queried",
                  )
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(ledgerParty, disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(
                          buildContractKey(ledgerParty),
                          Set(ledgerParty),
                        ) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  inside(result) {
                    case Right(SValue.SStruct(_, ArrayList(_, SValue.SContractId(`contractId`)))) =>
                      events shouldBe Seq(
                        "contractIdByKey queried",
                        "getKey queried",
                        "contractById queried",
                      )
                  }
                }
              }
            }
          }

          "looking up contract keys" - {
            "evaluate known contract IDs" - {
              "using SBuiltin" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateSExpr(
                      SBULookupKey(templateId)(SEValue(ledgerContractSKey)),
                      committers = Set(disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(`contractId`))))
                  events shouldBe Seq(
                    "contractIdByKey queried",
                    "getKey queried",
                    "contractById queried",
                  )
                }
              }

              "using AST.Expr" in {
                forAll(disclosedContracts) { case (disclosedContract, _) =>
                  val (result, events) =
                    evaluateExprApp(
                      e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                      Array(
                        SContractId(contractId),
                        SValue.SText(testKeyName),
                        SValue.SParty(ledgerParty),
                      ),
                      committers = Set(ledgerParty, disclosureParty),
                      getKey = Map(
                        GlobalKeyWithMaintainers(
                          buildContractKey(ledgerParty),
                          Set(ledgerParty),
                        ) -> contractId
                      ),
                      getContract = Map(contractId -> ledgerContract),
                      disclosedContracts = ImmArray(disclosedContract),
                    )

                  result shouldBe Right(SValue.SOptional(Some(SValue.SContractId(`contractId`))))
                  events shouldBe Seq(
                    "contractIdByKey queried",
                    "getKey queried",
                    "contractById queried",
                  )
                }
              }
            }
          }
        }

        "inactive contracts" - {
          "fetching contracts" - {
            "fail to fetch known contract IDs" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_id contractId
                    """,
                    Array(SContractId(contractId)),
                    committers = Set(disclosureParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) {
                  case Left(
                        SError.SErrorDamlException(ContractNotActive(`contractId`, `templateId`, _))
                      ) =>
                    events shouldBe Array("contractById queried")
                }
              }
            }
          }

          "fetching contract keys" - {
            "fail to fetch known contract keys" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:fetch_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(disclosureParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) { case Left(SError.SErrorDamlException(ContractKeyNotFound(key))) =>
                  key shouldBe buildContractKey(ledgerParty)
                  events shouldBe Array(
                    "contractById queried",
                    "contractIdByKey queried",
                    "getKey queried",
                  )
                }
              }
            }
          }

          "looking up contract keys" - {
            "fail to evaluate lookup contract keys" in {
              forAll(disclosedContracts) { case (disclosedContract, _) =>
                val (result, events) =
                  evaluateExprApp(
                    e"""\(contractId: ContractId TestMod:House) (label: Text) (maintainer: Party) ->
                          ubind ignore: Unit <- TestMod:exercise contractId
                          in TestMod:lookup_by_key label (Some @Party maintainer)
                    """,
                    Array(
                      SContractId(contractId),
                      SValue.SText(testKeyName),
                      SValue.SParty(ledgerParty),
                    ),
                    committers = Set(ledgerParty, disclosureParty),
                    getKey = Map(
                      GlobalKeyWithMaintainers(ledgerContractKey, Set(ledgerParty)) -> contractId
                    ),
                    getContract = Map(contractId -> ledgerContract),
                    disclosedContracts = ImmArray(disclosedContract),
                  )

                inside(result) { case Right(SValue.SOptional(None)) =>
                  events shouldBe Array(
                    "contractById queried",
                    "contractIdByKey queried",
                    "getKey queried",
                  )
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

  import SpeedyTestLib.Implicits._

  val testKeyName: String = "test-key"
  val pkg: PureCompiledPackages = SpeedyTestLib.typeAndCompile(
    p"""
       module TestMod {

         record @serializable Key = { label: Text, maintainers: List Party };

         record @serializable House = { owner: Party };
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
              (TestMod:Key { label = "test-key", maintainers = (TestMod:listOf @Party (TestMod:House {owner} this)) })
              (\(key: TestMod:Key) -> (TestMod:Key {maintainers} key));
         };

         val create: Party -> Update (ContractId TestMod:House) =
           \(party: Party) ->
             create @TestMod:House TestMod:House { owner = party };

         val exercise: ContractId TestMod:House -> Update Unit =
           \(contractId: ContractId TestMod:House) ->
             exercise @TestMod:House Destroy contractId ();

         val fetch_by_id: ContractId TestMod:House -> Update TestMod:House =
           \(contractId: ContractId TestMod:House) ->
             fetch_template @TestMod:House contractId;

         val fetch_by_key: Text -> Option Party -> Update <contract: TestMod:House, contractId: ContractId TestMod:House> =
           \(label: Text) (maintainer: Option Party) ->
             let key: TestMod:Key = TestMod:Key { label = label, maintainers = (TestMod:optToList @Party maintainer) }
             in fetch_by_key @TestMod:House key;

         val lookup_by_key: Text -> Option Party -> Update (Option(ContractId TestMod:House)) =
           \(label: Text) (maintainer: Option Party) ->
             let key: TestMod:Key = TestMod:Key { label = label, maintainers = (TestMod:optToList @Party maintainer) }
             in lookup_by_key @TestMod:House key;

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
  val ledgerParty: IdString.Party = Ref.Party.assertFromString("ledgerParty")
  val disclosureParty: IdString.Party = Ref.Party.assertFromString("disclosureParty")
  val contractId: ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val templateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val keyType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:Key")
  val ledgerContractKey: GlobalKey = buildContractKey(ledgerParty)
  val ledgerContractSKey: SValue = buildContractSKey(ledgerParty)
  val disclosureContractKey: GlobalKey = buildContractKey(disclosureParty)
  val disclosureContractSKey: SValue = buildContractSKey(disclosureParty)
  val ledgerContract: Value.VersionedContractInstance = buildContract(ledgerParty)
  val disclosedContracts =
    TableDrivenPropertyChecks.Table(
      ("disclosedContract", "caseLabel"),
      (buildDisclosedContract(disclosureParty, withHash = false), "disclosedContractNoHash"),
      (buildDisclosedContract(disclosureParty), "disclosedContractWithHash"),
    )

  def buildDisclosedContract(maintainer: Party, withHash: Boolean = true): DisclosedContract = {
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
        ImmArray(Ref.Name.assertFromString("owner")),
        ArrayList(SValue.SParty(maintainer)),
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

  def evaluateExprApp(
      expr: Ast.Expr,
      args: Array[SValue],
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      onLedger: Boolean = true,
  ): (Either[SError.SError, SValue], Seq[String]) = {
    // A token function closure is added as part of compiling the Expr
    val sexpr = pkg.compiler.unsafeCompile(expr)

    evaluateSExpr(
      if (args.isEmpty) sexpr else SExpr.SEApp(sexpr, args.map(SEValue(_))),
      committers = committers,
      disclosedContracts = disclosedContracts,
      getContract = getContract,
      getKey = getKey,
      onLedger = onLedger,
      requireTokenClosure = false,
    )
  }

  def evaluateSExpr(
      sexpr: SExpr.SExpr,
      committers: Set[Party] = Set.empty,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray.Empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      onLedger: Boolean = true,
      requireTokenClosure: Boolean = true,
  ): (Either[SError.SError, SValue], Seq[String]) = {
    import SpeedyTestLib.loggingContext

    val traceLog = new TestTraceLog()
    val machine =
      if (onLedger) {
        Speedy.Machine.fromUpdateSExpr(
          pkg,
          transactionSeed = crypto.Hash.hashPrivateKey("ExplicitDisclosureTest"),
          updateSE = if (requireTokenClosure) runUpdateSExpr(sexpr) else sexpr,
          committers = committers,
          disclosedContracts = disclosedContracts,
          traceLog = traceLog,
        )
      } else {
        Speedy.Machine.fromPureSExpr(
          pkg,
          expr = if (requireTokenClosure) runUpdateSExpr(sexpr) else sexpr,
          disclosedContracts = disclosedContracts,
          traceLog = traceLog,
        )
      }
    val result = SpeedyTestLib.run(
      machine = machine.traceDisclosureTable(traceLog),
      getContract = traceLog.tracePF("getContract queried", getContract),
      getKey = traceLog.tracePF("getKey queried", getKey),
    )

    (result, traceLog.getMessages)
  }

  def beAnFunctionEvalFailure(
      expectedFunction: SExpr.SEBuiltin,
      expectedArgs: Array[SExpr.SEValue],
  ): Matcher[Either[SError.SError, SValue]] = Matcher {
    case Right(
          SValue.SPAP(
            SValue.PClosure(_, SExpr.SEAppGeneral(function, args), frame),
            _,
            1,
          )
        ) =>
      MatchResult(
        function == expectedFunction
          && args.sameElements(expectedArgs)
          && frame.isEmpty,
        s"Evaluation failed: $function(${args.mkString("Array(", ", ", ")")}) != $expectedFunction(${expectedArgs
            .mkString("Array(", ", ", ")")})",
        s"Evaluation failed: $function(${args.mkString("Array(", ", ", ")")}) == $expectedFunction(${expectedArgs
            .mkString("Array(", ", ", ")")})",
      )

    case error =>
      MatchResult(
        matches = false,
        s"Evaluation unexpectedly failed with $error",
        s"Evaluation unexpectedly failed with $error",
      )
  }

  def beAnEvaluationFailure: Matcher[Either[SError.SError, SValue]] = Matcher {
    case Right(SValue.SPAP(SValue.PClosure(_, _, _), _, _)) =>
      MatchResult(
        matches = true,
        "Evaluation failed with partial application",
        "Evaluation failed with partial application",
      )

    case error =>
      MatchResult(
        matches = false,
        s"Evaluation unexpectedly failed with $error",
        s"Evaluation unexpectedly failed with $error",
      )
  }
}
