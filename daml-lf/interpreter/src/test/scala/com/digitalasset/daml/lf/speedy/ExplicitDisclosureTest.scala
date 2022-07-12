// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.speedy.SExpr.SEValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.speedy.SBuiltin.SBFetchAny
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.transaction.{TransactionVersion, Versioned}

class ExplicitDisclosureTest extends AnyFreeSpec with Inside with Matchers {

  import SpeedyTestLib.loggingContext

  val emptyPkg: PureCompiledPackages = PureCompiledPackages.Empty
  val contractId: ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val templateId = ???
  val cachedContract: Value.VersionedContractInstance = Versioned(
    TransactionVersion.DevVersions.max,
    ContractInstance(templateId, ???, ""),
  )
  val cachedContractKey = ???
  val disclosedContract = ???
  val disclosedContractKey = ???
  val unknownContractId = ???
  val unknownContractKey = ???

  // TODO: add in tests for inactive contracts

  "disclosed contract behaviour" - {
    "using on ledger contracts" - {
      "with no duplicate disclosed contract" - {
        "succeed with cached contract IDs" in {
          val result = SpeedyTestLib.run(
            machine = Speedy.Machine.fromPureSExpr(emptyPkg, SBFetchAny(SEValue(SContractId(contractId)), SEValue.None)),
            getContract = Map(contractId -> cachedContract),
          )

          result shouldBe Right(SValue.SContractId(contractId))
        }

//        "succeed with cached contract keys" in {
//          val result = SpeedyTestLib.run(
//            machine = Speedy.Machine.fromPureExpr(emptyPkg, EVal(cachedContractKey)),
//            getKey = {
//              case keyId if cachedContractKey == keyId => ???
//            },
//          )
//
//          result shouldBe Right(SValue.SContractId(cachedContract.unversioned))
//        }
      }

//      "with duplicate disclosed contract" - {
//        "succeed with cached contract IDs" in {
//          val result = SpeedyTestLib.run(
//            machine =
//              Speedy.Machine.fromPureExpr(
//                emptyPkg,
//                EVal(cachedContract),
//                disclosedContracts = ImmArray(DisclosedContract(???)),
//              ),
//            getContract = {
//              case `contractId` => ???
//            },
//          )
//
//          result shouldBe Right(SValue.SContractId(cachedContract))
//        }
//
//        "succeed with cached contract keys" in {
//          val result = SpeedyTestLib.run(
//            machine =
//              Speedy.Machine.fromPureExpr(
//                emptyPkg,
//                EVal(cachedContractKey),
//                disclosedContracts = ImmArray(DisclosedContract(???)),
//              ),
//            getKey = {
//              case keyId if cachedContractKey == keyId => ???
//            },
//          )
//
//          result shouldBe Right(SValue.SContractId(cachedContract.unversioned))
//        }
//      }
//    }
//
//    "using explicitly disclosed contract" - {
//      "succeed with disclosed contract IDs" in {
//        val result = SpeedyTestLib.run(
//          machine =
//            Speedy.Machine.fromPureExpr(
//              emptyPkg,
//              EVal(disclosedContract),
//              disclosedContracts = ImmArray(DisclosedContract(???)),
//            ),
//        )
//
//        result shouldBe Right(SValue.SContractId(disclosedContract))
//      }
//
//      "succeed with disclosed contract keys" in {
//        val result = SpeedyTestLib.run(
//          machine =
//            Speedy.Machine.fromPureExpr(
//              emptyPkg,
//              EVal(disclosedContractKey),
//              disclosedContracts = ImmArray(DisclosedContract(???)),
//            ),
//        )
//
//        result shouldBe Right(SValue.SContractId(disclosedContract))
//      }
//    }
//
//    "using unknown contracts" - {
//      "fail using unknown contract IDs" in {
//        val result = SpeedyTestLib.run(
//          machine = Speedy.Machine.fromPureExpr(emptyPkg, EVal(unknownContract)),
//        )
//
//        inside(result) { case Left(SError.SErrorDamlException(Error.ContractNotFound(contractId))) =>
//          contractId shouldBe unknownContractId
//        }
//      }
//
//      "fail using unknown contract keys" in {
//        val result = SpeedyTestLib.run(
//          machine = Speedy.Machine.fromPureExpr(emptyPkg, EVal(unknownContractKey)),
//        )
//
//        inside(result) { case Left(SError.SErrorDamlException(Error.ContractNotFound(unknownContractId))) =>
//          loc shouldBe "com.daml.lf.speedy.Speedy.Machine.lookupVal"
//          msg should include(s"definition ${LfDefRef(unknownContractKey)} not found")
//        }
//      }
//    }
  }
}
