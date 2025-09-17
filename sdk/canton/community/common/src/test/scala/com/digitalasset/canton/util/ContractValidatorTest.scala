// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPackageName,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ValueText
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.Future

class ContractValidatorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  private val engine = new Engine(
    EngineConfig(LanguageVersion.StableVersions(LanguageVersion.Major.V2))
  )
  private val packageResolver: PackageResolver =
    _ => _ => FutureUnlessShutdown.pure(None)

  private val pureCrypto = new SymbolicPureCrypto()
  private val underTest = ContractValidator(pureCrypto, engine, packageResolver)

  private def shouldFailAuthentication(invalid: FatContractInstance): Future[Assertion] =
    underTest
      .authenticate(invalid, invalid.templateId.packageId)
      .value
      .map(e =>
        inside(e) { case Left(error) =>
          error should startWith("Contract did not validate")
        }
      )

  forEvery(Seq(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11)) {
    authContractIdVersion =>
      s"ContractAuthenticatorImpl with $authContractIdVersion" when {

        val contractInstance =
          ExampleContractFactory.build[CreatedAt](cantonContractIdVersion = authContractIdVersion)
        val fatContractInstance: FatContractInstance = contractInstance.inst
        val targetPackageId = contractInstance.templateId.packageId

        val keyWithMaintainers = ExampleContractFactory.buildKeyWithMaintainers()
        val contractInstanceWithKey = ExampleContractFactory.build[CreatedAt](
          cantonContractIdVersion = authContractIdVersion,
          keyOpt = Some(keyWithMaintainers),
        )

        "using a valid contract id" should {
          "correctly authenticate the contract" in {
            underTest
              .authenticate(fatContractInstance, targetPackageId)
              .value
              .map(_ shouldBe Either.unit)
          }
        }

        "using a normalized values" should {
          if (authContractIdVersion >= AuthenticatedContractIdVersionV11) {

            val unNormalizedArg = Value.ValueRecord(
              None,
              ImmArray(
                (None, Value.ValueOptional(Some(Value.ValueTrue))),
                (None, Value.ValueOptional(None)),
              ),
            )

            val normalizedArg = Value.ValueRecord(
              None,
              ImmArray(
                (None, Value.ValueOptional(Some(Value.ValueTrue)))
              ),
            )

            val unNormalizedContract = ExampleContractFactory.build(argument = unNormalizedArg)
            val normalizedContract =
              ExampleContractFactory.modify(unNormalizedContract, arg = Some(normalizedArg))

            "correctly authenticate the unNormalizedContract" in {
              underTest
                .authenticate(unNormalizedContract.inst, unNormalizedContract.templateId.packageId)
                .value
                .map(_ shouldBe Either.unit)
            }
            "correctly authenticate the normalizedContract" in {
              underTest
                .authenticate(normalizedContract.inst, normalizedContract.templateId.packageId)
                .value
                .map(_ shouldBe Either.unit)
            }

          }
        }

        "using an invalid contract id" should {
          "fail authentication" in {
            val invalidContractId = ExampleContractFactory.buildContractId()
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, contractId = Some(invalidContractId))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed salt/authentication data" should {
          "fail authentication" in {
            val authenticationData = ContractAuthenticationDataV1(TestSalt.generateSalt(42))(
              authContractIdVersion
            ).toLfBytes
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, authenticationData = Some(authenticationData))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed ledger time" should {
          "fail authentication" in {
            val changedTime =
              CreatedAt(contractInstance.inst.createdAt.time.add(Duration.ofDays(1L)))
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, createdAt = Some(changedTime))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed contract argument" should {
          "fail authentication" in {
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, arg = Some(ValueText("changed")))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed template-id" should {
          "fail authentication" in {
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstance,
                templateId = Some(LfTemplateId.assertFromString("definitely:changed:template")),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed package-name" should {
          "fail authentication" in {
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstance,
                packageName =
                  Some(LfPackageName.assertFromString("definitely-changed-package-name")),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using changed signatories" should {
          "fail authentication" in {
            val changedSignatory: LfPartyId =
              LfPartyId.assertFromString("changed::signatory")
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstance,
                metadata = Some(
                  ContractMetadata.tryCreate(
                    signatories = contractInstance.metadata.signatories + changedSignatory,
                    stakeholders = contractInstance.metadata.stakeholders + changedSignatory,
                    maybeKeyWithMaintainersVersioned =
                      contractInstance.metadata.maybeKeyWithMaintainersVersioned,
                  )
                ),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using changed observers" should {
          "fail authentication" in {
            val changedObserver: LfPartyId =
              LfPartyId.assertFromString("changed::observer")
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstance,
                metadata = Some(
                  ContractMetadata.tryCreate(
                    signatories = contractInstance.metadata.signatories,
                    stakeholders = contractInstance.metadata.stakeholders + changedObserver,
                    maybeKeyWithMaintainersVersioned =
                      contractInstance.metadata.maybeKeyWithMaintainersVersioned,
                  )
                ),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed key value" should {
          "fail authentication" in {
            val changeKey = keyWithMaintainers.copy(globalKey =
              LfGlobalKey.assertBuild(
                contractInstance.templateId,
                ValueText("changed"),
                contractInstance.inst.packageName,
              )
            )
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstanceWithKey,
                metadata = Some(
                  ContractMetadata.tryCreate(
                    signatories = contractInstanceWithKey.metadata.signatories,
                    stakeholders = contractInstanceWithKey.metadata.stakeholders,
                    maybeKeyWithMaintainersVersioned =
                      Some(Versioned(contractInstanceWithKey.inst.version, changeKey)),
                  )
                ),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed key maintainers" should {
          "fail authentication" in {
            val changeKey = keyWithMaintainers.copy(maintainers = Set.empty)
            val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstanceWithKey,
                metadata = Some(
                  ContractMetadata.tryCreate(
                    signatories = contractInstanceWithKey.metadata.signatories,
                    stakeholders = contractInstanceWithKey.metadata.stakeholders,
                    maybeKeyWithMaintainersVersioned =
                      Some(Versioned(contractInstanceWithKey.inst.version, changeKey)),
                  )
                ),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }
      }
  }

}
