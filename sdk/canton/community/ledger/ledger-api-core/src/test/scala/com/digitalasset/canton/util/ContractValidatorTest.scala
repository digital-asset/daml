// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either.*
import com.daml.logging.LoggingContext
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.protocol.{CantonContractIdV1Version, *}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPackageName,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Versioned}
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

  implicit private val loggingContext: LoggingContext = LoggingContext.empty

  private val alice = LfPartyId.assertFromString("Alice")

  private val pureCrypto = new SymbolicPureCrypto()

  forEvery(CantonContractIdVersion.allV1) { authContractIdVersion =>
    //  val authContractIdVersion: Canton
    val testEngine =
      new TestEngine(
        packagePaths = Seq(CantonExamplesPath),
        iterationsBetweenInterruptions = 10,
        cantonContractIdVersion = authContractIdVersion,
      )

    val underTest =
      ContractValidator(pureCrypto, testEngine.engine, testEngine.packageResolver)

    def assertAuthenticationError(invalid: FatContractInstance): Future[Assertion] =
      assertErrorRegex(invalid, s"AuthenticationFailed.*${invalid.contractId.coid}")

    def assertTypeMismatch(invalid: FatContractInstance): Future[Assertion] =
      assertErrorRegex(invalid, s"TranslationFailed.*TypeMismatch")

    def assertValidationFailure(invalid: FatContractInstance): Future[Assertion] =
      assertErrorRegex(invalid, s"ValidationFailed.*${invalid.contractId.coid}.*")

    def assertTranslationFailure(invalid: FatContractInstance): Future[Assertion] =
      if (authContractIdVersion == AuthenticatedContractIdVersionV10)
        _assertErrorRegex(invalid, s"AuthenticationFailed.*${invalid.contractId.coid}")
      else
        _assertErrorRegex(invalid, s"TranslationFailed.*${invalid.contractId.coid}.*")

    def assertErrorRegex(
        invalid: FatContractInstance,
        v12errorRegex: String,
    ): Future[Assertion] = {
      val errorRegex =
        if (authContractIdVersion == AuthenticatedContractIdVersionV12) v12errorRegex
        else s"AuthenticationFailed.*${invalid.contractId.coid}"

      _assertErrorRegex(invalid, errorRegex)
    }

    def _assertErrorRegex(
        invalid: FatContractInstance,
        errorRegex: String,
    ): Future[Assertion] =
      underTest
        .authenticate(invalid, invalid.templateId.packageId)
        .value
        .map(e =>
          inside(e) { case Left(error) =>
            error should include regex errorRegex
          }
        )

    s"ContractAuthenticatorImpl with $authContractIdVersion" when {

      val (createTx, _) =
        testEngine.submitAndConsume(new Cycle("id", alice).create().commands.loneElement, alice)
      val createNode = createTx.nodes.values.collect { case c: LfNodeCreate => c }.loneElement
      val contractInstance = ContractInstance.create(testEngine.suffix(createNode)).value
      val targetPackageId = contractInstance.templateId.packageId

      "using a valid contract id" should {
        "correctly authenticate the contract" in {
          underTest
            .authenticate(contractInstance.inst, targetPackageId)
            .value
            .map(_ shouldBe Either.unit)
        }
      }

      "using values with unexpected trailing none fields" should {
        val unNormalizedArg = Value.ValueRecord(
          None,
          contractInstance.inst.createArg
            .asInstanceOf[Value.ValueRecord]
            .fields
            .slowAppend(ImmArray.from(Seq((None, Value.ValueOptional(None))))),
        )

        val unNormalizedContract = ExampleContractFactory
          .modify[CreationTime.CreatedAt](contractInstance, arg = Some(unNormalizedArg))

        "fail to authenticate" in {
          assertTranslationFailure(unNormalizedContract.inst)
        }
      }

      "using an invalid contract id" should {
        "fail authentication" in {
          val invalidContractId = ExampleContractFactory.buildContractId()
          val invalid: FatContractInstance = ExampleContractFactory
            .modify[CreatedAt](contractInstance, contractId = Some(invalidContractId))
            .inst
          assertAuthenticationError(invalid)
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
          assertAuthenticationError(invalid)
        }
      }

      "using a changed ledger time" should {
        "fail authentication" in {
          val changedTime =
            CreatedAt(contractInstance.inst.createdAt.time.add(Duration.ofDays(1L)))
          val invalid: FatContractInstance = ExampleContractFactory
            .modify[CreatedAt](contractInstance, createdAt = Some(changedTime))
            .inst
          assertAuthenticationError(invalid)
        }
      }

      "using a changed contract argument" should {
        "fail authentication" in {
          val invalid: FatContractInstance = ExampleContractFactory
            .modify[CreatedAt](contractInstance, arg = Some(ValueText("changed")))
            .inst
          assertTypeMismatch(invalid)
        }
      }

      "using a changed template-id" should {
        import com.digitalasset.canton.examples.java.iou.Iou
        "fail authentication" in {
          val invalid: FatContractInstance = ExampleContractFactory
            .modify[CreatedAt](
              contractInstance,
              templateId = Some(testEngine.toRefIdentifier(Iou.TEMPLATE_ID_WITH_PACKAGE_ID)),
            )
            .inst
          assertTypeMismatch(invalid)
        }
      }

      "using a changed package-name" should {
        "fail authentication" in {
          val expected = "definitely-changed-package-name"
          val invalid: FatContractInstance = ExampleContractFactory
            .modify[CreatedAt](
              contractInstance,
              packageName = Some(LfPackageName.assertFromString(expected)),
            )
            .inst
          assertErrorRegex(
            invalid,
            s"ValidationFailed.*${invalid.contractId.coid}.*package name mismatch.*$expected",
          )
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
          assertValidationFailure(invalid)
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
          assertValidationFailure(invalid)
        }
      }

    }

    // TODO(i16065): Re-enable contract key tests
    val keyEnabledContractIdVersions = Seq.empty[CantonContractIdV1Version]

    forEvery(keyEnabledContractIdVersions) { authContractIdVersion =>
      s"Contract key validations" when {

        val keyWithMaintainers = ExampleContractFactory.buildKeyWithMaintainers()
        val contractInstanceWithKey = ExampleContractFactory.build[CreatedAt](
          cantonContractIdVersion = authContractIdVersion,
          keyOpt = Some(keyWithMaintainers),
        )

        "using a changed key value" should {
          "fail authentication" in {
            val changeKey = keyWithMaintainers.copy(globalKey =
              LfGlobalKey.assertBuild(
                contractInstanceWithKey.templateId,
                ValueText("changed"),
                contractInstanceWithKey.inst.packageName,
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
            assertAuthenticationError(invalid)
          }
        }

        "using a changed key maintainers" should {
          "fail authentication" ignore {
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
            assertAuthenticationError(invalid)
          }
        }
      }
    }
  }
}
