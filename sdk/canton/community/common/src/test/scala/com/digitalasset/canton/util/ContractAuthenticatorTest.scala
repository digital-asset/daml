// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{BaseTest, LfPackageName, LfPartyId}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ValueText
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class ContractAuthenticatorTest extends AnyWordSpec with BaseTest {

  forEvery(Seq(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11)) {
    authContractIdVersion =>
      s"ContractAuthenticatorImpl with $authContractIdVersion" when {
        "using a valid contract id" should {
          "correctly authenticate the contract" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            contractAuthenticator.legacyAuthenticate(fatContractInstance) shouldBe Either.unit
            contractAuthenticator.authenticate(
              fatContractInstance,
              LegacyContractHash.tryFatContractHash(
                fatContractInstance,
                contractIdVersion.useUpgradeFriendlyHashing,
              ),
            ) shouldBe Either.unit
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

            "correctly authenticate the contract" in new WithContractAuthenticator(
              authContractIdVersion
            ) {
              contractAuthenticator.legacyAuthenticate(
                unNormalizedContract.inst
              ) shouldBe Either.unit
              contractAuthenticator.legacyAuthenticate(normalizedContract.inst) shouldBe Either.unit
            }
          }
        }

        "using an invalid contract id" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val invalidContractId = ExampleContractFactory.buildContractId()
            private val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, contractId = Some(invalidContractId))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using an invalid hash" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            val invalidHash: LfHash = ExampleTransactionFactory.lfHash(7)
            shouldFailAuthentication(fatContractInstance, Some(invalidHash))
          }
        }

        "using a changed salt/authentication data" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {

            val authenticationData =
              ContractAuthenticationDataV1(TestSalt.generateSalt(42))(contractIdVersion).toLfBytes
            private val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, authenticationData = Some(authenticationData))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed ledger time" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val changedTime =
              CreatedAt(contractInstance.inst.createdAt.time.add(Duration.ofDays(1L)))
            private val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, createdAt = Some(changedTime))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed contract argument" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](contractInstance, arg = Some(ValueText("changed")))
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed template-id" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val invalid: FatContractInstance = ExampleContractFactory
              .modify[CreatedAt](
                contractInstance,
                templateId = Some(LfTemplateId.assertFromString("definitely:changed:template")),
              )
              .inst
            shouldFailAuthentication(invalid)
          }
        }

        "using a changed package-name" should {
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val invalid: FatContractInstance = ExampleContractFactory
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
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val changedSignatory: IdString.Party =
              LfPartyId.assertFromString("changed::signatory")
            private val invalid: FatContractInstance = ExampleContractFactory
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
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val changedObserver: IdString.Party =
              LfPartyId.assertFromString("changed::observer")
            private val invalid: FatContractInstance = ExampleContractFactory
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
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val changeKey = keyWithMaintainers.copy(globalKey =
              LfGlobalKey.assertBuild(
                contractInstance.templateId,
                ValueText("changed"),
                contractInstance.inst.packageName,
              )
            )
            private val invalid: FatContractInstance = ExampleContractFactory
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
          "fail authentication" in new WithContractAuthenticator(authContractIdVersion) {
            private val changeKey = keyWithMaintainers.copy(maintainers = Set.empty)
            private val invalid: FatContractInstance = ExampleContractFactory
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

class WithContractAuthenticator(protected val contractIdVersion: CantonContractIdV1Version)
    extends BaseTest {

  def shouldFailAuthentication(
      invalid: FatContractInstance,
      contractHashO: Option[LfHash] = None,
  ): Unit = {
    val contractHash = contractHashO.getOrElse(LegacyContractHash.fatContractHash(invalid).value)

    inside(contractAuthenticator.authenticate(invalid, contractHash)) { case Left(error) =>
      error should startWith("Mismatching contract id suffixes.")
    }
  }

  protected val pureCrypto = new SymbolicPureCrypto()
  protected val contractIdSuffixer = new ContractIdSuffixer(pureCrypto, contractIdVersion)
  protected val unicumGenerator = new UnicumGenerator(pureCrypto)
  protected val contractAuthenticator = new ContractAuthenticatorImpl(unicumGenerator)
  protected val contractInstance =
    ExampleContractFactory.build[CreatedAt](cantonContractIdVersion = contractIdVersion)
  protected val keyWithMaintainers = ExampleContractFactory.buildKeyWithMaintainers()
  protected val contractInstanceWithKey = ExampleContractFactory.build[CreatedAt](
    cantonContractIdVersion = contractIdVersion,
    keyOpt = Some(keyWithMaintainers),
  )
  protected val fatContractInstance: FatContractInstance = contractInstance.inst
}
