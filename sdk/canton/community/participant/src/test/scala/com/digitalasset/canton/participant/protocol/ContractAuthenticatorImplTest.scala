// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.lfHash
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultPackageName, defaultTemplateId}
import com.digitalasset.canton.{BaseTest, LfPackageName, LfPartyId, protocol}
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.{CreationTime, Versioned}
import com.digitalasset.daml.lf.value.Value
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class ContractAuthenticatorImplTest extends AnyWordSpec with BaseTest {

  private def modifyCreateNode(
      f: LfNodeCreate => LfNodeCreate
  ): LfFatContractInst => LfFatContractInst =
    fci => LfFatContractInst.fromCreateNode(f(fci.toCreateNode), fci.createdAt, fci.cantonData)

  forEvery(Seq(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11)) {
    authContractIdVersion =>
      s"ContractAuthenticatorImpl with $authContractIdVersion" when {
        "using a contract id" should {
          "correctly authenticate the contract" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            contractAuthenticator.authenticate(fatContractInstance) shouldBe Either.unit
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

            val baseInstance = ExampleTransactionFactory.contractInstance()

            "authenticate" in new WithContractAuthenticator(
              authContractIdVersion
            ) {
              lazy override val contractInstance: LfThinContractInst =
                baseInstance.map(_.copy(arg = normalizedArg))
              contractAuthenticator.authenticate(fatContractInstance) shouldBe Either.unit

              private val unNormalizedContract =
                modifyCreateNode(_.copy(arg = unNormalizedArg))(fatContractInstance)

              contractAuthenticator.authenticate(unNormalizedContract) shouldBe Either.unit
            }
          }
        }

        "using a generic contract id" should {

          def genericContractId(discriminator: Int, suffix: Int): LfContractId =
            LfContractId.V1(lfHash(discriminator), Bytes.assertFromString(f"$suffix%04x"))

          "correctly authenticate the contract" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val nonAuthenticatedContractId = genericContractId(1, 2)
            val nonAuthenticatedContractInstance =
              modifyCreateNode(_.copy(coid = nonAuthenticatedContractId))(fatContractInstance)

            contractAuthenticator.authenticate(nonAuthenticatedContractInstance) shouldBe Left(
              "malformed contract id 'ContractId(0000010000000000000000000000000000000000000000000000000000000000000002)'. Suffix 0002 is not a supported contract-id prefix"
            )
          }
        }

        "using a contract with a mismatching salt" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedAuthenticationData =
              ContractAuthenticationDataV1(TestSalt.generateSalt(1337))(contractIdVersion)
            testFailedAuthentication(
              fci =>
                LfFatContractInst
                  .fromCreateNode(
                    fci.toCreateNode,
                    fci.createdAt,
                    changedAuthenticationData.toLfBytes,
                  ),
              testedAuthenticationData = changedAuthenticationData,
            )
          }
        }

        "using a contract with changed ledger time" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedLedgerTime = ledgerTime.add(Duration.ofDays(1L))
            testFailedAuthentication(
              fci =>
                LfFatContractInst.fromCreateNode(
                  fci.toCreateNode,
                  CreationTime.CreatedAt(changedLedgerTime.toLf),
                  fci.cantonData,
                ),
              testedLedgerTime = changedLedgerTime,
            )
          }
        }

        "using a contract with changed contract arguments" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            private val changedContractInstance = ExampleTransactionFactory.contractInstance(
              Seq(ExampleTransactionFactory.suffixedId(1, 2))
            )
            testFailedAuthentication(
              modifyCreateNode(_.copy(arg = changedContractInstance.unversioned.arg)),
              testedContractInstance =
                ExampleTransactionFactory.asSerializableRaw(changedContractInstance),
            )
          }
        }

        "using a contract with changed template-id" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            private val templateId = LfTemplateId.assertFromString("definitely:changed:templateid")
            private val changedContractInstance: LfThinContractInst =
              ExampleTransactionFactory.contractInstance(templateId = templateId)
            private val changedContractKey = contractKey.map(
              _.focus(_.globalKey).modify(gk =>
                LfGlobalKey.assertBuild(templateId, gk.key, gk.packageName)
              )
            )
            testFailedAuthentication(
              modifyCreateNode(
                _.copy(templateId = templateId, keyOpt = Some(changedContractKey.unversioned))
              ),
              testedContractInstance =
                ExampleTransactionFactory.asSerializableRaw(changedContractInstance),
              testedContractKey = Some(changedContractKey),
            )
          }
        }

        "using a contract with changed package-name" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            private val changedContractInstance: LfThinContractInst =
              ExampleTransactionFactory.contractInstance(
                packageName = LfPackageName.assertFromString("definitely-changed-package-name")
              )
            testFailedAuthentication(
              modifyCreateNode(
                _.copy(packageName = changedContractInstance.unversioned.packageName)
              ),
              testedContractInstance =
                ExampleTransactionFactory.asSerializableRaw(changedContractInstance),
            )
          }
        }

        "using a contract that has mismatching signatories" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedSignatories = signatories - alice
            // As observers == stakeholders -- signatories, we also need to ensure that alice is not a stakeholder - or unicum's will be calculated incorrectly
            testFailedAuthentication(
              modifyCreateNode(
                _.focus(_.signatories)
                  .replace(changedSignatories)
                  .focus(_.stakeholders)
                  .modify(_ - alice)
              ),
              testedSignatories = changedSignatories,
            )
          }
        }

        "using a contract that has mismatching observers" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val additionalObserver = LfPartyId.assertFromString("observer::changed")
            val changedObservers = observers incl additionalObserver
            testFailedAuthentication(
              modifyCreateNode(_.focus(_.stakeholders).modify(_ incl additionalObserver)),
              testedObservers = changedObservers,
            )
          }
        }

        "using a contract with a missing contract key" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            testFailedAuthentication(
              modifyCreateNode(_.copy(keyOpt = None)),
              testedContractKey = None,
            )
          }
        }

        "using a contract that has a mismatching contract key hash" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedContractKey = ExampleTransactionFactory.globalKeyWithMaintainers(
              key = LfGlobalKey.assertBuild(
                defaultTemplateId,
                Value.ValueInt64(0),
                defaultPackageName,
              ),
              maintainers = maintainers,
            )
            testFailedAuthentication(
              modifyCreateNode(_.copy(keyOpt = Some(changedContractKey.unversioned))),
              testedContractKey = Some(changedContractKey),
            )
          }
        }

        "using a contract that has mismatching contract key maintainers" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedContractKey =
              ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = Set(alice))
            testFailedAuthentication(
              modifyCreateNode(_.copy(keyOpt = Some(changedContractKey.unversioned))),
              testedContractKey = Some(changedContractKey),
            )
          }
        }

        "using a contract that has missing contract key maintainers" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedContractKey =
              ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = Set.empty)
            testFailedAuthentication(
              modifyCreateNode(_.copy(keyOpt = Some(changedContractKey.unversioned))),
              testedContractKey = Some(changedContractKey),
            )
          }
        }
      }
  }
}

class WithContractAuthenticator(protected val contractIdVersion: CantonContractIdV1Version)
    extends BaseTest {
  protected lazy val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())
  protected lazy val contractAuthenticator = new ContractAuthenticatorImpl(
    unicumGenerator
  )

  protected lazy val contractInstance: LfThinContractInst =
    ExampleTransactionFactory.contractInstance()
  protected lazy val ledgerTime: CantonTimestamp = CantonTimestamp.MinValue
  protected lazy val alice: IdString.Party = LfPartyId.assertFromString("alice")
  protected lazy val signatories: Set[LfPartyId] =
    Set(ExampleTransactionFactory.signatory, alice)
  protected lazy val observers: Set[LfPartyId] = Set(ExampleTransactionFactory.observer)
  protected lazy val maintainers: Set[LfPartyId] = Set(ExampleTransactionFactory.signatory)
  protected lazy val contractKey: Versioned[LfGlobalKeyWithMaintainers] =
    ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = maintainers)
  protected lazy val contractMetadata: ContractMetadata =
    ContractMetadata.tryCreate(signatories, signatories ++ observers, Some(contractKey))
  protected lazy val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
    psid = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")).toPhysical,
    mediator = MediatorGroupRecipient(MediatorGroupIndex.one),
    transactionUuid = new UUID(1L, 1L),
    viewPosition = ViewPosition(List.empty),
    viewParticipantDataSalt = TestSalt.generateSalt(1),
    createIndex = 0,
    ledgerCreateTime = CreationTime.CreatedAt(ledgerTime.toLf),
    metadata = contractMetadata,
    suffixedContractInstance = contractInstance.unversioned,
    cantonContractIdVersion = contractIdVersion,
  )
  protected lazy val authenticationData =
    ContractAuthenticationDataV1(contractSalt.unwrap)(contractIdVersion)

  protected lazy val contractId = contractIdVersion.fromDiscriminator(
    ExampleTransactionFactory.lfHash(1337),
    unicum,
  )

  lazy val fatContractInstance = LfFatContractInst.fromCreateNode(
    LfNodeCreate(
      coid = contractId,
      contract = contractInstance,
      signatories = contractMetadata.signatories,
      stakeholders = contractMetadata.stakeholders,
      key = contractMetadata.maybeKeyWithMaintainers,
    ),
    CreationTime.CreatedAt(ledgerTime.toLf),
    authenticationData.toLfBytes,
  )

  lazy val contract: SerializableContract =
    SerializableContract(
      contractId = contractId,
      contractInstance = contractInstance,
      metadata = contractMetadata,
      ledgerTime = ledgerTime,
      authenticationData = authenticationData,
    ).valueOrFail("Failed creating serializable contract instance")

  protected def testFailedAuthentication(
      changeContract: LfFatContractInst => LfFatContractInst,
      testedAuthenticationData: ContractAuthenticationData = authenticationData,
      testedLedgerTime: CantonTimestamp = ledgerTime,
      testedContractInstance: SerializableRawContractInstance =
        ExampleTransactionFactory.asSerializableRaw(contractInstance),
      testedSignatories: Set[LfPartyId] = signatories,
      testedObservers: Set[LfPartyId] = observers,
      testedContractKey: Option[Versioned[protocol.LfGlobalKeyWithMaintainers]] = Some(
        contractKey
      ),
  ): Assertion = {
    val salt = testedAuthenticationData match {
      case ContractAuthenticationDataV1(salt) => salt
      case ContractAuthenticationDataV2() =>
        // TODO(#23971) implement this
        fail("ContractAuthenticatorImplTest does not support V2 authentication data")
    }
    val recomputedUnicum = unicumGenerator
      .recomputeUnicum(
        contractSalt = salt,
        ledgerCreateTime = CreationTime.CreatedAt(testedLedgerTime.toLf),
        metadata = ContractMetadata
          .tryCreate(testedSignatories, testedSignatories ++ testedObservers, testedContractKey),
        suffixedContractInstance = testedContractInstance.contractInstance.unversioned,
        cantonContractIdVersion = contractIdVersion,
      )
      .valueOrFail("Failed unicum computation")
    val actualSuffix = unicum.toContractIdSuffix(contractIdVersion)
    val expectedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
    contractAuthenticator.authenticate(changeContract(fatContractInstance)) shouldBe Left(
      s"Mismatching contract id suffixes. Expected: $expectedSuffix vs actual: $actualSuffix"
    )
  }
}
