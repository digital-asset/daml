// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestSalt}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.lfHash
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultPackageName, defaultTemplateId}
import com.digitalasset.canton.{BaseTest, LfPackageName, LfPartyId, protocol}
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class ContractAuthenticatorImplTest extends AnyWordSpec with BaseTest {

  forEvery(Seq(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11)) {
    authContractIdVersion =>
      s"ContractAuthenticatorImpl with $authContractIdVersion" when {
        "using a contract id" should {
          "correctly authenticate the contract" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            contractAuthenticator.authenticateSerializable(contract) shouldBe Either.unit
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
              contractAuthenticator.authenticateSerializable(contract) shouldBe Either.unit

              private val unNormalizedContract = SerializableContract(
                contractId =
                  contractId, // Here we are using the same contract id authenticated above
                contractInstance = baseInstance.map(_.copy(arg = unNormalizedArg)),
                metadata = contractMetadata,
                ledgerTime = ledgerTime,
                contractSalt = contractSalt.unwrap,
              ).valueOrFail("Failed creating serializable contract instance")

              contractAuthenticator.authenticateSerializable(
                unNormalizedContract
              ) shouldBe Either.unit

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
            contractAuthenticator.authenticateSerializable(
              contract.copy(contractId = nonAuthenticatedContractId)
            ) shouldBe Left(
              "Unsupported contract authentication id scheme: Suffix 0002 is not a supported contract-id prefix"
            )
          }
        }

        "using a contract with a mismatching salt" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedSalt = TestSalt.generateSalt(1337)
            testFailedAuthentication(
              _.copy(contractSalt = changedSalt),
              testedSalt = changedSalt,
            )
          }
        }

        "using a contract with changed ledger time" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedLedgerTime = ledgerTime.add(Duration.ofDays(1L))
            testFailedAuthentication(
              _.copy(ledgerCreateTime = LedgerCreateTime(changedLedgerTime)),
              testedLedgerTime = changedLedgerTime,
            )
          }
        }

        "using a contract with changed contract arguments" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedContractInstance = ExampleTransactionFactory.contractInstance(
              Seq(ExampleTransactionFactory.suffixedId(1, 2))
            )
            testFailedAuthentication(
              _.copy(rawContractInstance =
                ExampleTransactionFactory.asSerializableRaw(changedContractInstance)
              ),
              testedContractInstance =
                ExampleTransactionFactory.asSerializableRaw(changedContractInstance),
            )
          }
        }

        "using a contract with changed template-id" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            private val changedRawContractInstance: SerializableRawContractInstance =
              ExampleTransactionFactory.asSerializableRaw(
                ExampleTransactionFactory.contractInstance(
                  templateId = LfTemplateId.assertFromString("definitely:changed:templateid")
                )
              )
            testFailedAuthentication(
              _.copy(rawContractInstance = changedRawContractInstance),
              testedContractInstance = changedRawContractInstance,
            )
          }
        }

        "using a contract with changed package-name" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            private val changedRawContractInstance: SerializableRawContractInstance =
              ExampleTransactionFactory.asSerializableRaw(
                ExampleTransactionFactory.contractInstance(
                  packageName = LfPackageName.assertFromString("definitely-changed-package-name")
                )
              )
            testFailedAuthentication(
              _.copy(rawContractInstance = changedRawContractInstance),
              testedContractInstance = changedRawContractInstance,
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
              contract =>
                contract.copy(metadata =
                  ContractMetadata.tryCreate(
                    changedSignatories,
                    contract.metadata.stakeholders - alice,
                    contract.metadata.maybeKeyWithMaintainersVersioned,
                  )
                ),
              testedSignatories = changedSignatories,
            )
          }
        }

        "using a contract that has mismatching observers" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            val changedObservers =
              observers ++ Set(LfPartyId.assertFromString("observer::changed"))
            testFailedAuthentication(
              contract =>
                contract.copy(metadata =
                  ContractMetadata.tryCreate(
                    contract.metadata.signatories,
                    contract.metadata.stakeholders ++ changedObservers,
                    contract.metadata.maybeKeyWithMaintainersVersioned,
                  )
                ),
              testedObservers = changedObservers,
            )
          }
        }

        "using a contract with a missing contract key" should {
          "fail authentication" in new WithContractAuthenticator(
            authContractIdVersion
          ) {
            testFailedAuthentication(
              contract =>
                contract.copy(metadata =
                  ContractMetadata
                    .tryCreate(
                      contract.metadata.signatories,
                      contract.metadata.stakeholders,
                      None,
                    )
                ),
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
              contract =>
                contract.copy(metadata =
                  ContractMetadata.tryCreate(
                    contract.metadata.signatories,
                    contract.metadata.stakeholders,
                    Some(changedContractKey),
                  )
                ),
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
              contract =>
                contract.copy(metadata =
                  ContractMetadata.tryCreate(
                    contract.metadata.signatories,
                    contract.metadata.stakeholders,
                    Some(changedContractKey),
                  )
                ),
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
              contract =>
                contract.copy(metadata =
                  ContractMetadata.tryCreate(
                    contract.metadata.signatories,
                    contract.metadata.stakeholders,
                    Some(changedContractKey),
                  )
                ),
              testedContractKey = Some(changedContractKey),
            )
          }
        }
      }
  }
}

class WithContractAuthenticator(contractIdVersion: CantonContractIdVersion) extends BaseTest {
  protected lazy val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())
  protected lazy val contractAuthenticator = new ContractAuthenticatorImpl(
    unicumGenerator
  )

  protected lazy val contractInstance: LfThinContractInst =
    ExampleTransactionFactory.contractInstance()
  protected lazy val ledgerTime: CantonTimestamp = CantonTimestamp.MinValue
  protected lazy val alice: IdString.Party = LfPartyId.assertFromString("alice")
  protected lazy val signatories: Set[IdString.Party] =
    Set(ExampleTransactionFactory.signatory, alice)
  protected lazy val observers: Set[LfPartyId] = Set(ExampleTransactionFactory.observer)
  protected lazy val maintainers: Set[LfPartyId] = Set(ExampleTransactionFactory.signatory)
  protected lazy val contractKey: Versioned[LfGlobalKeyWithMaintainers] =
    ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = maintainers)
  protected lazy val contractMetadata: ContractMetadata =
    ContractMetadata.tryCreate(signatories, signatories ++ observers, Some(contractKey))
  protected lazy val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
    synchronizerId = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")),
    mediator = MediatorGroupRecipient(MediatorGroupIndex.one),
    transactionUuid = new UUID(1L, 1L),
    viewPosition = ViewPosition(List.empty),
    viewParticipantDataSalt = TestSalt.generateSalt(1),
    createIndex = 0,
    ledgerCreateTime = LedgerCreateTime(ledgerTime),
    metadata = contractMetadata,
    suffixedContractInstance = ExampleTransactionFactory.asSerializableRaw(contractInstance),
    cantonContractIdVersion = contractIdVersion,
  )

  protected lazy val contractId = contractIdVersion.fromDiscriminator(
    ExampleTransactionFactory.lfHash(1337),
    unicum,
  )

  lazy val contract: SerializableContract =
    SerializableContract(
      contractId = contractId,
      contractInstance = contractInstance,
      metadata = contractMetadata,
      ledgerTime = ledgerTime,
      contractSalt = contractSalt.unwrap,
    ).valueOrFail("Failed creating serializable contract instance")

  protected def testFailedAuthentication(
      changeContract: SerializableContract => SerializableContract,
      testedSalt: Salt = contractSalt.unwrap,
      testedLedgerTime: CantonTimestamp = ledgerTime,
      testedContractInstance: SerializableRawContractInstance =
        ExampleTransactionFactory.asSerializableRaw(contractInstance),
      testedSignatories: Set[LfPartyId] = signatories,
      testedObservers: Set[LfPartyId] = observers,
      testedContractKey: Option[Versioned[protocol.LfGlobalKeyWithMaintainers]] = Some(
        contractKey
      ),
  ): Assertion = {
    val recomputedUnicum = unicumGenerator
      .recomputeUnicum(
        contractSalt = testedSalt,
        ledgerCreateTime = LedgerCreateTime(testedLedgerTime),
        metadata = ContractMetadata
          .tryCreate(testedSignatories, testedSignatories ++ testedObservers, testedContractKey),
        suffixedContractInstance = testedContractInstance,
        cantonContractIdVersion = contractIdVersion,
      )
      .valueOrFail("Failed unicum computation")
    val actualSuffix = unicum.toContractIdSuffix(contractIdVersion)
    val expectedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
    contractAuthenticator.authenticateSerializable(changeContract(contract)) shouldBe Left(
      s"Mismatching contract id suffixes. Expected: $expectedSuffix vs actual: $actualSuffix"
    )
  }
}
