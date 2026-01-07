// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/*package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres, UseH2}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.{
  CreatedContract,
  SerializableContract,
  SerializableRawContractInstance,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  TransactionCoder,
  TransactionOuterClass,
}
import com.digitalasset.daml.lf.value.ValueOuterClass
import monocle.Focus
import monocle.Traversal.fromTraverse

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

 TODO(#20297) Ensure that canton handles unsupported transaction versions correctly.

sealed abstract class TransactionVersionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private var maliciousP1: MaliciousParticipantNode = _
  private var alice: PartyId = _
  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      pureCryptoRef.set(sequencer1.crypto.pureCrypto)

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )

      alice = participant1.parties.enable("alice")

      participant1.dars.upload(CantonTestsPath)
    }

  "An unsupported transaction version" should {

    "Fail deserialization but not prevent further processing" in { implicit env =>
      import env.*
      val pureCrypto = sequencer1.crypto.pureCrypto

      val cmd =
        new UniversalContract(
          Seq(alice.toProtoPrimitive).asJava,
          Seq.empty.asJava,
          Seq.empty.asJava,
          Seq.empty.asJava,
        ).create.commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .map(c => Command.fromJavaProto(c.toProtoCommand))
          .toSeq

      val command = CommandsWithMetadata(
        commands = cmd,
        actAs = Seq(alice),
        disclosures = ImmArray.empty[FatContractInstance],
      )

      val changeTxVersion: GenTransactionTree => GenTransactionTree =
        GenTransactionTree.rootViewsUnsafe
          .andThen(
            MerkleSeq.unblindedElementsUnsafe[TransactionView](pureCrypto, testedProtocolVersion)
          )
          .andThen(fromTraverse[Seq, TransactionView])
          .andThen(TransactionView.viewParticipantDataUnsafe)
          .andThen(MerkleTree.tryUnwrap[ViewParticipantData])
          .andThen(Focus[ViewParticipantData](_.createdCore))
          .andThen(fromTraverse[Seq, CreatedContract])
          .andThen(CreatedContract.contractUnsafe)
          .andThen(Focus[SerializableContract].apply(_.rawContractInstance))
          .modify { c =>
            val tocBuilder = TransactionOuterClass.ThinContractInstance
              .newBuilder(TransactionCoder.encodeContractInstance(c.contractInstance).value)
            val vv = ValueOuterClass.VersionedValue
              .newBuilder(tocBuilder.getArgVersioned)
              .setVersion("3.99")
              .build()
            val toc = tocBuilder.setArgVersioned(vv).build()
            // To add an invalid transaction version we leave the contract instance as
            // but provide a corrupted deserialized form that will be used when
            // serializing without error but will error on deserialization
            SerializableRawContractInstance.createWithSerialization(c.contractInstance)(
              toc.toByteString
            )
          }

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val (_, events) = trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1
              .submitCommand(
                command = command,
                transactionTreeInterceptor = changeTxVersion,
              )
              .futureValueUS
          }
          events.assertNoTransactions()
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                MalformedRejects.Payloads,
                _ shouldBe """Rejected transaction due to malformed payload within views Vector(SymmetricDecryptError(
                                     |  FailedToDeserialize(DefaultDeserializationError(message = Unable to convert field `raw_contract_instance`: ValueConversionError(,DecodeError(Unsupported transaction version '3.99'))))
                                     |))""".stripMargin,
              ),
              "Malformed SymmetricDecryptError Error",
            ),
            (
              _.message shouldBe """Request 0: Decryption error: SymmetricDecryptError(
                                     |  FailedToDeserialize(DefaultDeserializationError(message = Unable to convert field `raw_contract_instance`: ValueConversionError(,DecodeError(Unsupported transaction version '3.99'))))
                                     |)""".stripMargin,
              "Decryption SymmetricDecryptError Error",
            ),
          )
        ),
      )
      participant1.health.ping(participant1)
    }
  }
}

final class ReferenceTransactionVersionIntegrationTestPostgres
    extends TransactionVersionIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
 */
