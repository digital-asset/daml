// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.{CachingConfigs, LoggingConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.{
  ParticipantAuthorizationError,
  TransactionTreeFactoryError,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractLookupError,
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  MemberRecipient,
  OpenEnvelope,
  Recipient,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.store.{SessionKeyStoreDisabled, SessionKeyStoreWithInMemoryCache}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.*
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

class TransactionConfirmationRequestFactoryTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasExecutorService {

  // Parties
  private val observerParticipant1: ParticipantId = ParticipantId("observerParticipant1")
  private val observerParticipant2: ParticipantId = ParticipantId("observerParticipant2")

  // General dummy parameters
  private val domain: DomainId = DefaultTestIdentities.domainId
  private val mediator: MediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero)
  private val ledgerTime: CantonTimestamp = CantonTimestamp.Epoch
  private val workflowId: Option[WorkflowId] = Some(
    WorkflowId.assertFromString("workflowIdConfirmationRequestFactoryTest")
  )
  private val submitterInfo: SubmitterInfo = DefaultParticipantStateValues.submitterInfo(submitters)
  private val maxSequencingTime: CantonTimestamp = ledgerTime.plusSeconds(10)

  private def createCryptoSnapshot(
      partyToParticipant: Map[ParticipantId, Seq[LfPartyId]],
      permission: ParticipantPermission = Submission,
      keyPurposes: Set[KeyPurpose] = KeyPurpose.All,
      freshKeys: Boolean = false,
  ): DomainSnapshotSyncCryptoApi = {

    val map = partyToParticipant.fmap(parties => parties.map(_ -> permission).toMap)
    TestingTopology()
      .withReversedTopology(map)
      .withDomains(domain)
      .withKeyPurposes(keyPurposes)
      .withFreshKeys(freshKeys)
      .build(loggerFactory)
      .forOwnerAndDomain(submittingParticipant, domain)
      .currentSnapshotApproximation
  }

  val defaultTopology: Map[ParticipantId, Seq[LfPartyId]] = Map(
    submittingParticipant -> Seq(submitter, signatory, signatoryReplica),
    observerParticipant1 -> Seq(observer),
    observerParticipant2 -> Seq(observer),
    extraParticipant -> Seq(extra),
  )

  // Collaborators

  // This is a def (and not a val), as the crypto api has the next symmetric key as internal state
  // Therefore, it would not make sense to reuse an instance. We also force the crypto api to not randomize
  // asymmetric encryption ciphertexts.
  private def newCryptoSnapshot: DomainSnapshotSyncCryptoApi = {
    val cryptoSnapshot = createCryptoSnapshot(defaultTopology)
    cryptoSnapshot.crypto.pureCrypto match {
      case crypto: SymbolicPureCrypto => crypto.setRandomnessFlag(true)
      case _ => ()
    }
    cryptoSnapshot
  }

  private val randomOps: RandomOps = new SymbolicPureCrypto()

  private val transactionUuid: UUID = new UUID(10L, 20L)

  private val seedGenerator: SeedGenerator =
    new SeedGenerator(randomOps) {
      override def generateUuid(): UUID = transactionUuid
    }

  // Device under test
  private def confirmationRequestFactory(
      transactionTreeFactoryResult: Either[TransactionTreeConversionError, GenTransactionTree]
  ): TransactionConfirmationRequestFactory = {

    val transactionTreeFactory: TransactionTreeFactory = new TransactionTreeFactory {
      override def createTransactionTree(
          transaction: WellFormedTransaction[WithoutSuffixes],
          submitterInfo: SubmitterInfo,
          _workflowId: Option[WorkflowId],
          _mediator: MediatorGroupRecipient,
          transactionSeed: SaltSeed,
          transactionUuid: UUID,
          _topologySnapshot: TopologySnapshot,
          _contractOfId: SerializableContractOfId,
          _keyResolver: LfKeyResolver,
          _maxSequencingTime: CantonTimestamp,
          validatePackageVettings: Boolean,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree] = {
        val actAs = submitterInfo.actAs.toSet
        if (actAs != Set(ExampleTransactionFactory.submitter))
          fail(
            s"Wrong submitters ${actAs.mkString(", ")}. Expected ${ExampleTransactionFactory.submitter}"
          )
        if (
          transaction.metadata.ledgerTime != TransactionConfirmationRequestFactoryTest.this.ledgerTime
        )
          fail(
            s"""Wrong ledger time ${transaction.metadata.ledgerTime}.
                  | Expected ${TransactionConfirmationRequestFactoryTest.this.ledgerTime}""".stripMargin
          )
        if (transactionUuid != TransactionConfirmationRequestFactoryTest.this.transactionUuid)
          fail(
            s"Wrong transaction UUID $transactionUuid. Expected ${TransactionConfirmationRequestFactoryTest.this.transactionUuid}"
          )
        transactionTreeFactoryResult.toEitherT
      }

      override def tryReconstruct(
          subaction: WellFormedTransaction[WithoutSuffixes],
          rootPosition: ViewPosition,
          mediator: MediatorGroupRecipient,
          submittingParticipantO: Option[ParticipantId],
          salts: Iterable[Salt],
          transactionUuid: UUID,
          topologySnapshot: TopologySnapshot,
          contractOfId: SerializableContractOfId,
          _rbContext: RollbackContext,
          _keyResolver: LfKeyResolver,
      )(implicit traceContext: TraceContext): EitherT[
        FutureUnlessShutdown,
        TransactionTreeConversionError,
        (TransactionView, WellFormedTransaction[WithSuffixes]),
      ] = ???

      override def saltsFromView(view: TransactionView): Iterable[Salt] = ???
    }

    // we force view requests to be handled sequentially, which makes results deterministic and easier to compare
    // in the end.
    new TransactionConfirmationRequestFactory(
      submittingParticipant,
      LoggingConfig(),
      loggerFactory,
      parallel = false,
    )(
      transactionTreeFactory,
      seedGenerator,
    )(executorService)
  }

  private val contractInstanceOfId: SerializableContractOfId = { (id: LfContractId) =>
    EitherT.leftT(ContractLookupError(id, "Error in test: argument should not be used"))
  }
  // This isn't used because the transaction tree factory is mocked

  // Input factory
  private val transactionFactory: ExampleTransactionFactory =
    new ExampleTransactionFactory()(ledgerTime = ledgerTime)

  // Since the ConfirmationRequestFactory signs the envelopes in parallel,
  // we cannot predict the counter that SymbolicCrypto uses to randomize the signatures.
  // So we simply replace them with a fixed empty signature. When we are using
  // EncryptedViewMessage we order the sequence of randomness encryption on both the actual
  // and expected messages so that they match.
  private def stripSignatureAndOrderMap(
      request: TransactionConfirmationRequest
  ): TransactionConfirmationRequest = {
    val requestNoSignature = request
      .focus(_.viewEnvelopes)
      .modify(
        _.map(
          _.focus(_.protocolMessage.submittingParticipantSignature)
            .modify(_.map(_ => SymbolicCrypto.emptySignature))
        )
      )

    val orderedTvm = requestNoSignature.viewEnvelopes.map(tvm =>
      tvm.protocolMessage match {
        case encViewMessage @ EncryptedViewMessage(_, _, _, _, _, _) =>
          val encryptedRandomnessOrdering: Ordering[AsymmetricEncrypted[SecureRandomness]] =
            Ordering.by(_.encryptedFor.unwrap)
          tvm.copy(protocolMessage =
            encViewMessage
              .copy(sessionKeyRandomness =
                encViewMessage.sessionKeys
                  .sorted(encryptedRandomnessOrdering)
              )
          )
        case _ => tvm
      }
    )

    requestNoSignature
      .focus(_.viewEnvelopes)
      .replace(orderedTvm)
  }

  // Expected output factory
  def expectedConfirmationRequest(
      example: ExampleTransaction,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): TransactionConfirmationRequest = {
    val cryptoPureApi = cryptoSnapshot.pureCrypto
    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme

    /* We create a new crypto api to reset the randomness counter for each test, so that keys generated
     * match those of the actual test.
     */
    val cryptoPureApiForRandomness = new SymbolicPureCrypto()

    val privateKeysetCache: TrieMap[Recipients, SecureRandomness] =
      TrieMap.empty

    val hashToKeyMap = example.transactionViewTreesWithWitnesses.map { case (tree, witnesses) =>
      val ec: ExecutionContext = executorService
      val recipients = witnesses
        .toRecipients(cryptoSnapshot.ipsSnapshot)(ec, traceContext)
        .value
        .futureValueUS
        .value

      // simulates session key cache
      val sessionKeyRandomness = privateKeysetCache.getOrElseUpdate(
        recipients,
        cryptoPureApiForRandomness.generateSecureRandomness(
          computeRandomnessLength(cryptoPureApi)
        ),
      )

      tree.viewHash -> (recipients, sessionKeyRandomness)
    }.toMap

    val expectedTransactionViewMessages = example.transactionViewTreesWithWitnesses.map {
      case (tree, _) =>
        val signature =
          if (tree.isTopLevel) {
            Some(
              Await
                .result(cryptoSnapshot.sign(tree.transactionId.unwrap).value, 10.seconds)
                .failOnShutdown
                .valueOr(err => fail(err.toString))
            )
          } else None

        val (recipients, sessionKeyRandomness) = hashToKeyMap(tree.viewHash)

        val sessionKey = cryptoPureApi
          .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
          .valueOrFail("fail to create symmetric key from randomness")

        val participants = tree.informees
          .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValueUS)
          .flatMap(_.keySet)

        val ltvt = LightTransactionViewTree
          .fromTransactionViewTree(
            tree,
            tree.subviewHashes.map(viewHash => hashToKeyMap(viewHash)._2),
            testedProtocolVersion,
          )
          .valueOrFail("fail to create light transaction view tree")

        val encryptedView = EncryptedView
          .compressed(
            cryptoPureApi,
            sessionKey,
            TransactionViewType,
          )(ltvt)
          .valueOr(err => fail(s"fail to encrypt view tree: $err"))

        val encryptedViewMessage: EncryptedViewMessage[TransactionViewType] = {

          val randomnessMapNE = NonEmpty
            .from(randomnessMap(sessionKeyRandomness, participants, cryptoPureApi).values.toSeq)
            .valueOrFail("session key randomness map is empty")

          EncryptedViewMessage(
            signature,
            tree.viewHash,
            randomnessMapNE,
            encryptedView,
            transactionFactory.domainId,
            SymmetricKeyScheme.Aes128Gcm,
            testedProtocolVersion,
          )
        }

        OpenEnvelope(encryptedViewMessage, recipients)(testedProtocolVersion)
    }

    val signature =
      cryptoSnapshot.sign(example.fullInformeeTree.transactionId.unwrap).failOnShutdown.futureValue

    TransactionConfirmationRequest(
      InformeeMessage(example.fullInformeeTree, signature)(testedProtocolVersion),
      expectedTransactionViewMessages,
      testedProtocolVersion,
    )
  }

  def randomnessMap(
      randomness: SecureRandomness,
      informeeParticipants: Set[ParticipantId],
      cryptoPureApi: CryptoPureApi,
  ): Map[ParticipantId, AsymmetricEncrypted[SecureRandomness]] = {

    val randomnessPairs = for {
      participant <- informeeParticipants
      publicKey = newCryptoSnapshot.ipsSnapshot
        .encryptionKey(participant)
        .futureValueUS
        .getOrElse(fail("The defaultIdentitySnapshot really should have at least one key."))
    } yield participant -> cryptoPureApi
      .encryptWithVersion(randomness, publicKey, testedProtocolVersion)
      .valueOr(err => fail(err.toString))

    randomnessPairs.toMap
  }

  private val singleFetch: transactionFactory.SingleFetch = transactionFactory.SingleFetch()
  private lazy val defaultRecipientGroup = {
    val recipientsTree = NonEmpty(
      Seq,
      RecipientsTree(
        NonEmpty(Set, submittingParticipant, observerParticipant1, observerParticipant2)
          .map(pId => MemberRecipient(pId).asInstanceOf[Recipient]),
        Seq.empty,
      ),
    )
    val recipients = Recipients(recipientsTree)

    RecipientGroup(
      recipients,
      newCryptoSnapshot.pureCrypto.defaultSymmetricKeyScheme,
    )
  }

  "A ConfirmationRequestFactory" when {
    "everything is ok" can {

      forEvery(transactionFactory.standardHappyCases) { example =>
        s"create a transaction confirmation request for: $example" in {

          val factory = confirmationRequestFactory(Right(example.transactionTree))

          factory
            .createConfirmationRequest(
              example.wellFormedUnsuffixedTransaction,
              submitterInfo,
              workflowId,
              example.keyResolver,
              mediator,
              newCryptoSnapshot,
              new SessionKeyStoreWithInMemoryCache(
                CachingConfigs.defaultSessionEncryptionKeyCacheConfig
              ),
              contractInstanceOfId,
              maxSequencingTime,
              testedProtocolVersion,
            )
            .value
            .failOnShutdown
            .map { res =>
              val expected = expectedConfirmationRequest(example, newCryptoSnapshot)
              stripSignatureAndOrderMap(res.value) shouldBe stripSignatureAndOrderMap(expected)
            }(executorService) // parallel executorService to avoid a deadlock
        }
      }

      "use the same session encryption key if view recipients tree is the same" in {
        val multipleRoots = transactionFactory.MultipleRoots
        val factory = confirmationRequestFactory(Right(multipleRoots.transactionTree))
        val store = SessionKeyStoreDisabled

        factory
          .createConfirmationRequest(
            multipleRoots.wellFormedUnsuffixedTransaction,
            submitterInfo,
            workflowId,
            multipleRoots.keyResolver,
            mediator,
            newCryptoSnapshot,
            store,
            contractInstanceOfId,
            maxSequencingTime,
            testedProtocolVersion,
          )
          .failOnShutdown
          .map { tcr =>
            tcr.viewEnvelopes.size shouldBe >(1)
            tcr.viewEnvelopes.map(_.protocolMessage.sessionKeys).distinct.length shouldBe 1

            // cache is disable so session key is not persisted for multiple transactions
            store.convertStore.getSessionKeyInfoIfPresent(defaultRecipientGroup) shouldBe None
          }
      }

      s"use different session key after key is revoked between two requests" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))
        // we use the same store for two requests to simulate what would happen in a real scenario
        val store =
          new SessionKeyStoreWithInMemoryCache(
            CachingConfigs.defaultSessionEncryptionKeyCacheConfig
          )

        def getSessionKeyFromConfirmationRequest(cryptoSnapshot: DomainSnapshotSyncCryptoApi) =
          factory
            .createConfirmationRequest(
              singleFetch.wellFormedUnsuffixedTransaction,
              submitterInfo,
              workflowId,
              singleFetch.keyResolver,
              mediator,
              cryptoSnapshot,
              store,
              contractInstanceOfId,
              maxSequencingTime,
              testedProtocolVersion,
            )
            .failOnShutdown
            .map(_ =>
              store
                .getSessionKeyInfoIfPresent(defaultRecipientGroup)
                .valueOrFail("session key not found")
            )

        for {
          firstSessionKeyInfo <- getSessionKeyFromConfirmationRequest(newCryptoSnapshot)
          secondSessionKeyInfo <- getSessionKeyFromConfirmationRequest(newCryptoSnapshot)
          anotherCryptoSnapshot = createCryptoSnapshot(defaultTopology, freshKeys = true)
          thirdSessionKeyInfo <- getSessionKeyFromConfirmationRequest(anotherCryptoSnapshot)
        } yield {
          firstSessionKeyInfo shouldBe secondSessionKeyInfo
          secondSessionKeyInfo should not be thirdSessionKeyInfo
        }
      }
    }

    "submitter node does not represent submitter" must {

      val emptyCryptoSnapshot = createCryptoSnapshot(Map.empty)

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            submitterInfo,
            workflowId,
            singleFetch.keyResolver,
            mediator,
            emptyCryptoSnapshot,
            new SessionKeyStoreWithInMemoryCache(
              CachingConfigs.defaultSessionEncryptionKeyCacheConfig
            ),
            contractInstanceOfId,
            maxSequencingTime,
            testedProtocolVersion,
          )
          .failOnShutdown
          .value
          .map(
            _ should equal(
              Left(
                ParticipantAuthorizationError(
                  s"$submittingParticipant does not host $submitter or is not active."
                )
              )
            )
          )
      }
    }

    "submitter node is not allowed to submit transactions" must {

      val confirmationOnlyCryptoSnapshot =
        createCryptoSnapshot(defaultTopology, permission = Confirmation)

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            submitterInfo,
            workflowId,
            singleFetch.keyResolver,
            mediator,
            confirmationOnlyCryptoSnapshot,
            new SessionKeyStoreWithInMemoryCache(
              CachingConfigs.defaultSessionEncryptionKeyCacheConfig
            ),
            contractInstanceOfId,
            maxSequencingTime,
            testedProtocolVersion,
          )
          .failOnShutdown
          .value
          .map(
            _ should equal(
              Left(
                ParticipantAuthorizationError(
                  s"$submittingParticipant is not authorized to submit transactions for $submitter."
                )
              )
            )
          )
      }
    }

    "transactionTreeFactory fails" must {
      "be rejected" in {
        val error = ContractLookupError(ExampleTransactionFactory.suffixedId(-1, -1), "foo")
        val factory = confirmationRequestFactory(Left(error))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            submitterInfo,
            workflowId,
            singleFetch.keyResolver,
            mediator,
            newCryptoSnapshot,
            new SessionKeyStoreWithInMemoryCache(
              CachingConfigs.defaultSessionEncryptionKeyCacheConfig
            ),
            contractInstanceOfId,
            maxSequencingTime,
            testedProtocolVersion,
          )
          .failOnShutdown
          .value
          .map(_ should equal(Left(TransactionTreeFactoryError(error))))
      }
    }

    // Note lack of test for ill-authorized transaction as authorization check is performed by LF upon ledger api submission as of Daml 1.6.0

    "informee participant cannot be found" must {

      val submitterOnlyCryptoSnapshot =
        createCryptoSnapshot(Map(submittingParticipant -> Seq(submitter)))

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            submitterInfo,
            workflowId,
            singleFetch.keyResolver,
            mediator,
            submitterOnlyCryptoSnapshot,
            new SessionKeyStoreWithInMemoryCache(
              CachingConfigs.defaultSessionEncryptionKeyCacheConfig
            ),
            contractInstanceOfId,
            maxSequencingTime,
            testedProtocolVersion,
          )
          .failOnShutdown
          .value
          .map(
            _ should equal(
              Left(
                TransactionConfirmationRequestFactory.RecipientsCreationError(
                  s"Found no active participants for informees: ${List(observer)}"
                )
              )
            )
          )
      }
    }

    "participants" when {
      def runNoKeyTest(name: String, availableKeys: Set[KeyPurpose]): Unit =
        name must {
          "be rejected" in {
            val noKeyCryptoSnapshot =
              createCryptoSnapshot(defaultTopology, keyPurposes = availableKeys)
            val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              factory
                .createConfirmationRequest(
                  singleFetch.wellFormedUnsuffixedTransaction,
                  submitterInfo,
                  workflowId,
                  singleFetch.keyResolver,
                  mediator,
                  noKeyCryptoSnapshot,
                  new SessionKeyStoreWithInMemoryCache(
                    CachingConfigs.defaultSessionEncryptionKeyCacheConfig
                  ),
                  contractInstanceOfId,
                  maxSequencingTime,
                  testedProtocolVersion,
                )
                .value
                .failOnShutdown
                .map {
                  case Left(ParticipantAuthorizationError(message)) =>
                    message shouldBe s"$submittingParticipant does not host $submitter or is not active."
                  case otherwise =>
                    fail(
                      s"should have failed with a participant authorization error, but returned result: $otherwise"
                    )
                },
              LogEntry.assertLogSeq(
                Seq.empty,
                mayContain = Seq(
                  _.warningMessage should include(
                    "has a domain trust certificate, but no keys on domain"
                  )
                ),
              ),
            )

          }
        }

      runNoKeyTest("they have no public keys", availableKeys = Set.empty)
      runNoKeyTest("they have no public signing keys", availableKeys = Set(KeyPurpose.Encryption))
      runNoKeyTest("they have no public encryption keys", availableKeys = Set(KeyPurpose.Signing))
    }
  }
}
