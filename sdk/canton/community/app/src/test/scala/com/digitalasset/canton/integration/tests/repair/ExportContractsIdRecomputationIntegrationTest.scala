// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import cats.implicits.*
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.daml.ledger.api.v2.state_service.ActiveContract.*
import com.daml.ledger.javaapi.data.{Command, CreatedEvent, ExercisedEvent, TreeEvent}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.java.refs.Refs
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule.{Level, forLogger}
import com.digitalasset.canton.participant.admin.data.{ActiveContract, ContractIdImportMode}
import com.digitalasset.canton.participant.admin.repair.ContractIdsImportProcessor
import com.digitalasset.canton.protocol.{LfContractId, LfContractInst, LfHash}
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ForceFlags, PartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction
import com.digitalasset.daml.lf.transaction.{FatContractInstance, TransactionCoder}
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.Assertion
import org.slf4j.event.Level.WARN

import scala.jdk.CollectionConverters.*

sealed trait ExportContractsIdRecomputationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)
      participants.all.dars.upload(CantonTestsPath)
    }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  /** Create contracts `participant` for the given `party` -- these will be the contracts exported
    * as part of the test setup.
    */
  protected def setup(participant: LocalParticipantReference, party: PartyId): Seq[Refs.Contract]

  protected def transformActiveContracts(
      contractIdTransformation: LfContractId => LfContractId,
      contracts: List[LapiActiveContract],
  ): List[LapiActiveContract] =
    contracts.map { contract =>
      val event = contract.getCreatedEvent
      val updatedActiveContract = contract.update(
        _.createdEvent.contractId.set(
          contractIdTransformation(LfContractId.assertFromString(event.contractId)).coid
        )
      )

      val fatContract = TransactionCoder
        .decodeFatContractInstance(event.createdEventBlob)
        .leftMap(decodeError =>
          s"Unable to decode contract event payload: ${decodeError.errorMessage}"
        )
        .value

      val updatedCreateNode = fatContract.toCreateNode.mapCid(contractIdTransformation)
      val updatedFatContract = FatContractInstance.fromCreateNode(
        updatedCreateNode.copy(coid = contractIdTransformation(updatedCreateNode.coid)),
        fatContract.createdAt,
        fatContract.cantonData,
      )

      updatedActiveContract.update(
        _.createdEvent.createdEventBlob.set(
          TransactionCoder.encodeFatContractInstance(updatedFatContract).value
        )
      )
    }

  /** If overridden, rearrange the given contracts before exporting them. This is useful to test
    * that the result is not bound to a specific way in which the contracts are ordered as part of
    * the export (e.g. causing deadlocks if a large number of contracts appear before their
    * dependencies).
    */
  protected def rearrange(contracts: List[LapiActiveContract]): List[LapiActiveContract] = contracts

  /** For every combination of ways to make an export and rearrange its contents:
    *   1. enable a fresh party on `participant1` 2. create a few contracts with the given `setup`
    *      method 3. apply the `break` functions to the export, as well as the `rearrange` method 4.
    *      apply the `f` function to the export, passing its size and the owning party
    */
  protected def withExport(break: List[LapiActiveContract] => List[LapiActiveContract] = identity)(
      f: (File, Long, PartyId) => Assertion
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*

    WithEnabledParties(participant1 -> Seq("alice")) { case Seq(alice) =>
      val contracts = setup(participant1, alice)
      assertDeepFetch(participant1, alice, contracts.map(_.id))

      // Remove the party from participant1 so that its ACS commitment
      // cannot mismatch if the ACS is successfully exported with
      // recomputed contract IDs
      participant1.topology.party_to_participant_mappings.propose_delta(
        party = alice,
        removes = List(participant1.id),
        store = daId,
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
      )

      // Prepare participant2 to be able to submit commands as the migrated
      // party in case the ACS is exported successfully
      for (participant <- Seq(participant1, participant2)) {
        participant.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          adds = List(participant2.id -> ParticipantPermission.Submission),
          store = daId,
        )
      }

      utils.retry_until_true(
        participant1.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = alice.filterString,
            filterParticipant = participant2.filterString,
          )
          .nonEmpty
      )

      val aliceAddedOnP2Offset = eventually() {
        participant1.parties
          .find_party_max_activation_offset(
            partyId = alice,
            participantId = participant2.id,
            synchronizerId = daId,
          )
      }

      val result = File.temporaryFile(suffix = ".gz").map { brokenExportFile =>
        val source =
          participant1.underlying.value.sync.internalStateService.value.activeContracts(
            Set(alice.toLf),
            Offset.fromLong(aliceAddedOnP2Offset.unwrap).toOption,
          )
        val cleanExport =
          source.runWith(Sink.seq).futureValue.map(resp => resp.getActiveContract).toList
        val brokenExportStream = brokenExportFile.newGzipOutputStream()
        val brokenExport = break(rearrange(cleanExport))
        for (contract <- brokenExport) {
          ActiveContract
            .tryCreate(contract)
            .writeDelimitedTo(brokenExportStream)
            .valueOrFail("Failed to write contract to broken export")
        }
        brokenExportStream.close()
        f(brokenExportFile, brokenExport.size.toLong, alice)
      }

      result.get()
    }

  }

  // Execute `f` while `p` is disconnected from `d` -- ensure that `p` is ultimately
  // reconnected to `d` to avoid cascading failures across test cases
  protected def whileDisconnected[A](p: LocalParticipantReference, d: SynchronizerAlias)(
      f: => A
  ): A =
    try {
      p.synchronizers.disconnect(d)
      f
    } finally {
      p.synchronizers.reconnect(d)
    }

  // Produce the command to create a contract referencing the given `refs`
  protected def createCommand(party: PartyId, refs: Refs.ContractId*): Command =
    Refs.create(party.toProtoPrimitive, java.util.List.of(refs*)).commands.loneElement

  // Issues a create command for `n` contracts, each referencing the given `refs`
  protected def create(
      participant: LocalParticipantReference,
      party: PartyId,
      n: Int,
      refs: Refs.ContractId*
  ): Seq[Refs.Contract] =
    participant.ledger_api.javaapi.commands
      .submit_flat(Seq(party), Seq.fill(n)(createCommand(party, refs*)))
      .getEvents
      .asScala
      .map {
        case event: CreatedEvent => Refs.Contract.fromCreatedEvent(event)
        case _ => fail("unexpected archive")
      }
      .toSeq

  protected def archive(
      participant: LocalParticipantReference,
      party: PartyId,
      ref: Refs.Contract,
  ): Unit =
    participant.ledger_api.javaapi.commands
      .submit_flat(Seq(party), ref.id.exerciseArchive().commands.asScala.toSeq)

  private def extractNestedRefs(e: TreeEvent): Seq[Refs.ContractId] =
    e.toProtoTreeEvent.getExercised.getExerciseResult.getList.getElementsList
      .iterator()
      .asScala
      .map(e => new Refs.ContractId(e.getContractId))
      .toSeq

  /** Assert that the given contracts can be fetched, as well as their dependencies, recursively. */
  @annotation.tailrec
  final protected def assertDeepFetch(
      participant: LocalParticipantReference,
      party: PartyId,
      refs: Seq[Refs.ContractId],
  ): Assertion =
    if (refs.isEmpty) succeed
    else {
      val fetches = refs.flatMap(_.exerciseRefs_Fetch().commands().asScala)
      val results = participant.ledger_api.javaapi.commands
        .submit(Seq(party), fetches)
        .getEventsById
        .asScala
        .valuesIterator
        .toSeq
      results should have size refs.length.toLong
      all(results) should matchPattern { case _: ExercisedEvent => }
      assertDeepFetch(participant, party, results.flatMap(extractNestedRefs))
    }

  protected def assertSuccessfulImport(
      participant: LocalParticipantReference,
      party: PartyId,
      exportSize: Long,
  ): Assertion =
    eventually() {
      val acs = participant.ledger_api.state.acs.of_party(party)
      acs.map(_.contractId.takeRight(7)) should have size exportSize
      assertDeepFetch(participant, party, acs.map(_.contractId).map(new Refs.ContractId(_)))
    }

  // Zero out the suffixes of the contract IDs in the given export and shuffle the export itself to
  // make sure that the order in which contracts appear don't cause a deadlock
  protected def zeroOutSuffixes(
      contracts: List[LapiActiveContract]
  ): List[LapiActiveContract] = transformActiveContracts(zeroOutSuffix, contracts)

  private def zeroOutSuffix(contractId: LfContractId): LfContractId = {
    val LfContractId.V1(discriminator, suffix) = contractId
    val brokenSuffix = Bytes.fromByteArray(Array.ofDim[Byte](suffix.length))
    LfContractId.V1.assertBuild(discriminator, brokenSuffix)
  }

}

object ExportContractsIdRecomputationIntegrationTest {

  sealed trait SharedTestCases { this: ExportContractsIdRecomputationIntegrationTest =>

    "Importing the ACS" should {

      "ensure that the contract IDs are valid" in { implicit env =>
        import env.*

        withClue("fail on broken contract IDs by default") {
          withExport(break = zeroOutSuffixes) { (brokenExportFile, _, alice) =>
            whileDisconnected(participant2, daName) {
              loggerFactory.assertThrowsAndLogs[CommandFailure](
                participant2.repair.import_acs(brokenExportFile.canonicalPath),
                _.errorMessage should include("malformed contract id"),
              )
            }
            participant2.ledger_api.state.acs.of_party(alice) should have size 0
          }
        }

        withClue("fix broken contract IDs if `allowContractIdSuffixRecomputation = true`") {
          withExport(break = zeroOutSuffixes) { (brokenExportFile, exportSize, alice) =>
            whileDisconnected(participant2, daName) {
              val remapping =
                participant2.repair.import_acs(
                  brokenExportFile.canonicalPath,
                  contractIdImportMode = ContractIdImportMode.Recomputation,
                )
              remapping should have size exportSize
              forAll(remapping) { case (oldCid, newCid) =>
                oldCid should not be newCid
              }
            }
            assertSuccessfulImport(participant2, alice, exportSize)
          }
        }
      }

      "return an empty map when asked to recompute the contract IDs for a sane export" in {
        implicit env =>
          import env.*

          withExport() { (exportFile, exportSize, alice) =>
            whileDisconnected(participant2, daName) {
              val remapping =
                participant2.repair.import_acs(
                  exportFile.canonicalPath,
                  contractIdImportMode = ContractIdImportMode.Recomputation,
                )
              remapping shouldBe empty
            }
            assertSuccessfulImport(participant2, alice, exportSize)
          }

      }

    }

  }

  sealed trait Star extends ExportContractsIdRecomputationIntegrationTest {

    // Create a star of contracts with 100 leaves, all referencing the center
    override protected def setup(
        participant: LocalParticipantReference,
        party: PartyId,
    ): Seq[Refs.Contract] = {
      val center = create(participant, party, 1)
      val leaves = create(participant, party, 100, center.head.id)
      center ++ leaves
    }
  }

  sealed trait Mesh extends ExportContractsIdRecomputationIntegrationTest {

    // Create an 10^2 mesh of contracts with 10 layers of references and
    // every layer referencing every contract in the previous layer up
    // to the leaves
    override protected def setup(
        participant: LocalParticipantReference,
        party: PartyId,
    ): Seq[Refs.Contract] = {
      val n = 10
      Iterator
        .unfold(create(participant, party, n)) { prev =>
          Some((prev, create(participant, party, n, prev.map(_.id)*)))
        }
        .take(n - 1)
        .flatten
        .toSeq
    }
  }

  sealed trait Reverse extends ExportContractsIdRecomputationIntegrationTest {
    override protected def rearrange(
        contracts: List[LapiActiveContract]
    ): List[LapiActiveContract] =
      contracts.reverse
  }

  sealed trait Shuffle extends ExportContractsIdRecomputationIntegrationTest {
    override protected def rearrange(
        contracts: List[LapiActiveContract]
    ): List[LapiActiveContract] =
      util.Random.shuffle(contracts)
  }

}

class ExportContractsIdRecomputationArchivedDependencyIntegrationTest
    extends ExportContractsIdRecomputationIntegrationTest {

  // Create one contract which refers to an archived one
  override protected def setup(
      participant: LocalParticipantReference,
      party: PartyId,
  ): Seq[Refs.Contract] = {
    val toBeArchived = create(participant, party, 1)
    val refersToArchivedContract = create(participant, party, 1, toBeArchived.head.id)
    toBeArchived.foreach(archive(participant, party, _))
    refersToArchivedContract
  }

  private def toContractInstance(contract: LapiActiveContract): LfContractInst = {
    val res = for {
      event <- Either.fromOption(
        contract.createdEvent,
        "Create node in ActiveContract should not be empty",
      )

      fatContract <- TransactionCoder
        .decodeFatContractInstance(event.createdEventBlob)
        .leftMap(decodeError =>
          s"Unable to decode contract event payload: ${decodeError.errorMessage}"
        )

    } yield LfContractInst(
      fatContract.packageName,
      fatContract.templateId,
      transaction.Versioned(fatContract.version, fatContract.createArg),
    )
    res.value
  }

  // Remove all contracts with no dependencies from the given export
  private def removeLeaves(contracts: List[LapiActiveContract]): List[LapiActiveContract] =
    contracts.filterNot(c => toContractInstance(c).unversioned.cids.isEmpty)

  "Importing the ACS where a dependency has been archived" should {

    "successfully import but issue a warning" in { implicit env =>
      import env.*

      withExport(break = removeLeaves andThen zeroOutSuffixes) {
        (brokenExportFile, exportSize, alice) =>
          whileDisconnected(participant2, daName) {
            loggerFactory.assertLogs(forLogger[ContractIdsImportProcessor] && Level(WARN))(
              participant2.repair.import_acs(
                brokenExportFile.canonicalPath,
                contractIdImportMode = ContractIdImportMode.Recomputation,
              ),
              _.message should include regex "Missing dependency with contract ID '.+'. The contract might have been archived. Its contract ID cannot be recomputed.",
            )
          }
          assertSuccessfulImport(participant2, alice, exportSize)
      }

    }
  }
}

class ExportContractsIdRecomputationDuplicateDiscriminatorIntegrationTest
    extends ExportContractsIdRecomputationIntegrationTest {

  override protected def setup(
      participant: LocalParticipantReference,
      party: PartyId,
  ): Seq[Refs.Contract] = create(participant, party, 2)

  private def zeroOutDiscriminators(
      contracts: List[LapiActiveContract]
  ): List[LapiActiveContract] =
    transformActiveContracts(zeroOutDiscriminators, contracts)

  private def zeroOutDiscriminators(contractId: LfContractId): LfContractId = {
    val LfContractId.V1(discriminator, suffix) = contractId
    val zeroes = Array.ofDim[Byte](discriminator.bytes.length)
    val brokenDiscriminator = LfHash.assertFromByteArray(zeroes)
    LfContractId.V1.assertBuild(brokenDiscriminator, suffix)
  }

  "Importing the ACS with a duplicate discriminator" should {

    "fail" in { implicit env =>
      import env.*

      withExport(break = zeroOutDiscriminators) { (brokenExportFile, _, alice) =>
        whileDisconnected(participant2, daName) {
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant2.repair.import_acs(
              brokenExportFile.canonicalPath,
              contractIdImportMode = ContractIdImportMode.Recomputation,
            ),
            _.errorMessage should include regex "Duplicate discriminator '0+' is used by 2 contract IDs, including",
          )
        }
        participant2.ledger_api.state.acs.of_party(alice) should have size 0
      }

    }
  }
}

class ExportContractsIdRecomputationMeshIntegrationTest
    extends ExportContractsIdRecomputationIntegrationTest.Mesh
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases

class ExportContractsIdRecomputationMeshReverseIntegrationTest
    extends ExportContractsIdRecomputationMeshIntegrationTest
    with ExportContractsIdRecomputationIntegrationTest.Reverse
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases

class ExportContractsIdRecomputationMeshShuffleIntegrationTest
    extends ExportContractsIdRecomputationMeshIntegrationTest
    with ExportContractsIdRecomputationIntegrationTest.Shuffle
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases

class ExportContractsIdRecomputationStarIntegrationTest
    extends ExportContractsIdRecomputationIntegrationTest.Star
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases

class ExportContractsIdRecomputationStarReverseIntegrationTest
    extends ExportContractsIdRecomputationStarIntegrationTest
    with ExportContractsIdRecomputationIntegrationTest.Reverse
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases

class ExportContractsIdRecomputationStarShuffleIntegrationTest
    extends ExportContractsIdRecomputationStarIntegrationTest
    with ExportContractsIdRecomputationIntegrationTest.Shuffle
    with ExportContractsIdRecomputationIntegrationTest.SharedTestCases
