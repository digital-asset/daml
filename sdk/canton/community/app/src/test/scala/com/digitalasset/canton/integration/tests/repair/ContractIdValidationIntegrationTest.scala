// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import cats.implicits.*
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.daml.ledger.api.v2.state_service.ActiveContract.*
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ForceFlags, PartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.{FatContractInstance, TransactionCoder}
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.Assertion

final class ContractIdValidationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)
    }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  /** Create contracts `participant` for the given `party` -- these will be the contracts exported
    * as part of the test setup.
    */
  private def setup(participant: LocalParticipantReference, party: PartyId): Seq[Iou.Contract] =
    IouSyntax.createIous(participant, party, party, (1 to 10))

  private def transformActiveContracts(
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
        fatContract.authenticationData,
      )

      updatedActiveContract.update(
        _.createdEvent.createdEventBlob.set(
          TransactionCoder.encodeFatContractInstance(updatedFatContract).value
        )
      )
    }

  /** For every combination of ways to make an export and rearrange its contents:
    *
    *   - enable a fresh party on `participant1`
    *   - create a few contracts with the given `setup` method
    *   - apply the `break` functions to the export, as well as the `rearrange` method
    *   - apply the `f` function to the export, passing its size and the owning party
    */
  private def withExport(break: List[LapiActiveContract] => List[LapiActiveContract])(
      f: (File, Long, PartyId) => Assertion
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*

    WithEnabledParties(participant1 -> Seq("alice")) { case Seq(alice) =>
      setup(participant1, alice.partyId)

      // Remove the party from participant1 so that its ACS commitment
      // cannot mismatch
      participant1.topology.party_to_participant_mappings.propose_delta(
        party = alice.partyId,
        removes = List(participant1.id),
        store = daId,
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
      )

      // Prepare participant2 to be able to submit commands as the migrated
      // party in case the ACS is exported successfully
      for (participant <- Seq(participant1, participant2)) {
        participant.topology.party_to_participant_mappings.propose_delta(
          party = alice.partyId,
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
            partyId = alice.partyId,
            participantId = participant2.id,
            synchronizerId = daId,
          )
      }

      val result = File.temporaryFile(suffix = ".gz").map { brokenExportFile =>
        val source =
          participant1.underlying.value.sync.internalIndexService.value.activeContracts(
            Set(alice.toLf),
            Offset.fromLong(aliceAddedOnP2Offset).toOption,
          )
        val cleanExport =
          source.runWith(Sink.seq).futureValue.map(resp => resp.getActiveContract).toList
        val brokenExportStream = brokenExportFile.newGzipOutputStream()
        val brokenExport = break(cleanExport)
        for (contract <- brokenExport) {
          ActiveContract
            .tryCreate(contract)
            .writeDelimitedTo(brokenExportStream)
            .valueOrFail("Failed to write contract to broken export")
        }
        brokenExportStream.close()
        f(brokenExportFile, brokenExport.size.toLong, alice.partyId)
      }

      result.get()
    }
  }

  // Zero out the suffixes of the contract IDs in the given export and shuffle the export itself to
  // make sure that the order in which contracts appear don't cause a deadlock
  private def zeroOutSuffixes(
      contracts: List[LapiActiveContract]
  ): List[LapiActiveContract] = transformActiveContracts(zeroOutSuffix, contracts)

  private def zeroOutSuffix(contractId: LfContractId): LfContractId = {
    val LfContractId.V1(discriminator, suffix) = contractId match {
      case cid: LfContractId.V1 => cid
      case _ => sys.error("ContractId V2 are not supported")
    }
    val brokenSuffix = Bytes.fromByteArray(Array.ofDim[Byte](suffix.length))
    LfContractId.V1.assertBuild(discriminator, brokenSuffix)
  }

  "Importing the ACS" should {
    "ensure that the contract IDs are valid" in { implicit env =>
      import env.*

      withClue("fail on broken contract IDs by default") {
        withExport(break = zeroOutSuffixes) { (brokenExportFile, _, alice) =>
          participant2.synchronizers.disconnect_all()
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant2.repair.import_acsV2(brokenExportFile.canonicalPath, daId),
            _.errorMessage should include("Malformed contract ID"),
          )

          participant2.synchronizers.reconnect_all()
          participant2.ledger_api.state.acs.of_party(alice) should have size 0
        }
      }
    }
  }
}
