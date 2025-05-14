// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Namespace, PartyId}

import scala.concurrent.Future

/** Test various entangled topology change scenarios
  *
  * One difficulty with topology changes is testing whether the change has had any adverse impact on
  * any other functionality such as a subsequent onboarding of another node.
  *
  * Therefore, we perform here a set of relative harsh topology changes and test whether everything
  * still works as it should.
  */
class TopologyChangeIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .withNetworkBootstrap { implicit env =>
        import env.*
        nodes.local.start()
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_multi(daName, sequencers.all.map(_.sequencerConnection))
      }

  "initial topology state" should {
    "create an initial topology state" in { implicit env =>
      import env.*

      // create some parties
      (1 to 20).foreach { ii =>
        participant1.parties.enable(s"testParty$ii")
      }

    }
    "roll participant and mediator keys" in { implicit env =>
      import env.*
      participant1.keys.secret.rotate_node_keys()
      participant1.health.maybe_ping(participant1) should not be empty
      mediator1.keys.secret.rotate_node_keys()
      participant1.health.maybe_ping(participant1) should not be empty

    }

    "disconnect p1 and then roll sequencer keys" in { implicit env =>
      import env.*
      participant1.synchronizers.disconnect_all()
      sequencer1.keys.secret.rotate_node_keys()
      participant1.synchronizers.reconnect_all()
      participant1.health.maybe_ping(participant1) should not be empty
    }

    "change some synchronizer parameters" in { implicit env =>
      import env.*
      val sid = sequencer1.synchronizer_id
      sequencer1.topology.synchronizer_parameters.propose_update(
        sid,
        _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(31)),
        mustFullyAuthorize = true,
      )
      sequencer1.topology.synchronizer_parameters.propose_update(
        sid,
        _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(32)),
        mustFullyAuthorize = true,
      )
    }
    "remove a few parties again" in { implicit env =>
      import env.*

      // remove some parties
      (1 to 10).foreach { ii =>
        participant1.parties.disable(PartyId.tryCreate(s"testParty$ii", participant1.namespace))
      }
    }
  }

  "create a long certificate chain" should {

    val newNamespaceKey = "new-namespace-key"
    val rootDelegationKey = "root-delegation-key"
    val intermediateDelegationKey = "intermediate-delegation-key"
    def getKey(str: String)(implicit env: TestConsoleEnvironment) =
      env.participant1.keys.secret
        .list(filterName = str)
        .find(_.name.exists(_.unwrap == str))
        .flatMap(_.publicKey.asSigningKey)
        .valueOrFail("must be there")

    "add a namespace for a new set of parties" in { implicit env =>
      import env.*

      val ns = participant1.keys.secret
        .generate_signing_key(newNamespaceKey, SigningKeyUsage.NamespaceOnly)
      val key2 = participant1.keys.secret
        .generate_signing_key(rootDelegationKey, SigningKeyUsage.NamespaceOnly)
      val key3 = participant1.keys.secret
        .generate_signing_key(intermediateDelegationKey, SigningKeyUsage.NamespaceOnly)
      val party = PartyId.tryCreate("indirect", ns.fingerprint)

      participant1.topology.namespace_delegations.propose_delegation(
        Namespace(ns.fingerprint),
        ns,
        CanSignAllMappings,
        synchronize = None,
      )

      participant1.topology.namespace_delegations.propose_delegation(
        Namespace(ns.fingerprint),
        key2,
        CanSignAllMappings,
        signedBy = Seq(ns.fingerprint),
        synchronize = None,
      )

      participant1.topology.namespace_delegations.propose_delegation(
        Namespace(ns.fingerprint),
        key3,
        CanSignAllButNamespaceDelegations,
        signedBy = Seq(key2.fingerprint),
      )

      participant1.topology.party_to_participant_mappings.propose(
        party,
        Seq((participant1.id, ParticipantPermission.Submission)),
        signedBy = Seq(key3.fingerprint, participant1.id.uid.namespace.fingerprint),
        synchronize = None,
      )

      eventually() {
        participant1.parties.list().map(_.party) should contain(party)
      }

      eventually() {
        participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = party.filterString) should not be empty
      }
    }

    "roll certificates" in { implicit env =>
      import env.*

      val ns = getKey(newNamespaceKey)
      val ns2 = getKey(rootDelegationKey)
      val ns3 = getKey(intermediateDelegationKey)

      val newNS2Key = participant1.keys.secret
        .generate_signing_key("2" + rootDelegationKey, SigningKeyUsage.NamespaceOnly)
      val newNS3Key = participant1.keys.secret.generate_signing_key(
        "2" + intermediateDelegationKey,
        SigningKeyUsage.NamespaceOnly,
      )

      // add new chains
      Seq(
        (newNS2Key, ns, CanSignAllMappings),
        (newNS3Key, newNS2Key, CanSignAllButNamespaceDelegations),
      ).foreach { case (newKey, signedBy, delegationRestriction) =>
        participant1.topology.namespace_delegations.propose_delegation(
          Namespace(ns.fingerprint),
          newKey,
          delegationRestriction,
          signedBy = Seq(signedBy.fingerprint),
          synchronize = None,
        )
      }

      // remove old authorizations
      Seq((ns3, newNS2Key), (ns2, ns)).foreach { case (currentKey, signedBy) =>
        participant1.topology.namespace_delegations.propose_revocation(
          Namespace(ns.fingerprint),
          currentKey,
          signedBy = Seq(signedBy.fingerprint),
        )
      }

    }

    "verify that all keys in chain can authorize" in { implicit env =>
      import env.*

      val ns = getKey(newNamespaceKey)
      val key2 = getKey("2" + rootDelegationKey)
      val key3 = getKey("2" + intermediateDelegationKey)
      val key3Old = getKey(intermediateDelegationKey)

      val party =
        participant1.parties.list("indirect").headOption.valueOrFail("must be there").party
      val syncId = participant1.synchronizers
        .list_connected()
        .map(_.synchronizerId)
        .headOption
        .valueOrFail("it's there")

      def getSigningKeyOfPartyToParticipantMapping =
        participant1.topology.party_to_participant_mappings
          .list(synchronizerId = syncId, filterParty = party.filterString)
          .loneElement
          .context
          .signedBy

      // as we don't have cascading updates, the party to participant mapping should
      // still be there, but signed by the old key
      getSigningKeyOfPartyToParticipantMapping.forgetNE should contain theSameElementsAs Seq(
        key3Old.fingerprint,
        participant1.id.uid.namespace.fingerprint,
      )

      // renew the party to participant mapping using all keys
      Seq(key3, key2, ns).zipWithIndex.foreach { case (key, idx) =>
        val permission =
          if (idx % 2 == 0) ParticipantPermission.Confirmation
          else ParticipantPermission.Submission
        participant1.topology.party_to_participant_mappings.propose(
          party,
          Seq((participant1.id, permission)),
          signedBy = Seq(key.fingerprint),
        )
        eventually() {
          getSigningKeyOfPartyToParticipantMapping.forgetNE should contain theSameElementsAs Seq(
            key.fingerprint
          )
        }
      }
    }

  }

  "concurrently add topology transactions" in { implicit env =>
    import env.*
    val parties = Future
      .sequence((0 to 100).map { idx =>
        Future {
          val party =
            PartyId.tryCreate(s"concurrentParty$idx", participant1.id.uid.namespace)
          participant1.topology.party_to_participant_mappings.propose(
            party,
            Seq((participant1.id, ParticipantPermission.Submission)),
          )
          party
        }
      })
      .futureValue
      .toSet
    eventually() {
      val current = participant1.parties.list("concurrentParty").map(_.party).toSet
      current should contain theSameElementsAs parties
    }
  }

  "onboard a new participant" in { implicit env =>
    import env.*

    participant2.synchronizers.connect_multi(daName, sequencers.all.map(_.sequencerConnection))
    participant2.health.maybe_ping(participant1) should not be empty

    def compare[T](fetch: ParticipantReference => Seq[T]) =
      fetch(participant1).map(_.toString).sorted.mkString("\n") shouldBe fetch(participant2)
        .map(_.toString)
        .sorted
        .mkString("\n")

    val fs = participant1.synchronizers
      .list_connected()
      .headOption
      .valueOrFail("must be there")
      .synchronizerId
      .logical

    compare(_.topology.decentralized_namespaces.list(store = fs).map(_.item))
    compare(_.topology.namespace_delegations.list(store = fs).map(_.item))
    compare(_.topology.party_to_participant_mappings.list(synchronizerId = fs).map(_.item))
    compare(
      _.topology.participant_synchronizer_permissions
        .list(store = fs)
        .map(_.item)
    )
    compare(_.topology.synchronizer_parameters.list(store = fs).map(_.item))
    compare(_.topology.vetted_packages.list(store = fs).map(_.item))
    compare(_.topology.sequencers.list(store = fs).map(_.item))
    compare(_.topology.mediators.list(synchronizerId = fs).map(_.item))
    compare(_.topology.owner_to_key_mappings.list(store = fs).map(_.item))
    compare(_.topology.party_hosting_limits.list(store = fs).map(_.item))

  }

}
