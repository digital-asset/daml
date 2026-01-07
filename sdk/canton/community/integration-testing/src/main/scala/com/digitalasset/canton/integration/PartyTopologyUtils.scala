// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands.Write.GenerateTransactions.Proposal
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.crypto.{
  Fingerprint,
  SigningKeyUsage,
  SigningKeysWithThreshold,
  SigningPublicKey,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.scalatest.{LoneElement, OptionValues}

/** Utility methods for test to modify the PartyToParticipant state of a Party (local or external)
  * This is only useful to invoke the authority of the party.
  */
trait PartyTopologyUtils extends LoneElement with OptionValues {
  implicit class EnrichedParty(party: Party) {
    private[canton] object topology {
      private[canton] object party_to_participant_mappings {
        @VisibleForTesting
        private[canton] def propose(
            node: ParticipantReference,
            newParticipants: Seq[(ParticipantId, ParticipantPermission)],
            threshold: PositiveInt = PositiveInt.one,
            partySigningKeys: Option[SigningKeysWithThreshold] = None,
            serial: Option[PositiveInt] = None,
            operation: TopologyChangeOp = TopologyChangeOp.Replace,
            mustFullyAuthorize: Boolean = false,
            store: TopologyStoreId = TopologyStoreId.Authorized,
            forceFlags: ForceFlags = ForceFlags.none,
            participantsRequiringPartyToBeOnboarded: Seq[ParticipantId] = Nil,
        ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
          val env = node.consoleEnvironment
          val synchronize = Some(env.commandTimeouts.bounded)

          val currentKeys = node.topology.party_to_participant_mappings
            .findCurrent(
              party.partyId,
              store,
            )
            .flatMap(_.item.partySigningKeysWithThreshold.map(_.keys))

          party match {
            case externalParty: ExternalParty =>
              val updatedMapping = PartyToParticipant.tryCreate(
                partyId = party.partyId,
                threshold = threshold,
                participants = newParticipants.map { case (pid, permission) =>
                  HostingParticipant(
                    pid,
                    permission,
                    onboarding = participantsRequiringPartyToBeOnboarded.contains(pid),
                  )
                },
                partySigningKeys,
              )
              // In the test we could easily build the transaction directly, but external parties will likely
              // use the generate endpoint, so let's use it too
              val proposals = Seq(
                Proposal(
                  mapping = updatedMapping,
                  store = store,
                  change = operation,
                  serial = serial,
                )
              )

              val updatedTransaction = node.topology.transactions
                .generate(proposals)
                .toList
                .loneElement
                .selectMapping[PartyToParticipant]
                .getOrElse(throw new IllegalStateException("Expected PartyToParticipant mapping"))

              val newKeys =
                updatedTransaction.mapping.partySigningKeys.diff(
                  currentKeys.map(_.forgetNE).getOrElse(Set.empty)
                )
              val newSignatures = newKeys.map { newKey =>
                env.global_secret.sign(
                  updatedTransaction.hash.hash.getCryptographicEvidence,
                  newKey.fingerprint,
                  NonEmpty.mk(Set, SigningKeyUsage.Protocol: SigningKeyUsage),
                )
              }

              val namespaceSignature = env.global_secret.sign(
                updatedTransaction.hash.hash.getCryptographicEvidence,
                externalParty.fingerprint,
                NonEmpty.mk(Set, SigningKeyUsage.Namespace: SigningKeyUsage),
              )

              val signedTopologyTransaction = SignedTopologyTransaction.withTopologySignatures(
                updatedTransaction,
                NonEmpty
                  .mk(Seq, namespaceSignature, newSignatures.toSeq*)
                  .map(SingleTransactionSignature(updatedTransaction.hash, _)),
                isProposal = true,
                protocolVersion = ProtocolVersion.latest,
              )

              node.topology.transactions.load(
                Seq(signedTopologyTransaction),
                store,
                synchronize = synchronize,
                forceFlags = forceFlags,
              )

              signedTopologyTransaction
            case localParty: PartyId =>
              node.topology.party_to_participant_mappings.propose(
                party.partyId,
                newParticipants,
                threshold,
                partySigningKeys,
                serial,
                signedBy = Seq.empty,
                operation,
                synchronize,
                mustFullyAuthorize,
                store,
                forceFlags,
                participantsRequiringPartyToBeOnboarded,
              )
          }
        }

        /** Update the party to participant mapping of a party and authorize that update with the
          * party's namespace
          * @param node
          *   node through which to submit the update. For local parties, this must be the node that
          *   shares the party's namespace
          * @param synchronizerId
          *   synchronizerId on which to update the topology
          * @param updater
          *   function to update the PTP
          */
        @VisibleForTesting
        private[canton] def propose_delta(
            node: ParticipantReference,
            adds: Seq[(ParticipantId, ParticipantPermission)] = Nil,
            removes: Seq[ParticipantId] = Nil,
            serial: Option[PositiveInt] = None,
            mustFullyAuthorize: Boolean = false,
            store: TopologyStoreId = TopologyStoreId.Authorized,
            forceFlags: ForceFlags = ForceFlags.none,
            requiresPartyToBeOnboarded: Boolean = false,
            signingKeysAdds: Set[SigningPublicKey] = Set.empty,
            signingKeysRemoves: Set[Fingerprint] = Set.empty,
            newSigningThreshold: Option[PositiveInt] = None,
        ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
          val env = node.consoleEnvironment
          val synchronize = Some(env.commandTimeouts.bounded)

          val (existingPermissions, newPermissions, newSerial, threshold, partySigningKeys) =
            node.topology.party_to_participant_mappings.computeDelta(
              party.partyId,
              adds,
              removes,
              store,
              serial,
            )

          val newSigningKeys =
            partySigningKeys.map { case SigningKeysWithThreshold(keys, threshold) =>
              SigningKeysWithThreshold.tryCreate(
                NonEmpty
                  .from(
                    keys.filterNot(k =>
                      signingKeysRemoves.contains(k.fingerprint)
                    ) ++ signingKeysAdds
                  )
                  .getOrElse(
                    throw new IllegalStateException("New signing keys set is empty")
                  )
                  .toSeq,
                newSigningThreshold.getOrElse(threshold),
              )
            }

          party match {
            case _: PartyId =>
              // For local parties we delegate to the node's propose_delta
              node.topology.party_to_participant_mappings.propose_delta(
                party = party.partyId,
                adds = adds,
                removes = removes,
                signedBy = Option.empty,
                serial = serial,
                synchronize = synchronize,
                mustFullyAuthorize = mustFullyAuthorize,
                store = store,
                forceFlags = forceFlags,
                requiresPartyToBeOnboarded = requiresPartyToBeOnboarded,
              )
            case _: ExternalParty if newPermissions.nonEmpty =>
              propose(
                node = node,
                newParticipants = newPermissions.toSeq,
                threshold = threshold,
                operation = TopologyChangeOp.Replace,
                serial = newSerial,
                mustFullyAuthorize = mustFullyAuthorize,
                store = store,
                forceFlags = forceFlags,
                participantsRequiringPartyToBeOnboarded =
                  if (requiresPartyToBeOnboarded) adds.map(_._1) else Nil,
                partySigningKeys = newSigningKeys,
              )
            case _: ExternalParty =>
              propose(
                node = node,
                newParticipants = existingPermissions.toSeq,
                threshold = threshold,
                operation = TopologyChangeOp.Remove,
                serial = newSerial,
                mustFullyAuthorize = mustFullyAuthorize,
                store = store,
                forceFlags = forceFlags,
                partySigningKeys = newSigningKeys,
              )
          }
        }
      }
    }
  }
}
