# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time
import argparse

from typing import Optional
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from interactive_topology_util import (
    serialize_topology_transaction,
    compute_topology_transaction_hash,
    sign_hash,
)
from cryptography.hazmat.primitives.serialization import load_der_private_key
from cryptography.hazmat.backends import default_backend
from grpc import Channel
from google.protobuf.json_format import MessageToJson
import grpc
from google.protobuf import empty_pb2
from com.digitalasset.canton.protocol.v30 import topology_pb2
from com.digitalasset.canton.crypto.v30 import crypto_pb2
from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_write_service_pb2_grpc,
)
from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_write_service_pb2,
)
from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_read_service_pb2_grpc,
)
from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_read_service_pb2,
    common_pb2,
)
from com.digitalasset.canton.admin.participant.v30 import (
    participant_status_service_pb2,
    participant_status_service_pb2_grpc,
)
from external_party_onboarding_admin_api import (
    onboard_external_party,
    wait_to_observe_party_to_participant,
)


# Authorize an external party hosting on a participant node
def authorize_external_party_hosting(
    participant_id: str,
    party_id: str,
    synchronizer_id: str,
    channel: Channel,
    auto_accept: bool,
) -> bool:
    """
    Authorizes the hosting of a multi-hosted external party on the current node.
    Expects the PartyToParticipant proposal to have already been published to the synchronizer.

    Args:
        party_id (str): ID of the party.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        channel (grpc.Channel): gRPC channel to the confirming participant Admin API.
        auto_accept (bool): Will not ask for confirmation when true.
    """
    print(f"Authorizing hosting of {party_id} on target participant {participant_id}")

    topology_write_client = (
        topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub(channel)
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )

    # Retrieve the pending proposal
    transaction_in_store = False
    party_to_participant_proposals: (
        topology_manager_read_service_pb2.ListPartyToParticipantResponse
    )
    while not transaction_in_store:
        party_to_participant_proposals: (
            topology_manager_read_service_pb2.ListPartyToParticipantResponse
        ) = topology_read_client.ListPartyToParticipant(
            topology_manager_read_service_pb2.ListPartyToParticipantRequest(
                base_query=topology_manager_read_service_pb2.BaseQuery(
                    store=common_pb2.StoreId(
                        synchronizer=common_pb2.Synchronizer(
                            id=synchronizer_id,
                        ),
                    ),
                    proposals=True,
                    head_state=empty_pb2.Empty(),
                ),
                filter_party=party_id,
            )
        )
        if len(party_to_participant_proposals.results) > 0:
            break
        else:
            time.sleep(0.5)
            continue
    # Expecting a single pending proposal for the party
    party_to_participant_proposal: (
        topology_manager_read_service_pb2.ListPartyToParticipantResponse.Result
    ) = party_to_participant_proposals.results[0]

    if not auto_accept:
        print(MessageToJson(party_to_participant_proposal))
        user_input = input("Authorize party hosting? (y/n): ")
        if user_input.lower() != "y":
            print("Transaction rejected.")
            sys.exit(0)

    # Authorize the hosting
    topology_write_client.Authorize(
        topology_manager_write_service_pb2.AuthorizeRequest(
            transaction_hash=party_to_participant_proposal.context.transaction_hash.hex(),
            must_fully_authorize=False,
            store=common_pb2.StoreId(
                synchronizer=common_pb2.Synchronizer(
                    id=synchronizer_id,
                ),
            ),
        )
    )


def get_participant_id(channel: grpc.Channel) -> str:
    status_service_client = (
        participant_status_service_pb2_grpc.ParticipantStatusServiceStub(channel)
    )
    status_response: participant_status_service_pb2.ParticipantStatusResponse = (
        status_service_client.ParticipantStatus(
            participant_status_service_pb2.ParticipantStatusRequest()
        )
    )
    return status_response.status.common_status.uid


def update_party_to_participant_transaction(
    channel: grpc.Channel,
    party_id: str,
    additional_participants: [topology_pb2.PartyToParticipant.HostingParticipant],
    synchronizer_id: str,
    confirming_threshold: Optional[int],
) -> (bytes, list[str]):
    """
    Constructs a topology transaction that updates the party-to-participant mapping with additional hosting nodes.

    Args:
        channel (grpc.Channel): gRPC channel for communication with the topology manager.
        party_id (str): Identifier of the party whose key mapping is being updated.
        additional_participants ([topology_pb2.PartyToParticipant.HostingParticipant]): A list of additional hosting participants and their hosting permission.
        synchronizer_id (str): ID of the synchronizer to query the topology state.
        confirming_threshold (int): Updated confirming threshold

    Returns:
        bytes: Serialized topology transaction containing the updated mapping. None if the provided nodes already host the party with those permissions
        [str]: list of participant_ids added to the party hosting and requiring approval
    """
    # Retrieve the current party to participant mapping
    list_party_to_participant_request = (
        topology_manager_read_service_pb2.ListPartyToParticipantRequest(
            base_query=topology_manager_read_service_pb2.BaseQuery(
                store=common_pb2.StoreId(
                    synchronizer=common_pb2.Synchronizer(
                        id=synchronizer_id,
                    ),
                ),
                head_state=empty_pb2.Empty(),
            ),
            filter_party=party_id,
        )
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )
    party_to_participant_response: (
        topology_manager_read_service_pb2.ListPartyToParticipantResponse
    ) = topology_read_client.ListPartyToParticipant(list_party_to_participant_request)
    if len(party_to_participant_response.results) == 0:
        current_serial = 1
        current_participants_list = []
    else:
        # Sort the results by serial in descending order and take the first one
        sorted_results = sorted(
            party_to_participant_response.results,
            key=lambda result: result.context.serial,
            reverse=True,
        )
        # Get the mapping with the highest serial and its list of hosting participants
        current_serial = sorted_results[0].context.serial
        current_participants_list: topology_pb2.PartyToParticipant = sorted_results[
            0
        ].item

    # Map of existing participant_uid -> hosting
    participant_id_to_hosting = {
        participant.participant_uid: participant
        for participant in current_participants_list.participants
    }
    # Keep track of the new hosting nodes, as we'll need to approve the hosting on each of them as well
    new_hosting_nodes = []
    # Update map with new participants
    for new_hosting in additional_participants:
        if new_hosting.participant_uid not in participant_id_to_hosting:
            new_hosting_nodes = new_hosting_nodes + [new_hosting.participant_uid]
        participant_id_to_hosting[new_hosting.participant_uid] = new_hosting

    if confirming_threshold is not None:
        updated_threshold = confirming_threshold
    else:
        updated_threshold = current_participants_list.threshold

    # Create a new mapping with the updated hosting relationships and increment the serial
    updated_mapping = topology_pb2.TopologyMapping(
        party_to_participant=topology_pb2.PartyToParticipant(
            party=party_id,
            threshold=updated_threshold,
            participants=list(participant_id_to_hosting.values()),
        )
    )

    # Build the serialized transaction
    return (
        serialize_topology_transaction(updated_mapping, serial=current_serial + 1),
        new_hosting_nodes,
    )


def update_external_party_hosting(
    party_id: str,
    synchronizer_id: str,
    confirming_threshold: Optional[int],
    additional_hosting_participants: [
        topology_pb2.PartyToParticipant.HostingParticipant
    ],
    namespace_private_key: EllipticCurvePrivateKey,
    admin_api_channel: Channel,
) -> [str]:
    """
    Authorize replication of an external party to additional hosting nodes.

    Args:
        party_id (str): Identifier of the party whose key mapping is being updated.
        synchronizer_id (str): ID of the synchronizer to query the topology state.
        confirming_threshold (Optional[int]): Updated confirming threshold. Optional, if None the threshold stays unchanged.
        admin_api_channel (grpc.Channel): gRPC channel to the .
        additional_hosting_participants ([topology_pb2.PartyToParticipant.HostingParticipant]): A list of additional hosting participants and their hosting permission.
        namespace_private_key (EllipticCurvePrivateKey): private namespace key of the external party

    Returns:
        [str]: list of participant_ids added to the party hosting and requiring approval
    """
    updated_party_to_participant_transaction, nodes_requiring_auth = (
        update_party_to_participant_transaction(
            admin_api_channel,
            party_id,
            additional_hosting_participants,
            synchronizer_id,
            confirming_threshold,
        )
    )

    party_to_participant_transaction_hash = compute_topology_transaction_hash(
        updated_party_to_participant_transaction
    )
    signature = sign_hash(namespace_private_key, party_to_participant_transaction_hash)
    fingerprint = party_id.split("::")[1]
    signed_topology_transaction = topology_pb2.SignedTopologyTransaction(
        transaction=updated_party_to_participant_transaction,
        signatures=[
            crypto_pb2.Signature(
                format=crypto_pb2.SignatureFormat.SIGNATURE_FORMAT_DER,
                signature=signature,
                signed_by=fingerprint,
                signing_algorithm_spec=crypto_pb2.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256,
            )
        ],
        multi_transaction_signatures=[],
        proposal=True,
    )

    topology_write_client = (
        topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub(
            admin_api_channel
        )
    )

    add_transactions_request = (
        topology_manager_write_service_pb2.AddTransactionsRequest(
            transactions=[signed_topology_transaction],
            store=common_pb2.StoreId(
                synchronizer=common_pb2.Synchronizer(
                    id=synchronizer_id,
                ),
            ),
        )
    )
    topology_write_client.AddTransactions(add_transactions_request)
    return nodes_requiring_auth


def multi_host_party(
    party_name: str,
    synchronizer_id: str,
    confirming_threshold: int,
    participant_endpoints: [str],
    auto_accept: bool,
) -> (EllipticCurvePrivateKey, str):
    """
    Onboard a multi hosted party.

    Args:
        party_name (str): Name of the party.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        confirming_threshold (int): Minimum number of confirmations that must be received from the confirming participants to authorize a transaction.
        participant_endpoints ([str]]): List of endpoints to the respective hosting participant Admin APIs.
        auto_accept (bool): Will not ask for confirmation when true.
    """
    print(f"Authorizing hosting of {party_name}")
    channels = []
    participant_ids = []
    for participant_endpoint in participant_endpoints:
        channel = grpc.insecure_channel(participant_endpoint)
        channels = channels + [channel]
        # Get the participant id from each participant
        participant_ids = participant_ids + [get_participant_id(channel)]

    (party_private_key, party_namespace) = onboard_external_party(
        party_name,
        participant_ids,
        confirming_threshold,
        synchronizer_id,
        # Pick one of the participants to do the initial external party onboarding
        channels[0],
    )
    party_id = party_name + "::" + party_namespace

    # Authorize hosting for each additional confirming participant
    # In reality this wouldn't be done from a central place like here but every hosting participant validator
    # would run this on their own node
    for index, additional_participant_channel in enumerate(channels[1:]):
        authorize_external_party_hosting(
            participant_ids[index],
            party_id,
            synchronizer_id,
            additional_participant_channel,
            auto_accept,
        )

    # Wait for the party to appear in topology for all participants
    for participant_channel in channels:
        with participant_channel:
            topology_read_client = (
                topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(
                    participant_channel
                )
            )
            wait_to_observe_party_to_participant(
                topology_read_client, synchronizer_id, party_id
            )

    print(f"Multi-Hosted party {party_id} fully onboarded")
    return party_private_key, party_namespace


def read_id_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


"""
   Exemple script demonstrating how to onboard a multi hosted external party, and update the hosting relationships of an existing party.
   ATTENTION: Replicating an existing party to additional hosting nodes requires following a specific procedure.
   Check the offline party replication documentation for more details. This script simply demonstrates how to authorize changes
   to the PartyToParticipant mapping for an external party.
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Hosted external party")
    parser.add_argument(
        "--admin-endpoint",
        type=str,
        nargs="+",
        help="address:port of the admin API of hosting nodes",
    )
    parser.add_argument(
        "--synchronizer-id",
        type=str,
        help="Synchronizer ID",
        default=read_id_from_file("synchronizer_id"),
    )
    parser.add_argument(
        "--party-name",
        type=str,
        help="Party name",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        help="Confirmation threshold",
    )
    parser.add_argument(
        "--auto-accept",
        "-a",
        help="Authorize party hosting without explicit confirmation",
        action="store_true",
    )
    parser.add_argument(
        "--private-key-file",
        type=str,
        help="Path of the file holding the external party's private key",
    )

    subparsers = parser.add_subparsers(required=True, dest="subcommand")
    parser_onboard = subparsers.add_parser(
        "onboard", help="Onboard a multi-hosted external party"
    )
    parser_replicate = subparsers.add_parser(
        "update",
        help="Update the permissions or add new hosting nodes to the party-to-participant mapping of an existing external party",
    )
    parser_replicate.add_argument(
        "--party-id",
        type=str,
        help="External party ID",
    )
    parser_replicate.add_argument(
        "--participant-id",
        type=str,
        help="Participant ID of the new hosting participant",
    )
    parser_replicate.add_argument(
        "--participant-permission",
        type=str,
        choices=["confirmation", "observation"],
        nargs="+",
        help="Permission of the new hosting participants (confirmation or observation). One per new hosting participant.",
    )

    args = parser.parse_args()

    if args.subcommand == "onboard":
        party_private_key, party_fingerprint = multi_host_party(
            args.party_name,
            args.synchronizer_id,
            args.threshold,
            args.admin_endpoint,
            args.auto_accept,
        )

        private_key_file = (
            args.private_key_file
            or f"{args.party_name}::{party_fingerprint}-private-key.der"
        )
        with open(private_key_file, "wb") as key_file:
            key_file.write(
                party_private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
        print(f"Party ID: {args.party_name}::{party_fingerprint}")
        print(f"Written private key to: {private_key_file}")

    elif args.subcommand == "update":
        with open(args.private_key_file, "rb") as key_file:
            private_key = load_der_private_key(
                key_file.read(),
                password=None,  # Use this if the key is not encrypted
                backend=default_backend(),
            )
            channels = {}
            # New hosting relationships
            hosting = []
            for index, endpoint in enumerate(args.admin_endpoint):
                channel = grpc.insecure_channel(endpoint)
                participant_id = get_participant_id(channel)
                channels[participant_id] = grpc.insecure_channel(endpoint)
                permission_str = args.participant_permission[index]
                if permission_str == "confirmation":
                    permission = (
                        topology_pb2.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
                    )
                else:
                    permission = (
                        topology_pb2.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
                    )

                hosting = hosting + [
                    topology_pb2.PartyToParticipant.HostingParticipant(
                        participant_uid=participant_id, permission=permission
                    )
                ]

            nodes_requiring_auth = update_external_party_hosting(
                args.party_id,
                args.synchronizer_id,
                args.threshold,
                hosting,
                private_key,
                # Pick one of the participants to load the updated hosting mapping signed by the party.
                # It doesn't matter which one here, we just use the node's admin API to load the externally signed
                # updated topology transaction onto the synchronizer
                list(channels.values())[0],
            )
            # Then authorize the hosting on each new hosting node
            for participant_id, channel in channels.items():
                # If new nodes are hosting the party, approve the hosting on the nodes
                if participant_id in nodes_requiring_auth:
                    authorize_external_party_hosting(
                        participant_id,
                        args.party_id,
                        args.synchronizer_id,
                        channel,
                        args.auto_accept,
                    )

                # Observe the party on the participants
                # TODO(i27030): check the permission matches
                topology_read_client = topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(
                    channel
                )
                wait_to_observe_party_to_participant(
                    topology_read_client, args.synchronizer_id, args.party_id
                )
            print("Hosting updated ")
    else:
        parser.print_help()
