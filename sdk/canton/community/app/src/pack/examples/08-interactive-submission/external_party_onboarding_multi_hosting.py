# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time
import json
import argparse
from grpc import Channel
from google.protobuf.json_format import MessageToJson
import grpc
from google.protobuf import empty_pb2
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
from external_party_onboarding import (
    onboard_external_party,
    wait_to_observe_party_to_participant,
)


# Authorize an external party hosting on a participant node
def authorize_external_party_hosting(
    party_id: str,
    synchronizer_id: str,
    channel: Channel,
    auto_accept: bool,
):
    """
    Authorizes the hosting of a multi-hosted external party on the current node.
    Expects the PartyToParticipant proposal to have already been published to the synchronizer.

    Args:
        party_id (str): ID of the party.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        channel (grpc.Channel): gRPC channel to the confirming participant Admin API.
        auto_accept (bool): Will not ask for confirmation when true.
    """
    print(f"Authorizing hosting of {party_id}")

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
                        synchronizer=common_pb2.StoreId.Synchronizer(
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
            transaction_hash_bytes=party_to_participant_proposal.context.transaction_hash,
            must_fully_authorize=False,
            store=common_pb2.StoreId(
                synchronizer=common_pb2.StoreId.Synchronizer(
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
    print(f"Participant ID = {status_response.status.common_status.uid}")
    return status_response.status.common_status.uid


def multi_host_party(
    party_name: str,
    synchronizer_id: str,
    confirming_threshold: int,
    participant_data: object,
    auto_accept: bool,
):
    """
    Onboard a multi hosted party.

    Args:
        party_name (str): Name of the party.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        confirming_threshold (int): Minimum number of confirmations that must be received from the confirming participants to authorize a transaction.
        participant_data (object): Mapping of participant ID to endpoint of their admin API.
        auto_accept (bool): Will not ask for confirmation when true.
    """
    print(f"Authorizing hosting of {party_name}")
    participant_names = list(participant_data.keys())
    if not participant_names:
        raise ValueError("No participants provided in the participant data.")
    channels = {}
    participant_ids = []
    for participant_name in participant_names:
        channels[participant_name] = grpc.insecure_channel(
            participant_data[participant_name]
        )
        # Get the participant id from each participant
        participant_ids = participant_ids + [
            get_participant_id(channels[participant_name])
        ]

    (_, party_namespace) = onboard_external_party(
        party_name,
        participant_ids,
        confirming_threshold,
        synchronizer_id,
        # Pick one of the participants to do the initial external party onboarding
        channels[participant_names[0]],
    )
    party_id = party_name + "::" + party_namespace

    # Authorize hosting for each additional confirming participant
    # In reality this wouldn't be done from a central place like here but every hosting participant validator
    # would run this on their own node
    for additional_participant_name in participant_names[1:]:
        authorize_external_party_hosting(
            party_id,
            synchronizer_id,
            channels[additional_participant_name],
            auto_accept,
        )

    # Wait for the party to appear in topology for all participants
    for _, channel in channels.items():
        with channel:
            topology_read_client = (
                topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(
                    channel
                )
            )
            wait_to_observe_party_to_participant(
                topology_read_client, synchronizer_id, party_id
            )

    print(f"Multi-Hosted party {party_id} fully onboarded")


def read_id_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Hosted external party")
    parser.add_argument(
        "--participant-endpoints",
        type=str,
        help="Path to JSON file containing participant IDs and their endpoints (address + port)",
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

    args = parser.parse_args()

    if args.participant_endpoints:
        try:
            with open(args.participant_endpoints, "r") as f:
                participant_data_raw = json.load(f)
                # Extract only the adminApi port and hardcode the address to localhost
                participant_data = {
                    # In this demo we assume all hosting participants are running on localhost
                    participant_id: f"localhost:{details['adminApi']}"
                    for participant_id, details in participant_data_raw.items()
                    if details.get("adminApi") is not None
                }
                multi_host_party(
                    args.party_name,
                    args.synchronizer_id,
                    args.threshold,
                    participant_data,
                    args.auto_accept,
                )
        except FileNotFoundError:
            print(f"File {args.participant_endpoints} not found.")
        except json.JSONDecodeError:
            print(f"Failed to decode JSON file {args.participant_endpoints}.")
    else:
        parser.print_help()
