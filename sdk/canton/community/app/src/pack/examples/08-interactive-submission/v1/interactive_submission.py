# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.

# Simple example of an interactive submission demonstrating the external signing flow

import argparse
import grpc
import uuid
from com.daml.ledger.api.v2.interactive import interactive_submission_service_pb2_grpc
from com.daml.ledger.api.v2.interactive import interactive_submission_service_pb2
from com.daml.ledger.api.v2 import commands_pb2, value_pb2, completion_pb2
from external_party_onboarding import onboard_external_party
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from transaction_util import create_nodes_dict, encode_prepared_transaction
from com.daml.ledger.api.v2 import (
    command_completion_service_pb2,
    command_completion_service_pb2_grpc,
    update_service_pb2,
    update_service_pb2_grpc,
    event_pb2,
    state_service_pb2_grpc,
    state_service_pb2,
    transaction_filter_pb2,
    event_query_service_pb2_grpc,
    event_query_service_pb2,
)
import sys
import os
import json

application_id = "demo_python_app"
# Path to the Canton ports file - This file is created when canton starts using the configuration in this folder
# and stores the ports for the Ledger and Admin API.
json_file_path = "canton_ports.json"
# Load the JSON content from the file
# Try to load the JSON file, if it doesn't exist, fallback to defaults
try:
    with open(json_file_path, "r") as f:
        config = json.load(f)
    # Default port values from JSON if available
    default_lapi_port = config.get("participant1", {}).get("ledgerApi")
    default_admin_port = config.get("participant1", {}).get("adminApi")
except FileNotFoundError:
    print(f"{json_file_path} not found. Using default port values.")
except json.JSONDecodeError:
    print(f"Failed to decode {json_file_path}. Using default port values.")

# Get ports from environment variables, fallback to default values (from JSON) if not set
lapi_port = os.environ.get("CANTON_LAPI_PORT", default_lapi_port)
admin_port = os.environ.get("CANTON_ADMIN_PORT", default_admin_port)

# Create gRPC channels for the admin and Ledger API (lapi)
lapi_channel = grpc.insecure_channel(f"localhost:{lapi_port}")
admin_channel = grpc.insecure_channel(f"localhost:{admin_port}")

# Interactive submission service client - used to submit externally signed transactions
iss_client = interactive_submission_service_pb2_grpc.InteractiveSubmissionServiceStub(
    lapi_channel
)
# Command completion service client - used to observe command execution results
ccs_client = command_completion_service_pb2_grpc.CommandCompletionServiceStub(
    lapi_channel
)
# Update service client - used to query transactions once they've completed
us_client = update_service_pb2_grpc.UpdateServiceStub(lapi_channel)
# State service client - used to query active contracts
state_client = state_service_pb2_grpc.StateServiceStub(lapi_channel)
# Event query service client - used to retrieve event information of a completed transaction
eqs_client = event_query_service_pb2_grpc.EventQueryServiceStub(lapi_channel)

ping_template_id = value_pb2.Identifier(
    package_id="#AdminWorkflows",
    module_name="Canton.Internal.Ping",
    entity_name="Ping",
)


# Return active contracts for a party
def get_active_contracts(party: str):
    ledger_end_response: state_service_pb2.GetLedgerEndResponse = (
        state_client.GetLedgerEnd(state_service_pb2.GetLedgerEndRequest())
    )
    active_contracts_response = state_client.GetActiveContracts(
        state_service_pb2.GetActiveContractsRequest(
            filter=transaction_filter_pb2.TransactionFilter(
                filters_by_party={
                    party: transaction_filter_pb2.Filters(
                        cumulative=[
                            transaction_filter_pb2.CumulativeFilter(
                                wildcard_filter=transaction_filter_pb2.WildcardFilter(
                                    include_created_event_blob=True
                                )
                            )
                        ]
                    )
                }
            ),
            active_at_offset=ledger_end_response.offset,
            verbose=True,
        )
    )
    return active_contracts_response


def get_events(party: str, contract_id: str):
    contract_event_response: event_query_service_pb2.GetEventsByContractIdResponse = (
        eqs_client.GetEventsByContractId(
            event_query_service_pb2.GetEventsByContractIdRequest(
                contract_id=contract_id, requesting_parties=[party]
            )
        )
    )
    return contract_event_response


# Execute a submission request and return the corresponding event
# For simplicity this assumes a single contract was either created or archived by the transaction
def execute_and_get_contract_id(
    prepared_transaction: interactive_submission_service_pb2.PreparedTransaction,
    party: str,
    party_private_key: Ed25519PrivateKey,
    pub_fingerprint: str,
):

    # Compute the transaction hash
    transaction_hash = encode_prepared_transaction(
        prepared_transaction, create_nodes_dict(prepared_transaction)
    )
    # Sign it
    signed_hash = party_private_key.sign(
        transaction_hash, signature_algorithm=ec.ECDSA(hashes.SHA256())
    )
    # Create the execute request
    execute_request = interactive_submission_service_pb2.ExecuteSubmissionRequest(
        prepared_transaction=prepared_transaction,
        application_id=application_id,
        party_signatures=interactive_submission_service_pb2.PartySignatures(
            signatures=[
                interactive_submission_service_pb2.SinglePartySignatures(
                    party=party,
                    signatures=[
                        interactive_submission_service_pb2.Signature(
                            format=interactive_submission_service_pb2.SignatureFormat.SIGNATURE_FORMAT_RAW,
                            signature=signed_hash,
                            signed_by=pub_fingerprint,
                            signing_algorithm_spec=interactive_submission_service_pb2.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256,
                        )
                    ],
                )
            ]
        ),
        hashing_scheme_version=interactive_submission_service_pb2.HashingSchemeVersion.HASHING_SCHEME_VERSION_V1,
        submission_id=str(uuid.uuid4()),
    )

    # Submit the transaction to the ledger
    iss_client.ExecuteSubmission(execute_request)

    # Waiting for the transaction to show on the completion stream
    update_request = command_completion_service_pb2.CompletionStreamRequest(
        application_id=application_id, parties=[party]
    )
    completion_stream = ccs_client.CompletionStream(update_request)
    for update in completion_stream:
        if (
            update.HasField("completion")
            and update.completion.submission_id == execute_request.submission_id
        ):
            completion: completion_pb2.Completion = update.completion
            break

    transaction_response: update_service_pb2.GetTransactionResponse = (
        us_client.GetTransactionById(
            update_service_pb2.GetTransactionByIdRequest(
                update_id=completion.update_id,
                requesting_parties=[party],
            )
        )
    )
    for event in transaction_response.transaction.events:
        if event.HasField("created"):
            contract_id = event.created.contract_id
            break
        if event.HasField("archived"):
            contract_id = event.archived.contract_id
            break

    return contract_id


def create_ping_contract(
    initiator: str,
    initiator_private_key: Ed25519PrivateKey,
    initiator_fingerprint: str,
    responder: str,
    domain_id: str,
) -> event_pb2.CreatedEvent:
    # A command to create a ping contract
    ping_create_command = commands_pb2.Command(
        create=commands_pb2.CreateCommand(
            template_id=ping_template_id,
            create_arguments=value_pb2.Record(
                record_id=None,
                fields=[
                    value_pb2.RecordField(
                        label="id", value=value_pb2.Value(text="ping_id")
                    ),
                    value_pb2.RecordField(
                        label="initiator", value=value_pb2.Value(party=initiator)
                    ),
                    value_pb2.RecordField(
                        label="responder", value=value_pb2.Value(party=responder)
                    ),
                ],
            ),
        )
    )

    print("Preparing create ping transaction")
    # Prepare the submission request
    prepare_create_request = (
        interactive_submission_service_pb2.PrepareSubmissionRequest(
            application_id=application_id,
            command_id=str(uuid.uuid4()),
            act_as=[initiator],
            read_as=[initiator],
            domain_id=domain_id,
            commands=[ping_create_command],
        )
    )

    # Call the PrepareSubmission RPC
    prepare_create_response = iss_client.PrepareSubmission(prepare_create_request)

    prepared_create_transaction = prepare_create_response.prepared_transaction

    # Create the ping contract
    print("Submitting create ping transaction")
    ping_contract_id = execute_and_get_contract_id(
        prepared_create_transaction,
        initiator,
        initiator_private_key,
        initiator_fingerprint,
    )

    ping_created_event: event_pb2.CreatedEvent
    initiator_active_contracts = get_active_contracts(initiator)
    # Find the contract in the active contract store
    for active_contract_response in initiator_active_contracts:
        if (
            active_contract_response.HasField("active_contract")
            and active_contract_response.active_contract.created_event.contract_id
            == ping_contract_id
        ):
            ping_created_event = active_contract_response.active_contract.created_event
            break

    print(
        f"Ping contract with ID {ping_contract_id} is found in {initiator}'s active contract store"
    )

    return ping_created_event


def exercise_respond_choice(
    responder: str,
    responder_private_key: Ed25519PrivateKey,
    responder_fingerprint: str,
    domain_id: str,
    contract_id: str,
    created_event_blob: bytes,
    template_id: value_pb2.Identifier,
):
    ping_exercise_command = commands_pb2.Command(
        exercise=commands_pb2.ExerciseCommand(
            template_id=ping_template_id,
            contract_id=contract_id,
            choice="Respond",
            choice_argument=value_pb2.Value(
                record=value_pb2.Record(record_id=None, fields=[])
            ),
        )
    )

    print("Preparing exercise Respond choice transaction")
    prepare_exercise_request = interactive_submission_service_pb2.PrepareSubmissionRequest(
        application_id=application_id,
        command_id=str(uuid.uuid4()),
        act_as=[responder],
        read_as=[responder],
        domain_id=domain_id,
        commands=[ping_exercise_command],
        # We need to explicitly disclosed the ping contract we created earlier
        disclosed_contracts=[
            commands_pb2.DisclosedContract(
                template_id=template_id,
                contract_id=contract_id,
                created_event_blob=created_event_blob,
                domain_id=domain_id,
            )
        ],
    )

    prepare_exercise_response = iss_client.PrepareSubmission(prepare_exercise_request)

    prepared_exercise_transaction = prepare_exercise_response.prepared_transaction

    print("Submitting exercise Respond choice transaction")
    # Exercise the Respond choice on the ping contract by bob
    execute_and_get_contract_id(
        prepared_exercise_transaction,
        responder,
        responder_private_key,
        responder_fingerprint,
    )

    # The contract was archived by exercising the choice, we we get an archived event this time
    contract_events = get_events(responder, contract_id)

    print(
        f"Ping contract with ID {contract_events.archived.archived_event.contract_id} has been archived"
    )


def demo_interactive_submissions(participant_id: str, domain_id: str):
    alice_pk, alice_pub_fingerprint = onboard_external_party(
        "alice", participant_id, domain_id, admin_channel
    )
    print("Alice onboarded successfully")
    alice = "alice::" + alice_pub_fingerprint
    bob_pk, bob_pub_fingerprint = onboard_external_party(
        "bob", participant_id, domain_id, admin_channel
    )
    print("Bob onboarded successfully")
    bob = "bob::" + bob_pub_fingerprint

    # Alice creates the ping contract
    ping_created_event = create_ping_contract(
        alice, alice_pk, alice_pub_fingerprint, bob, domain_id
    )

    # Bob exercises the respond choice, which archives the contract
    exercise_respond_choice(
        bob,
        bob_pk,
        bob_pub_fingerprint,
        domain_id,
        ping_created_event.contract_id,
        ping_created_event.created_event_blob,
        ping_created_event.template_id,
    )


def read_id_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interactive submission utility")
    parser.add_argument(
        "--participant-id",
        type=str,
        help="Participant ID",
        default=read_id_from_file("participant_id"),
    )
    parser.add_argument(
        "--domain-id",
        type=str,
        help="Domain ID",
        default=read_id_from_file("domain_id"),
    )

    subparsers = parser.add_subparsers(required=True, dest="subcommand")
    parser_run_demo = subparsers.add_parser("run-demo", help="Run the ping demo")
    parser_onboard_party = subparsers.add_parser(
        "create-party", help="Create a new external party"
    )
    parser_onboard_party.add_argument(
        "--name",
        type=str,
        help="Name of the party",
        required=True,
    )
    parser_onboard_party.add_argument(
        "--private-key-file",
        type=str,
        help="Path of the file to which the private key should be written to",
    )

    args = parser.parse_args()

    if args.subcommand == "run-demo":
        demo_interactive_submissions(args.participant_id, args.domain_id)
    elif args.subcommand == "create-party":
        party_private_key, party_fingerprint = onboard_external_party(
            args.name, args.participant_id, args.domain_id, admin_channel
        )
        private_key_file = (
            args.private_key_file or f"{args.name}::{party_fingerprint}-private-key.pem"
        )
        with open(private_key_file, "wb") as key_file:
            key_file.write(
                party_private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
        print(f"Party ID: {args.name}::{party_fingerprint}")
        print(f"Written private key to: {private_key_file}")
    else:
        parser.print_help()
