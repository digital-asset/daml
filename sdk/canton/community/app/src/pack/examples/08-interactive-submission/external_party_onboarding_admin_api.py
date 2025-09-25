# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# [Imports start]
import time

import grpc
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from grpc import Channel

import google.protobuf.empty_pb2
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
from com.digitalasset.canton.protocol.v30 import topology_pb2
from com.digitalasset.canton.crypto.v30 import crypto_pb2
from google.protobuf import empty_pb2
from interactive_topology_util import (
    compute_fingerprint,
    compute_sha256_canton_hash,
    serialize_topology_transaction,
    compute_multi_transaction_hash,
    sign_hash,
    compute_topology_transaction_hash,
)

# [Imports end]


def build_signed_topology_transaction(
    transaction: bytes,
    hashes: [bytes],
    signature: bytes,
    signed_by: str,
    proposal: bool = False,
):
    """
    Builds a signed topology transaction, optionally including multi-transaction signatures.

    Args:
        transaction (bytes): The raw bytes representing the transaction to be signed.
        hashes (list[bytes]): A list of transaction hashes for the multi-transaction signature.
        signature (bytes): The signature for the transaction.
        signed_by (str): The identifier of the entity signing the transaction.
        proposal (bool, optional): A flag indicating if this transaction is part of a proposal. Defaults to False.

    Returns:
        topology_pb2.SignedTopologyTransaction
    """
    return topology_pb2.SignedTopologyTransaction(
        transaction=transaction,
        # Not set because we use the multi transactions signature
        signatures=[],
        multi_transaction_signatures=[
            topology_pb2.MultiTransactionSignatures(
                transaction_hashes=hashes,
                signatures=[
                    crypto_pb2.Signature(
                        format=crypto_pb2.SignatureFormat.SIGNATURE_FORMAT_RAW,
                        signature=signature,
                        signed_by=signed_by,
                        signing_algorithm_spec=crypto_pb2.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256,
                    )
                ],
            )
        ],
        proposal=proposal,
    )


def build_serialized_transaction_and_hash(
    mapping: topology_pb2.TopologyMapping,
) -> (bytes, bytes):
    """
    Generates a serialized topology transaction and its corresponding hash.

    Args:
        mapping (topology_pb2.TopologyMapping): The topology mapping to be serialized.

    Returns:
        tuple: A tuple containing:
            - bytes: The serialized transaction.
            - bytes: The SHA-256 hash of the serialized transaction.
    """
    transaction = serialize_topology_transaction(mapping)
    transaction_hash = compute_sha256_canton_hash(11, transaction)
    return transaction, transaction_hash


# Onboard a new external party
def onboard_external_party(
    party_name: str,
    confirming_participant_ids: [str],
    confirming_threshold: int,
    synchronizer_id: str,
    channel: Channel,
) -> (EllipticCurvePrivateKey, str):
    """
    Onboard a new external party.
    Generates an in-memory signing key pair to authenticate the external party.

    Args:
        party_name (str): Name of the party.
        confirming_participant_ids (str): Participant IDs on which the party will be hosted for transaction confirmation.
        confirming_threshold (int): Minimum number of confirmations that must be received from the confirming participants to authorize a transaction.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        channel (grpc.Channel): gRPC channel to one of the confirming participants Admin API.

    Returns:
        tuple: A tuple containing:
            - EllipticCurvePrivateKey: Private key created for the party.
            - str: Fingerprint of the public key created for the party.
    """
    print(f"Onboarding {party_name}")

    # [Create clients for the Admin API]
    topology_write_client = (
        topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub(channel)
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )
    # [Created clients for the Admin API]

    # [Generate a public/private key pair]
    # For the sake of simplicity in the demo, we use a single signing key pair for the party namespace (used to manage the party itself on the network),
    # and for the signing of transactions via the interactive submission service. We however recommend to use different keys in real world deployment for better security.
    private_key = ec.generate_private_key(curve=ec.SECP256R1())
    public_key = private_key.public_key()

    # Extract the public key in the DER format
    public_key_bytes: bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    # Wrap the public key in a Canton protobuf message
    signing_public_key = crypto_pb2.SigningPublicKey(
        # Must match the format to which the key was exported to above
        format=crypto_pb2.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER,
        public_key=public_key_bytes,
        # Must match the scheme of the key
        scheme=crypto_pb2.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256,
        # Because we have only one key, we specify both NAMESPACE and PROTOCOL usage for it
        # When using different keys, ensure to use only the correct usage for each
        usage=[
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
        ],
        # This field is deprecated in favor of scheme but python requires us to set it
        key_spec=crypto_pb2.SIGNING_KEY_SPEC_EC_P256,
    )
    # [Generated a public/private key pair]

    # [Compute the fingerprint of the public key]
    public_key_fingerprint = compute_fingerprint(public_key_bytes)
    # [Computed the fingerprint of the public key]

    # [Construct party ID]
    # The party id is constructed with party_name :: fingerprint
    # This must be the fingerprint of the _namespace signing key_
    party_id = party_name + "::" + public_key_fingerprint
    # [Constructed party ID]

    # [Build onboarding transactions and their hash]
    # Namespace delegation: registers a root namespace with the public key of the party to the network
    # effectively creating the party.
    namespace_delegation_mapping = topology_pb2.TopologyMapping(
        namespace_delegation=topology_pb2.NamespaceDelegation(
            namespace=public_key_fingerprint,
            target_key=signing_public_key,
            is_root_delegation=True,
        )
    )
    (namespace_delegation_transaction, namespace_transaction_hash) = (
        build_serialized_transaction_and_hash(namespace_delegation_mapping)
    )

    # Party to key: registers the public key as the one that will be used to sign and authorize Daml transactions submitted
    # to the ledger via the interactive submission service
    party_to_key_transaction = build_party_to_key_transaction(
        channel, party_id, signing_public_key, synchronizer_id
    )
    party_to_key_transaction_hash = compute_topology_transaction_hash(
        party_to_key_transaction
    )

    # Party to participant: records the fact that the party wants to be hosted on the participants with confirmation rights
    # This means those participants are not allowed to submit transactions on behalf of this party but will validate transactions
    # on behalf of the party by confirming or rejecting them according to the ledger model. They also records transaction for that party on the ledger.
    confirming_participants_hosting = []
    for confirming_participant_id in confirming_participant_ids:
        confirming_participants_hosting.append(
            topology_pb2.PartyToParticipant.HostingParticipant(
                participant_uid=confirming_participant_id,
                permission=topology_pb2.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
            )
        )
    party_to_participant_mapping = topology_pb2.TopologyMapping(
        party_to_participant=topology_pb2.PartyToParticipant(
            party=party_id,
            threshold=confirming_threshold,
            participants=confirming_participants_hosting,
        )
    )
    (party_to_participant_transaction, party_to_participant_transaction_hash) = (
        build_serialized_transaction_and_hash(party_to_participant_mapping)
    )
    # [Built onboarding transactions and their hash]

    # [Compute multi hash]
    # Combine the hashes of all three transactions, so we can perform a single signature
    multi_hash = compute_multi_transaction_hash(
        [
            namespace_transaction_hash,
            party_to_key_transaction_hash,
            party_to_participant_transaction_hash,
        ]
    )
    # [Computed multi hash]

    # [Sign multi hash]
    signature = sign_hash(private_key, multi_hash)
    # [Signed multi hash]

    # [Build signed topology transactions]
    hash_list = [
        namespace_transaction_hash,
        party_to_key_transaction_hash,
        party_to_participant_transaction_hash,
    ]
    signed_namespace_transaction = build_signed_topology_transaction(
        namespace_delegation_transaction, hash_list, signature, public_key_fingerprint
    )
    signed_party_to_key_transaction = build_signed_topology_transaction(
        party_to_key_transaction, hash_list, signature, public_key_fingerprint
    )
    signed_party_to_participant_transaction = build_signed_topology_transaction(
        party_to_participant_transaction,
        hash_list,
        signature,
        public_key_fingerprint,
        True,
    )
    # [Built signed topology transactions]

    # [Load all three transactions onto the participant node]
    add_transactions_request = (
        topology_manager_write_service_pb2.AddTransactionsRequest(
            transactions=[
                signed_namespace_transaction,
                signed_party_to_key_transaction,
                signed_party_to_participant_transaction,
            ],
            store=common_pb2.StoreId(
                synchronizer=common_pb2.StoreId.Synchronizer(
                    id=synchronizer_id,
                )
            ),
        )
    )
    topology_write_client.AddTransactions(add_transactions_request)
    # [Loaded all three transactions onto the participant node]


    # [Authorize hosting from the confirming node]
    topology_write_client.Authorize(
        topology_manager_write_service_pb2.AuthorizeRequest(
            proposal=topology_manager_write_service_pb2.AuthorizeRequest.Proposal(
                change=topology_pb2.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
                serial=1,
                mapping=party_to_participant_mapping,
            ),
            # False because the authorization from the participant is not enough:
            # - it requires the signatures from the party (already submitted above)
            # - as well as signatures from any other hosting participant
            must_fully_authorize=False,
            store=common_pb2.StoreId(
                synchronizer=common_pb2.StoreId.Synchronizer(
                    id=synchronizer_id,
                ),
            ),
        )
    )
    # [Authorized hosting from the confirming node]

    # Finally wait for the party to appear in the topology, ensuring the onboarding succeeded
    print(f"Waiting for {party_name} to appear in topology")
    # [Waiting for party]
    # If there's only one confirming participant, onboarding should be complete already
    if len(confirming_participant_ids) == 1:
        wait_to_observe_party_to_participant(
            topology_read_client, synchronizer_id, party_id
        )
    # [Party found]

    return private_key, public_key_fingerprint


def wait_to_observe_party_to_participant(
    topology_read_client: topology_manager_read_service_pb2_grpc,
    synchronizer_id: str,
    party_id,
):
    party_in_topology = False
    while not party_in_topology:
        party_to_participant_response: (
            topology_manager_read_service_pb2.ListPartyToParticipantResponse
        ) = topology_read_client.ListPartyToParticipant(
            topology_manager_read_service_pb2.ListPartyToParticipantRequest(
                base_query=topology_manager_read_service_pb2.BaseQuery(
                    store=common_pb2.StoreId(
                        synchronizer=common_pb2.StoreId.Synchronizer(
                            id=synchronizer_id,
                        )
                    ),
                    head_state=google.protobuf.empty_pb2.Empty(),
                ),
                filter_party=party_id,
            )
        )
        if len(party_to_participant_response.results) > 0:
            break
        else:
            time.sleep(0.5)
            continue


def build_party_to_key_transaction(
    channel: grpc.Channel,
    party_id: str,
    new_signing_key: crypto_pb2.SigningPublicKey,
    synchronizer_id: str,
) -> bytes:
    """
    Constructs a topology transaction that updates the party-to-key mapping.

    Args:
        channel (grpc.Channel): gRPC channel for communication with the topology manager.
        party_id (str): Identifier of the party whose key mapping is being updated.
        new_signing_key (crypto_pb2.SigningPublicKey): The new signing key to be added.
        synchronizer_id (str): ID of the synchronizer to query the topology state.

    Returns:
        bytes: Serialized topology transaction containing the updated mapping.
    """
    # Retrieve the current party to key mapping
    list_party_to_key_request = (
        topology_manager_read_service_pb2.ListPartyToKeyMappingRequest(
            base_query=topology_manager_read_service_pb2.BaseQuery(
                store=common_pb2.StoreId(
                    synchronizer=common_pb2.StoreId.Synchronizer(id=synchronizer_id)
                ),
                head_state=empty_pb2.Empty(),
            ),
            filter_party=party_id,
        )
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )
    party_to_key_response: (
        topology_manager_read_service_pb2.ListPartyToKeyMappingResponse
    ) = topology_read_client.ListPartyToKeyMapping(list_party_to_key_request)
    if len(party_to_key_response.results) == 0:
        current_serial = 1
        current_keys_list = []
    else:
        # Sort the results by serial in descending order and take the first one
        sorted_results = sorted(
            party_to_key_response.results,
            key=lambda result: result.context.serial,
            reverse=True,
        )
        # Get the mapping with the highest serial and its list of hosting participants
        current_serial = sorted_results[0].context.serial
        current_keys_list: [crypto_pb2.SigningPublicKey] = sorted_results[
            0
        ].item.signing_keys

    # Create a new mapping adding the new participant to the list and incrementing the serial
    updated_mapping = topology_pb2.TopologyMapping(
        party_to_key_mapping=topology_pb2.PartyToKeyMapping(
            party=party_id,
            threshold=1,
            signing_keys=current_keys_list + [new_signing_key],
        )
    )
    # Build the serialized transaction
    return serialize_topology_transaction(updated_mapping, serial=current_serial + 1)
