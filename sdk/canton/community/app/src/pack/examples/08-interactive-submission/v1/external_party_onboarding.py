# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
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
from com.digitalasset.canton.topology.admin.v30 import topology_manager_read_service_pb2
from com.digitalasset.canton.protocol.v30 import topology_pb2
from com.digitalasset.canton.crypto.v30 import crypto_pb2
import hashlib


# Computes a canton compatible hash using sha256
# purpose: Canton prefixes all hashes with a hash purpose
# content: payload to be hashed
def compute_sha256_canton_hash(purpose: int, content: bytes):
    hash_purpose = (purpose).to_bytes(4, byteorder="big")
    # Hashed public key
    hashed_public_key = hashlib.sha256(hash_purpose + content).digest()

    # Multi-hash encoding
    # Canton uses an implementation of multihash (https://github.com/multiformats/multihash)
    # Since we use sha256 always here, we can just hardcode the prefixes
    # This may be improved and simplified in subsequent versions
    sha256_algorithm_prefix = bytes([0x12])
    sha256_length_prefix = bytes([32])
    return sha256_algorithm_prefix + sha256_length_prefix + hashed_public_key


# Computes the fingerprint of a public key by hashing it and adding some Canton specific data
def compute_fingerprint(public_key_bytes: bytes) -> str:
    # 12 is the hash purpose for public keys
    return compute_sha256_canton_hash(12, public_key_bytes).hex()


# Sign a topology transaction with the provided key
def sign_topology_transaction(
    generated_transaction: topology_manager_write_service_pb2.GenerateTransactionsResponse.GeneratedTransaction,
    is_proposal: bool,
    private_key: EllipticCurvePrivateKey,
    public_key_fingerprint: str,
):
    signature = private_key.sign(
        data=generated_transaction.transaction_hash,
        signature_algorithm=ec.ECDSA(hashes.SHA256()),
    )
    canton_signature = crypto_pb2.Signature(
        format=crypto_pb2.SignatureFormat.SIGNATURE_FORMAT_RAW,
        signature=signature,
        signed_by=public_key_fingerprint,
        signing_algorithm_spec=crypto_pb2.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256,
    )
    return topology_pb2.SignedTopologyTransaction(
        transaction=generated_transaction.serialized_transaction,
        signatures=[canton_signature],
        proposal=is_proposal,
    )


# Utility method to generate and sign a topology transaction from a topology mapping
def generate_and_sign_topology_transaction(
    mapping: topology_pb2.TopologyMapping,
    private_key: EllipticCurvePrivateKey,
    public_key_fingerprint: str,
    topology_client: topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub,
) -> topology_pb2.SignedTopologyTransaction:
    proposal = topology_manager_write_service_pb2.GenerateTransactionsRequest.Proposal(
        operation=topology_pb2.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
        serial=1,
        mapping=mapping,
        store="Authorized",
    )

    # Generate the topology transaction from the proposal
    namespace_transaction_request = (
        topology_manager_write_service_pb2.GenerateTransactionsRequest(
            proposals=[proposal]
        )
    )
    generate_transactions_response: (
        topology_manager_write_service_pb2.GenerateTransactionsResponse
    ) = topology_client.GenerateTransactions(namespace_transaction_request)

    # Sign the transaction with the party's private key
    return sign_topology_transaction(
        generate_transactions_response.generated_transactions[0],
        False,
        private_key,
        public_key_fingerprint,
    )


# Namespace delegation: this registers a root namespace with the public key of the party onto the network
# effectively creating the party.
def create_signed_namespace_transaction(
    private_key: EllipticCurvePrivateKey,
    signing_public_key: crypto_pb2.SigningPublicKey,
    public_key_fingerprint: str,
    topology_client: topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub,
):
    namespace_delegation_mapping = topology_pb2.TopologyMapping(
        namespace_delegation=topology_pb2.NamespaceDelegation(
            namespace=public_key_fingerprint,
            target_key=signing_public_key,
            is_root_delegation=True,
        )
    )

    return generate_and_sign_topology_transaction(
        namespace_delegation_mapping,
        private_key,
        public_key_fingerprint,
        topology_client,
    )


# Party to key: This registers the public key as the one that will be used to sign and authorize daml transactions submitted
# to the ledger via the interactive submission service
def create_signed_party_to_key_transaction(
    party_id: str,
    private_key: EllipticCurvePrivateKey,
    signing_public_key: crypto_pb2.SigningPublicKey,
    public_key_fingerprint: str,
    topology_client: topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub,
):
    party_to_key_mapping = topology_pb2.TopologyMapping(
        party_to_key_mapping=topology_pb2.PartyToKeyMapping(
            party=party_id,
            signing_keys=[signing_public_key],
            threshold=1,
        )
    )

    return generate_and_sign_topology_transaction(
        party_to_key_mapping, private_key, public_key_fingerprint, topology_client
    )


# Party to participant: This records the fact that the party wants to be hosted on the participant with confirmation rights
# This means this participant is not allowed to submit transactions on behalf of this party but will validate transactions
# on behalf of the party by confirming or rejecting them according to the ledger model. It also records transaction for that party on the ledger.
def create_signed_party_to_participant_transaction(
    party_id: str,
    confirming_participant_id: str,
    private_key: EllipticCurvePrivateKey,
    public_key_fingerprint: str,
    topology_client: topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub,
):
    party_to_participant_mapping = topology_pb2.TopologyMapping(
        party_to_participant=topology_pb2.PartyToParticipant(
            party=party_id,
            threshold=1,
            participants=[
                topology_pb2.PartyToParticipant.HostingParticipant(
                    participant_uid=confirming_participant_id,
                    permission=topology_pb2.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
                )
            ],
        )
    )

    return generate_and_sign_topology_transaction(
        party_to_participant_mapping,
        private_key,
        public_key_fingerprint,
        topology_client,
    )


# Onboard a new external party
def onboard_external_party(
    party_name: str,
    confirming_participant_id: str,
    synchronizer_id: str,
    channel: Channel,
) -> (EllipticCurvePrivateKey, str):
    print(f"Onboarding {party_name}")

    # Generate a real public/private key pair
    # For the sake of simplicity in the demo, we use a single signing key pair for the party namespace (used to manage the party itself on the network),
    # and for the signing of transactions via the interactive submission service. We however recommend to use different keys in real world deployment for better security.
    private_key = ec.generate_private_key(curve=ec.SECP256R1())
    public_key = private_key.public_key()

    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    public_key_fingerprint = compute_fingerprint(public_key_bytes)
    party_id = party_name + "::" + public_key_fingerprint

    # Client for the Admin API
    topology_write_client = (
        topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub(channel)
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )

    signing_public_key = crypto_pb2.SigningPublicKey(
        format=crypto_pb2.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER,
        public_key=public_key_bytes,
        scheme=crypto_pb2.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256,
        usage=[
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
        ],
        key_spec=crypto_pb2.SIGNING_KEY_SPEC_EC_P256,
    )

    # The onboarding consists of 3 topology transactions:
    namespace_transaction = create_signed_namespace_transaction(
        private_key, signing_public_key, public_key_fingerprint, topology_write_client
    )
    party_to_key_transaction = create_signed_party_to_key_transaction(
        party_id,
        private_key,
        signing_public_key,
        public_key_fingerprint,
        topology_write_client,
    )
    party_to_participant_transaction = create_signed_party_to_participant_transaction(
        party_id,
        confirming_participant_id,
        private_key,
        public_key_fingerprint,
        topology_write_client,
    )

    # Additionally, the party to participant transaction needs to be signed by the participant as well, thereby agreeing to host that party
    sign_transaction_request = (
        topology_manager_write_service_pb2.SignTransactionsRequest(
            transactions=[party_to_participant_transaction],
            store="Authorized",
        )
    )
    fully_signed_party_to_participant_transaction: (
        topology_manager_write_service_pb2.SignTransactionsResponse
    ) = topology_write_client.SignTransactions(sign_transaction_request).transactions[0]

    add_transactions_request = (
        topology_manager_write_service_pb2.AddTransactionsRequest(
            transactions=[
                namespace_transaction,
                party_to_key_transaction,
                fully_signed_party_to_participant_transaction,
            ],
            store="Authorized",
        )
    )
    topology_write_client.AddTransactions(add_transactions_request)

    print(f"Waiting for {party_name} to appear in topology")
    party_in_topology = False
    while not party_in_topology:
        party_to_participant_response: (
            topology_manager_read_service_pb2.ListPartyToParticipantResponse
        ) = topology_read_client.ListPartyToParticipant(
            topology_manager_read_service_pb2.ListPartyToParticipantRequest(
                base_query=topology_manager_read_service_pb2.BaseQuery(
                    filter_store=topology_manager_read_service_pb2.Store(
                        synchronizer=topology_manager_read_service_pb2.Store.Synchronizer(
                            id=synchronizer_id,
                        )
                    ),
                    head_state=google.protobuf.empty_pb2.Empty(),
                ),
                filter_party=party_id,
                filter_participant=confirming_participant_id,
            )
        )
        if len(party_to_participant_response.results) > 0:
            break
        else:
            time.sleep(0.5)
            continue

    return private_key, public_key_fingerprint
