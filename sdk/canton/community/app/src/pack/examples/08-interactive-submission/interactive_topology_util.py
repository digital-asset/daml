# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# [Imports start]
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from grpc import Channel

from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_write_service_pb2_grpc,
    topology_manager_read_service_pb2_grpc,
)
from com.digitalasset.canton.topology.admin.v30 import (
    topology_manager_write_service_pb2,
    topology_manager_read_service_pb2,
    common_pb2,
)
from com.digitalasset.canton.protocol.v30 import topology_pb2
from com.digitalasset.canton.version.v1 import untyped_versioned_message_pb2
from com.digitalasset.canton.crypto.v30 import crypto_pb2
from google.rpc import status_pb2, error_details_pb2
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToJson
import hashlib
import grpc


# [Imports end]
def handle_grpc_error(func):
    """
    Decorator to handle gRPC errors and print detailed error information.

    Args:
        func (function): The gRPC function to be wrapped.

    Returns:
        function: Wrapped function with error handling.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            print("gRPC error occurred:")
            grpc_metadata: grpc.aio.Metadata = grpc.aio.Metadata.from_tuple(
                e.trailing_metadata()
            )
            metadata = grpc_metadata.get("grpc-status-details-bin")
            if metadata is None:
                raise
            status: status_pb2.Status = status_pb2.Status.FromString(metadata)
            for detail in status.details:
                if detail.type_url == "type.googleapis.com/google.rpc.ErrorInfo":
                    error: error_details_pb2.ErrorInfo = (
                        error_details_pb2.ErrorInfo.FromString(detail.value)
                    )
                    print(MessageToJson(error))
                else:
                    print(MessageToJson(detail))
            raise

    return wrapper


# Computes a canton compatible hash using sha256
# purpose: Canton prefixes content with a hash purpose
# https://github.com/digital-asset/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
# content: payload to be hashed
def compute_sha256_canton_hash(purpose: int, content: bytes):
    hash_purpose = purpose.to_bytes(4, byteorder="big")
    # Hashed content
    hashed_content = hashlib.sha256(hash_purpose + content).digest()

    # Multi-hash encoding
    # Canton uses an implementation of multihash (https://github.com/multiformats/multihash)
    # Since we use sha256 always here, we can just hardcode the prefixes
    # This may be improved and simplified in subsequent versions
    sha256_algorithm_prefix = bytes([0x12])
    sha256_length_prefix = bytes([0x20])
    return sha256_algorithm_prefix + sha256_length_prefix + hashed_content


# Computes the fingerprint of a public key by hashing it and adding some Canton specific data
def compute_fingerprint(public_key_bytes: bytes) -> str:
    """
    Computes the fingerprint of a public signing key.

    Args:
        public_key_bytes (bytes): The serialized transaction data.

    Returns:
        str: The computed fingerprint in hexadecimal format.
    """
    # 12 is the hash purpose for public key fingerprints
    # https://github.com/digital-asset/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
    return compute_sha256_canton_hash(12, public_key_bytes).hex()


def compute_topology_transaction_hash(serialized_versioned_transaction: bytes) -> bytes:
    """
    Computes the hash of a serialized topology transaction.

    Args:
        serialized_versioned_transaction (bytes): The serialized transaction data.

    Returns:
        bytes: The computed hash.
    """
    # 11 is the hash purpose for topology transaction signatures
    # https://github.com/digital-asset/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
    return compute_sha256_canton_hash(11, serialized_versioned_transaction)


def compute_multi_transaction_hash(hashes: [bytes]) -> bytes:
    """
    Computes a combined hash for multiple topology transactions.

    This function sorts the given hashes, concatenates them with length encoding,
    and computes a Canton-specific SHA-256 hash with a predefined purpose.

    Args:
        hashes (list[bytes]): A list of hashes representing individual topology transactions.

    Returns:
        bytes: The computed multi-transaction hash.
    """
    # Sort the hashes by their hex representation
    sorted_hashes = sorted(hashes, key=lambda h: h.hex())

    # Start with the number of hashes encoded as a 4 bytes integer in big endian
    combined_hashes = len(sorted_hashes).to_bytes(4, byteorder="big")

    # Concatenate each hash, prefixing them with their size as a 4 bytes integer in big endian
    for h in sorted_hashes:
        combined_hashes += len(h).to_bytes(4, byteorder="big") + h

    # 55 is the hash purpose for multi topology transaction hashes
    return compute_sha256_canton_hash(55, combined_hashes)


def sign_hash(
    private_key: EllipticCurvePrivateKey,
    data: bytes,
):
    """
    Signs the given data using an elliptic curve private key.

    Args:
        private_key (EllipticCurvePrivateKey): The private key used for signing.
        data (bytes): The data to be signed.

    Returns:
        bytes: The generated signature.
    """
    return private_key.sign(
        data=data,
        signature_algorithm=ec.ECDSA(hashes.SHA256()),
    )


def build_add_transaction_request(
    signed_transactions: [topology_pb2.SignedTopologyTransaction],
    synchronizer_id: str,
):
    """
    Builds an AddTransactionsRequest for the topology API.

    Args:
        signed_transactions (list[topology_pb2.SignedTopologyTransaction]): List of signed transactions.
        synchronizer_id (str): The synchronizer ID for the transaction.

    Returns:
        topology_manager_write_service_pb2.AddTransactionsRequest: The request object.
    """
    return topology_manager_write_service_pb2.AddTransactionsRequest(
        transactions=signed_transactions,
        store=common_pb2.StoreId(
            synchronizer=common_pb2.StoreId.Synchronizer(
                id=synchronizer_id,
            )
        ),
    )


def build_canton_signature(
    signature: bytes,
    signed_by: str,
    format: crypto_pb2.SignatureFormat,
    spec: crypto_pb2.SigningAlgorithmSpec,
):
    """
    Builds a Canton-compatible digital signature.

    Args:
        signature (bytes): The cryptographic signature bytes.
        signed_by (str): The identifier of the entity that signed the data.
        format (crypto_pb2.SignatureFormat): The format of the signature.
        spec (crypto_pb2.SigningAlgorithmSpec): The signing algorithm specification.

    Returns:
        crypto_pb2.Signature: A protocol buffer representation of the Canton signature.
    """
    return crypto_pb2.Signature(
        format=format,
        signature=signature,
        signed_by=signed_by,
        signing_algorithm_spec=spec,
    )


def build_signed_transaction(
    serialized_versioned_transaction: bytes,
    signatures: [crypto_pb2.Signature],
):
    """
    Builds a signed topology transaction.

    Args:
        serialized_versioned_transaction (bytes): Serialized topology transaction.
        signatures (list[crypto_pb2.Signature]): List of cryptographic signatures.

    Returns:
        topology_pb2.SignedTopologyTransaction: The signed transaction.
    """
    return topology_pb2.SignedTopologyTransaction(
        transaction=serialized_versioned_transaction,
        signatures=signatures,
    )


def build_namespace_mapping(
    public_key_fingerprint: str,
    public_key_bytes: bytes,
    key_format: crypto_pb2.CryptoKeyFormat,
    key_scheme: crypto_pb2.SigningKeyScheme,
):
    """
    Constructs a topology mapping for namespace delegation.

    Args:
        public_key_fingerprint (str): The fingerprint of the public key.
        public_key_bytes (bytes): The raw bytes of the public key.
        key_format (crypto_pb2.CryptoKeyFormat): The format of the public key.
        key_scheme (crypto_pb2.SigningKeyScheme): The signing scheme of the key.

    Returns:
        topology_pb2.TopologyMapping: A topology mapping for namespace delegation.
    """
    return topology_pb2.TopologyMapping(
        namespace_delegation=topology_pb2.NamespaceDelegation(
            namespace=public_key_fingerprint,
            target_key=crypto_pb2.SigningPublicKey(
                # Must match the format to which the key was exported
                format=key_format,
                public_key=public_key_bytes,
                # Must match the scheme of the key
                scheme=key_scheme,
                # Keys in NamespaceDelegation are used only for namespace operations
                usage=[
                    crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
                ],
            ),
            is_root_delegation=True,
        )
    )


def build_topology_transaction(
    mapping: topology_pb2.TopologyMapping,
    serial: int = 1,
):
    """
    Builds a topology transaction.

    Args:
        mapping (topology_pb2.TopologyMapping): The topology mapping to include in the transaction.
        serial (int): The serial of the topology transaction. Defaults to 1.

    Returns:
        topology_pb2.TopologyTransaction: The topology transaction object.
    """
    return topology_pb2.TopologyTransaction(
        mapping=mapping,
        operation=topology_pb2.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
        serial=serial,
    )


def build_versioned_transaction(
    data: bytes,
):
    """
    Builds a versioned transaction wrapper for the given data.

    Args:
        data (bytes): Serialized transaction data.

    Returns:
        untyped_versioned_message_pb2.UntypedVersionedMessage: The versioned transaction object.
    """
    return untyped_versioned_message_pb2.UntypedVersionedMessage(
        data=data,
        version=30,
    )


def serialize_topology_transaction(
    mapping: topology_pb2.TopologyMapping,
    serial: int = 1,
):
    """
    Serializes a topology transaction.

    Args:
        mapping (topology_pb2.TopologyMapping): The topology mapping to serialize.
        serial (int): The serial of the topology transaction. Defaults to 1.

    Returns:
        bytes: The serialized topology transaction.
    """
    topology_transaction = build_topology_transaction(mapping, serial)
    versioned_topology_transaction = build_versioned_transaction(
        topology_transaction.SerializeToString()
    )
    return versioned_topology_transaction.SerializeToString()


@handle_grpc_error
def submit_signed_transactions(
    channel: Channel,
    signed_transactions: [topology_pb2.SignedTopologyTransaction],
    synchronizer_id: str,
) -> (EllipticCurvePrivateKey, str):
    """
    Submits signed topology transactions to the Canton topology API.

    Args:
        channel (Channel): The gRPC channel used to communicate with the topology service.
        signed_transactions (list[topology_pb2.SignedTopologyTransaction]):
            A list of signed topology transactions to be submitted.
        synchronizer_id (str): The identifier of the synchronizer to target.

    Raises:
        grpc.RpcError: If there is an issue communicating with the topology API.
    """
    add_transactions_request = build_add_transaction_request(
        signed_transactions,
        synchronizer_id,
    )
    topology_write_client = (
        topology_manager_write_service_pb2_grpc.TopologyManagerWriteServiceStub(channel)
    )
    topology_write_client.AddTransactions(add_transactions_request)


@handle_grpc_error
def list_namespace_delegation(
    channel: Channel,
    synchronizer_id: str,
    fingerprint: str,
):
    """
    Retrieves namespace delegations from the topology API.

    Args:
        channel (Channel): The gRPC channel used to communicate with the topology service.
        synchronizer_id (str): The identifier of the synchronizer managing the namespace.
        fingerprint (str): The fingerprint of the public key associated with the namespace.

    Returns:
        topology_manager_read_service_pb2.ListNamespaceDelegationResponse:
            The response containing the list of namespace delegations.

    Raises:
        grpc.RpcError: If there is an issue communicating with the topology API.
    """
    list_namespace_delegation_request = (
        topology_manager_read_service_pb2.ListNamespaceDelegationRequest(
            base_query=topology_manager_read_service_pb2.BaseQuery(
                store=common_pb2.StoreId(
                    synchronizer=common_pb2.StoreId.Synchronizer(id=synchronizer_id)
                ),
                head_state=empty_pb2.Empty(),
            ),
            filter_namespace=fingerprint,
        )
    )
    topology_read_client = (
        topology_manager_read_service_pb2_grpc.TopologyManagerReadServiceStub(channel)
    )
    return topology_read_client.ListNamespaceDelegation(
        list_namespace_delegation_request
    )
