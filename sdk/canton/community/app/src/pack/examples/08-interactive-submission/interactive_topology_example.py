# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Simple example of an interactive submission demonstrating the external signing flow

# [Imports]
import time
import argparse
import grpc
from interactive_topology_util import *
import os
import json
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from grpc import Channel

from com.digitalasset.canton.crypto.v30 import crypto_pb2

# [Imports End]

# Path to the Canton ports file - This file is created when canton starts using the configuration in this folder
# and stores the ports for the Ledger and Admin API.
json_file_path = "canton_ports.json"
# Load the JSON content from the file
# Try to load the JSON file, if it doesn't exist, fallback to defaults
default_admin_port = 0
try:
    with open(json_file_path, "r") as f:
        config = json.load(f)
    # Default port values from JSON if available
    default_admin_port = config.get("participant1", {}).get("adminApi")
except FileNotFoundError:
    print(f"{json_file_path} not found. Using default port values.")
except json.JSONDecodeError:
    print(f"Failed to decode {json_file_path}. Using default port values.")

# Get ports from environment variables, fallback to default values (from JSON) if not set
admin_port = os.environ.get("CANTON_ADMIN_PORT", default_admin_port)

# [Create Admin API gRPC Channel]
admin_channel = grpc.insecure_channel(f"localhost:{admin_port}")
# [Created Admin API gRPC Channel]


# Submit a namespace topology transaction
def run_demo(
    channel: Channel,
    synchronizer_id: str,
) -> (EllipticCurvePrivateKey, str):
    # [start generate keys]
    private_key = ec.generate_private_key(curve=ec.SECP256R1())
    public_key = private_key.public_key()
    # [end generate keys]

    # [start compute fingerprint]
    public_key_bytes: bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_key_fingerprint = compute_fingerprint(public_key_bytes)
    # [end compute fingerprint]

    # [start build mapping]
    mapping = build_namespace_mapping(
        public_key_fingerprint,
        public_key_bytes,
        crypto_pb2.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER,
        crypto_pb2.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256,
    )
    # [end build mapping]
    # [start build serialized versioned transaction]
    serialized_versioned_topology_transaction = serialize_topology_transaction(mapping)
    # [end build serialized versioned transaction]
    # [start compute transaction hash]
    transaction_hash = compute_topology_transaction_hash(
        serialized_versioned_topology_transaction
    )
    # [end compute transaction hash]
    # [start sign hash]
    signature = sign_hash(private_key, transaction_hash)
    # [end sign hash]
    # [start submit request]
    canton_signature = build_canton_signature(
        signature,
        public_key_fingerprint,
        crypto_pb2.SignatureFormat.SIGNATURE_FORMAT_DER,
        crypto_pb2.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256,
    )
    signed_transaction = build_signed_transaction(
        serialized_versioned_topology_transaction,
        [canton_signature],
    )
    submit_signed_transactions(channel, [signed_transaction], synchronizer_id)
    print(f"Transaction submitted successfully")
    # [end submit request]

    # [start observe transaction]
    # Topology transaction submission is asynchronous, so we may need to wait a bit before observing the delegation in the topology state
    namespace_delegation_response = None
    while True:
        namespace_delegation_response = list_namespace_delegation(
            channel, synchronizer_id, public_key_fingerprint
        )
        if namespace_delegation_response.results:
            print("Namespace delegation is now active")
            break
        time.sleep(1)  # Wait for 1 second before retrying
    # [end observe transaction]


def read_id_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interactive topology utility")
    parser.add_argument(
        "--synchronizer-id",
        type=str,
        help="Synchronizer ID the topology transaction should be targeted to",
        default=read_id_from_file("synchronizer_id"),
    )

    subparsers = parser.add_subparsers(required=True, dest="subcommand")
    parser_run_demo = subparsers.add_parser(
        "run-demo", help="Run the namespace delegation demo"
    )

    args = parser.parse_args()

    if args.subcommand == "run-demo":
        run_demo(admin_channel, args.synchronizer_id)
    else:
        parser.print_help()
