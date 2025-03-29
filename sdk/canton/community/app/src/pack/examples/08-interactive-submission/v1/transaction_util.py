# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Implements the transaction hashing specification defined in the README.md at https://github.com/digital-asset/canton/blob/main/community/ledger-api/src/release-line-3.2/protobuf/com/daml/ledger/api/v2/interactive/README.md

import com.daml.ledger.api.v2.interactive.interactive_submission_service_pb2 as interactive_submission_service_pb2
from google.protobuf.json_format import MessageToJson
import argparse
import hashlib
import struct
import base64


def encode_bool(value):
    return b"\x01" if value else b"\x00"


def encode_int32(value):
    return struct.pack(">i", value)


def encode_int64(value):
    return struct.pack(">q", value)


def encode_string(value):
    utf8_bytes = value.encode("utf-8")
    return encode_bytes(utf8_bytes)


def encode_bytes(value):
    length = encode_int32(len(value))
    return length + value


# Like encode_bytes but without the length prefix, as hashes have a fixed size
def encode_hash(value):
    return value


def encode_hex_string(value):
    return encode_bytes(bytes.fromhex(value))


def encode_optional(value, encode_fn):
    if value is not None:
        return b"\x01" + encode_fn(value)
    else:
        return b"\x00"


def encode_proto_optional(parent_value, field_name, value, encode_fn):
    if parent_value.HasField(field_name):
        return b"\x01" + encode_fn(value)
    else:
        return b"\x00"


def encode_repeated(values, encode_fn):
    length = encode_int32(len(values))
    encoded_values = b"".join(encode_fn(v) for v in values)
    return length + encoded_values


def sha256(data):
    return hashlib.sha256(data).digest()


def find_seed(
    node_id, node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed]
):
    for node_seed in node_seeds:
        if str(node_seed.node_id) == node_id:
            return node_seed.seed
    return None


def encode_prepared_transaction(
    prepared_transaction: interactive_submission_service_pb2.PreparedTransaction,
    nodes_dict: dict,
):
    transaction_hash = hash_transaction(prepared_transaction.transaction, nodes_dict)
    metadata_hash = hash_metadata(prepared_transaction.metadata)
    return sha256(b"\x00\x00\x00\x30" + transaction_hash + metadata_hash)


def hash_transaction(
    transaction: interactive_submission_service_pb2.DamlTransaction, nodes_dict: dict
):
    encoded_transaction = encode_transaction(
        transaction, nodes_dict, transaction.node_seeds
    )
    return sha256(b"\x00\x00\x00\x30" + encoded_transaction)


def encode_transaction(
    transaction,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    version = encode_string(transaction.version)
    roots = encode_repeated(transaction.roots, encode_node_id(nodes_dict, node_seeds))
    return version + roots


def encode_identifier(identifier):
    return (
        encode_string(identifier.package_id)
        + encode_repeated(identifier.module_name.split("."), encode_string)
        + encode_repeated(identifier.entity_name.split("."), encode_string)
    )


def encode_node_id(
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    def encode(node_id):
        node = nodes_dict[node_id]
        return sha256(encode_node(node, nodes_dict, node_seeds))

    return encode


def encode_node(
    node,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    node_id = node.node_id
    if node.HasField("v1"):
        return encode_node_v1(node.v1, node_id, nodes_dict, node_seeds)
    raise ValueError("Unsupported node version")


def encode_node_v1(
    node,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    if node.HasField("create"):
        return encode_create_node(node.create, node_id, node_seeds)
    elif node.HasField("exercise"):
        return encode_exercise_node(node.exercise, node_id, nodes_dict, node_seeds)
    elif node.HasField("fetch"):
        return encode_fetch_node(node.fetch, node_id)
    elif node.HasField("rollback"):
        return encode_rollback_node(node.rollback, node_id, nodes_dict, node_seeds)
    raise ValueError("Unsupported node type")


def encode_create_node(
    create,
    node_id,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        b"\x01"
        + encode_string(create.lf_version)
        + b"\x00"
        + encode_optional(find_seed(node_id, node_seeds), encode_hash)
        + encode_hex_string(create.contract_id)
        + encode_string(create.package_name)
        + encode_identifier(create.template_id)
        + encode_value(create.argument)
        + encode_repeated(create.signatories, encode_string)
        + encode_repeated(create.stakeholders, encode_string)
    )


def encode_exercise_node(
    exercise,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        b"\x01"
        + encode_string(exercise.lf_version)
        + b"\x01"
        + encode_hash(find_seed(node_id, node_seeds))
        + encode_hex_string(exercise.contract_id)
        + encode_string(exercise.package_name)
        + encode_identifier(exercise.template_id)
        + encode_repeated(exercise.signatories, encode_string)
        + encode_repeated(exercise.stakeholders, encode_string)
        + encode_repeated(exercise.acting_parties, encode_string)
        + encode_proto_optional(
            exercise, "interface_id", exercise.interface_id, encode_identifier
        )
        + encode_string(exercise.choice_id)
        + encode_value(exercise.chosen_value)
        + encode_bool(exercise.consuming)
        + encode_proto_optional(
            exercise, "exercise_result", exercise.exercise_result, encode_value
        )
        + encode_repeated(exercise.choice_observers, encode_string)
        + encode_repeated(exercise.children, encode_node_id(nodes_dict, node_seeds))
    )


def encode_fetch_node(fetch, node_id):
    return (
        b"\x01"
        + encode_string(fetch.lf_version)
        + b"\x02"
        + encode_hex_string(fetch.contract_id)
        + encode_string(fetch.package_name)
        + encode_identifier(fetch.template_id)
        + encode_repeated(fetch.signatories, encode_string)
        + encode_repeated(fetch.stakeholders, encode_string)
        + encode_repeated(fetch.acting_parties, encode_string)
    )


def encode_rollback_node(
    rollback,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        b"\x01"
        + b"\x03"
        + encode_repeated(rollback.children, encode_node_id(nodes_dict, node_seeds))
    )


def hash_metadata(metadata):
    encoded_metadata = encode_metadata(metadata)
    return sha256(b"\x00\x00\x00\x30" + encoded_metadata)


def encode_metadata(metadata):
    return (
        b"\x01"
        + encode_repeated(metadata.submitter_info.act_as, encode_string)
        + encode_string(metadata.submitter_info.command_id)
        + encode_string(metadata.transaction_uuid)
        + encode_int32(metadata.mediator_group)
        + encode_string(metadata.synchronizer_id)
        + encode_proto_optional(
            metadata,
            "ledger_effective_time",
            metadata.ledger_effective_time,
            encode_int64,
        )
        + encode_int64(metadata.submission_time)
        + encode_repeated(metadata.input_contracts, encode_input_contract)
    )


def encode_input_contract(contract):
    return encode_int64(contract.created_at) + sha256(
        encode_create_node(contract.v1, "unused_node_id", [])
    )


def encode_value(value):
    if value.HasField("unit"):
        return b"\x00"
    elif value.HasField("bool"):
        return b"\x01" + encode_bool(value.bool)
    elif value.HasField("int64"):
        return b"\x02" + encode_int64(value.int64)
    elif value.HasField("numeric"):
        return b"\x03" + encode_string(value.numeric)
    elif value.HasField("timestamp"):
        return b"\x04" + encode_int64(value.timestamp)
    elif value.HasField("date"):
        return b"\x05" + encode_int32(value.date)
    elif value.HasField("party"):
        return b"\x06" + encode_string(value.party)
    elif value.HasField("text"):
        return b"\x07" + encode_string(value.text)
    elif value.HasField("contract_id"):
        return b"\x08" + encode_hex_string(value.contract_id)
    elif value.HasField("optional"):
        return b"\x09" + encode_proto_optional(
            value.optional, "value", value.optional.value, encode_value
        )
    elif value.HasField("list"):
        return b"\x0a" + encode_repeated(value.list.elements, encode_value)
    elif value.HasField("text_map"):
        return b"\x0b" + encode_repeated(value.text_map.entries, encode_text_map_entry)
    elif value.HasField("record"):
        return (
            b"\x0c"
            + encode_proto_optional(
                value.record, "record_id", value.record.record_id, encode_identifier
            )
            + encode_repeated(value.record.fields, encode_record_field)
        )
    elif value.HasField("variant"):
        return (
            b"\x0d"
            + encode_proto_optional(
                value.variant, "variant_id", value.variant.variant_id, encode_identifier
            )
            + encode_string(value.variant.constructor)
            + encode_value(value.variant.value)
        )
    elif value.HasField("enum"):
        return (
            b"\x0e"
            + encode_proto_optional(
                value.enum, "enum_id", value.enum.enum_id, encode_identifier
            )
            + encode_string(value.enum.constructor)
        )
    elif value.HasField("gen_map"):
        return b"\x0f" + encode_repeated(value.gen_map.entries, encode_gen_map_entry)
    raise ValueError("Unsupported value type")


def encode_text_map_entry(entry):
    return encode_string(entry.key) + encode_value(entry.value)


def encode_record_field(field):
    return encode_optional(field.label, encode_string) + encode_value(field.value)


def encode_gen_map_entry(entry):
    return encode_value(entry.key) + encode_value(entry.value)


def create_nodes_dict(prepared_transaction):
    nodes_dict = {}
    for node in prepared_transaction.transaction.nodes:
        nodes_dict[node.node_id] = node
    return nodes_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transaction utility",
        epilog="""
        Examples:
          python transaction_util.py --decode transaction.bin
          python transaction_util.py --decode --base64 <base64 encoded prepared transaction>
          python transaction_util.py --hash transaction.bin
          python transaction_util.py --hash --base64 <base64 encoded prepared transaction>
        """,
    )
    parser.add_argument(
        "--decode",
        "-d",
        help="Decode Transaction and print in JSON format",
        action="store_true",
    )
    parser.add_argument("--hash", help="Hash Transaction", action="store_true")
    parser.add_argument(
        "--base64",
        "-b",
        help="Expect the input as a base64 encoded string instead of a file",
        action="store_true",
    )
    parser.add_argument("input")

    args = parser.parse_args()

    input_arg = args.input

    prepared_transaction = interactive_submission_service_pb2.PreparedTransaction()
    if args.base64:
        prepared_transaction.ParseFromString(base64.b64decode(input_arg))
    else:
        with open(input_arg, "rb") as f:
            prepared_transaction.ParseFromString(f.read())

    if args.decode:
        print(MessageToJson(prepared_transaction))
    elif args.hash:
        encoded_hash = encode_prepared_transaction(
            prepared_transaction, create_nodes_dict(prepared_transaction)
        )
        print(encoded_hash.hex())
    else:
        parser.print_help()
