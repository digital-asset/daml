# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Implements the transaction hashing specification defined in the README.md at https://github.com/digital-asset/canton/blob/main/community/ledger-api/src/release-line-3.2/protobuf/com/daml/ledger/api/v2/interactive/README.md

import com.daml.ledger.api.v2.interactive.interactive_submission_service_pb2 as interactive_submission_service_pb2
from google.protobuf.json_format import MessageToJson
import argparse
import base64
from daml_transaction_hashing_v2 import encode_prepared_transaction, create_nodes_dict

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transaction utility",
        epilog="""
        Examples:
          python daml_transaction_util.py --decode transaction.bin
          python daml_transaction_util.py --decode --base64 <base64 encoded prepared transaction>
          python daml_transaction_util.py --hash transaction.bin
          python daml_transaction_util.py --hash --base64 <base64 encoded prepared transaction>
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
