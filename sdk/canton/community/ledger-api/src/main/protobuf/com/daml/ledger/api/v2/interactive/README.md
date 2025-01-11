# Deterministic Encoding of Canton Transaction

## Introduction

This document specifies the encoding algorithm used to produce a deterministic hash of a `com.daml.ledger.api.v2.interactive.PreparedTransaction`.
The resulting hash will be signed by the holder of the external party's private key. The signature
authorizes the ledger changes described by the transaction on behalf of the external party.

The specification can be implemented in any language, but certain encoding patterns are biased due to Canton being implemented in a JVM based
language and using the Java protobuf library. Those biases will be made explicit in the specification.

Because protobuf serialization is not canonical, it is not suitable for the signing of cryptographic hashes.
For that reason we must define a more precise encoding specification that can be re-implemented deterministically across languages
and provide the cryptographic guarantees we need. See https://protobuf.dev/programming-guides/serialization-not-canonical/ for more information on the topic.

## Versioning

### Hashing Scheme Version

The hashing algorithm as a whole is versioned. This enables updates to be made to accommodate changes in the underlying Daml format,
or to the way the protocol verifies signatures for instance. It is imperative that the implementation respects the specification of the version it implements.

```protobuf
enum HashingSchemeVersion {
  HASHING_SCHEME_VERSION_UNSPECIFIED = 0;
  HASHING_SCHEME_VERSION_V1 = 1;
}
```

The hashing algorithm is tied to the Protocol Version of the synchronizer that will be used to synchronize the transaction.
Specifically, each hashing scheme version will be supported on one or several protocol versions.
Implementations must use a hashing scheme version supported on the synchronizer on which the transaction will be submitted.

| Protocol Version | Supported Hashing Schemes |
|------------------|---------------------------|
| v32              | v1                        |


### Transaction Nodes

Transaction nodes are additionally individually versioned with a Daml version (also called LF version).
The encoding version is decoupled from the LF version and implementations should only focus on the hashing version.
However, new LF versions may introduce new fields in nodes or new node types. For that reason, the protobuf representation of a node is
versioned to accommodate those future changes. In practice, every new Daml language version results in a new hashing version.

```protobuf
message Node {
    string node_id = 1;
    // Specific LF version of the node.
    // This field is not directly relevant for how the transaction is encoded and can be treated as an opaque value for
    // our purpose here.
    optional string lf_version = 2;

    // Versioned node
    oneof versioned_node {
      // This is the protobuf representation of a node
      // Each version here may change the node content as well as the corresponding hashing algorithm
      interactive.v1.Node v1 = 1000;
    }
  }
```

# V1

## General approach

The hash of the `PreparedTransaction` will be computed by encoding every protobuf field of the messages to byte arrays,
and feeding those encoded values into a `SHA-256` hash builder. The rest of this section will detail how to deterministically encode
every proto message into a byte array. Sometimes during the process, we will hash partially encoded results with `SHA-256` and use the resulting hash value as the encoding
in messages further up. This will be explicit when necessary.

For numeric values, the Big Endian notation is used.
Furthermore, protobuf numeric values will be encoded according to their Java type representation.
Please refer to the official protobuf documentation for more information about protobuf to Java type mappings: https://protobuf.dev/programming-guides/proto3/#scalar
In particular:

>  In Java, unsigned 32-bit and 64-bit integers are represented using their signed counterparts, with the top bit simply being stored in the sign bit

Additionally, this is the java library used under the hood in Canton to serialize and deserialize protobuf: https://github.com/protocolbuffers/protobuf/tree/v3.25.5/java

### Notation and Utility Functions

- `encode`: Function that takes a protobuf message or primitive type `T` and transforms it into an array of bytes: `encode: T => byte[]`

e.g: `encode(false) = [0x00]`

- `to_utf_8`: Function converting a Java `String` to its UTF-8 encoded version: `to_utf_8: string => byte[]`

e.g: `to_utf_8("hello") = [0x68, 0x65, 0x6c, 0x6c, 0x6f]`

- `len`: Function returning the size of a collection (`array`, `list` etc...) as a signed 4 bytes integer: `len: Col => Int`

e.g: `len([4, 2, 8]) = 3`

- `split`: Function converting a Java `String` to a list of `String`s, by splitting the input using the provided delimiter: `split: (string, char) => byte[]`

e.g: `split("com.digitalasset.canton", '.') = ["com", "digitalasset", "canton"]`

- `||`:  Symbol representing concatenation of byte arrays

e.g: `[0x00] || [0x01] = [0x00, 0x01]`

- `[]`: Empty byte array. Denotes that the value should not be encoded.

- `from_hex_string`: Function that takes a string in the hexadecimal format as input and decodes it as a byte array: `from_hex_string: string => byte[]`

e.g: `from_hex_string("08020a") = [0x08, 0x02, 0x0a]`

- `int_to_string`: Function that takes an int and converts it to a string : `int_to_string: int => string`

e.g: `int_to_string(42) = "42"`

- `some`: Value wrapped in a defined optional. Should be encoded as a defined optional value: `some: T => optional T`

e.g: `encode(some(5)) = 0x01 || encode(5)`

See encoding of optional values below for details.

## Primitive Types

Unless otherwise specified, this is how primitive protobuf types should be encoded.

> [!NOTE]
> Not all protobuf types are described here, only the ones necessary to encode a `PreparedTransaction` message.

> [!IMPORTANT]
> Even default values must be included in the encoding.
> For instance if an int32 field is not set in the serialized protobuf, its default value (0) should be encoded.
> Similarly, an empty repeated field will still result in a `0x00` byte encoding (see the `repeated` section below for more details)

### google.protobuf.Empty

```
fn encode(empty): 0x00
```

### bool

```
fn encode(bool):
   if (bool)
      0x01
   else
      0x00
```

### int64 - uint64 - sint64 - sfixed64

```
fn encode(long):
   long # Java `Long` value equivalent: 8 bytes
```

e.g:

```
31380 (base 10) == 0x0000000000007a94
```

### int32 - uint32 - sint32 - sfixed32

```
fn encode(int):
   int # Java `Int` value equivalent: 4 bytes
```

e.g:

```
5 (base 10) == 0x00000005
```

### bytes / byte[]

```
fn encode(bytes):
   encode(len(bytes)) || bytes
```

e.g
```
0x68656c6c6f ->
    0x00000005 || # length
    0x68656c6c6f # content
```

### string
```
fn encode(string):
   encode(to_utf8(string))
```

e.g
```
"hello" ->
    0x00000005 || # length
    0x68656c6c6f # utf-8 encoding of "hello"
```

## Collections / Wrappers

### repeated

`repeated` protobuf fields represent an ordered collection of values of a specific message of type `T`.
It is critical that the order of values in the list is not modified, both for the encoding process and in the protobuf
itself when submitting the transaction for execution.
Below is the pseudocode algorithm encoding a protobuf value `repeated T list;`

```
fn encode(list):
   # prefix the result with the serialized length of the list
   result = encode(len(list)) # (result is mutable)

   # successively add encoded elements to the result, in order
   for each element in list:
      result = result || encode(element)

   return result
```

> [!NOTE]
> This encoding function also applies to lists generated from utility functions (e.g: split).

### optional

```
fn encode(optional):
   if (is_set(optional))
      0x01 || encode(optional.value)
   else
      0x00
```

`is_set` returns `true` if the value was set in the protobuf, `false` otherwise.

### map

The ordering of `map` entries in protobuf serialization is not guaranteed, and therefore problematic for deterministic encoding.
For that reason, we use `repeated` values instead of `map` throughout the protobuf.

## Ledger API Value

Encoding for the `Value` message defined in `com.daml.ledger.api.v2.value.proto`
For clarity, we exhaustively list all value types here. Important to note that each value is prefixed by a tag unique to the value type.
This tag will be made explicit for each value below.

### Unit

```
fn encode(unit):
    0X00 # Unit Type Tag
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L27)

### Bool

```
fn encode(bool):
    0X01 || # Bool Type Tag
    encode(bool) # Primitive boolean encoding
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L30)

### Int64

```
fn encode(int64):
    0X02 || # Int64 Type Tag
    encode(int64) # Primitive int64 encoding
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L32)

### Numeric

```
fn encode(numeric):
    0X03 || # Numeric Type Tag
    encode(numeric) # Primitive string encoding
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L52)

### Timestamp

```
fn encode(timestamp):
    0X04 || # Timestamp Type Tag
    encode(timestamp) # Primitive sfixed64 encoding
```

[Protobug Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L45)

### Date

```
fn encode(date):
    0X05 || # Date Type Tag
    encode(date) # Primitive int32 encoding
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L37)

### Party

```
fn encode(party):
    0X06 || # Party Type Tag
    encode(party) # Primitive string encoding
```


[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L56)

### Text

```
fn encode(text):
    0X07 || # Text Type Tag
    encode(text) # Primitive string encoding
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L59)

### Contract_id

```
fn encode(contract_id):
    0X08 || # Contract Id Type Tag
    from_hex_string(contract_id) # Contract IDs are hexadecimal strings, so they need to be decoded as such. They should not be encoded as classic strings
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L63)

### Optional

```
fn encode(optional):
   if (optional.value is set)
      0X09 || # Optional Type Tag
      0x01 || # Defined optional
      encode(optional.value)
   else
      0X09 || # Optional Type Tag
      0x00 || # Undefined optional
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L66)

Note this is conceptually the same as for the primitive `optional` protobuf modifier, with the addition of the type tag prefix.

### List

```
fn encode(list):
   0X0a || # List Type Tag
   encode(list.elements)
```

[Protobuf Definition]((https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L156-L160))

### TextMap

```
fn encode(text_map):
   0X0b || # TextMap Type Tag
   encode(text_map.entries)
```

[Protobuf Definition]((https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L169-L176))

#### TextMap.Entry

```
fn encode(entry):
   encode(entry.key) || encode(entry.value)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L170C3-L173C4)

### Record

```
fn encode(record):
   0X0c || # Record Type Tag
   encode(some(record.record_id)) ||
   encode(record.fields)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L87-L95)

#### RecordField

```
fn encode(record_field):
   encode(some(record_field.label)) || encode(record_field.value)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L98-L109)

### Variant

```
fn encode(variant):
   0X0d || # Variant Type Tag
   encode(some(variant.variant_id)) ||
   encode(variant.constructor) || encode(variant.value)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L79)

### Enum

```
fn encode(enum):
   0X0e || # Enum Type Tag
   encode(some(enum.enum_id)) ||
   encode(enum.constructor)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L82)

### GenMap

```
fn encode(gen_map):
   0X0f || # GenMap Type Tag
   encode(gen_map.entries)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L178-L185)

#### GenMap.Entry

```
fn encode(entry):
   encode(entry.key) || encode(entry.value)
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L179-L182)

### Identifier

```
fn encode(identifier):
   encode(identifier.package_id) || encode(split(identifier.module_name, '.')) || encode(split(identifier.entity_name, '.')))
```

[Protobuf Definition](https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L112-L125)

## Transaction

A transaction is a forest (list of trees). It is represented with a following protobuf message found [here](https://github.com/digital-asset/daml/blob/ba14c4430b8345e7f0f8b20c3feead2b88c90fb8/sdk/canton/community/ledger-api/src/main/protobuf/com/daml/ledger/api/v2/interactive/interactive_submission_service.proto#L283-L315).

The encoding function for a transaction is

```
fn encode(transaction):
   encode(transaction.version) || encode_node_ids(transaction.roots)
```

`encode_node_ids(node_ids)` encodes lists in the same way as we've described before, except the encoding of a `node_id` is
NOT done by encoding it as a string, but instead uses the following `encode(node_id)` function:

```
fn encode(node_id):
    for node in nodes:
        if node.node_id == node_id:
           return sha_256(encode_node(node))
    fail("Missing node") # All node ids should have a unique node in the nodes list. If a node is missing it should be reported as a bug.
```

> [!IMPORTANT]
> `encode(node_id)` effectively finds the corresponding node in the list of nodes and encodes the node.
> The `node_id` is an opaque value only used to reference nodes and is itself never encoded.
> Additionally, each node's encoding is **hashed using the sha_256 hashing algorithm**. This is relevant when encoding root nodes here as well as
> when recursively encoding sub-nodes of `Exercise` and `Rollback` nodes as we'll see below.

## Node

> [!NOTE]
> Each node's encoding is prefixed with additional meta information about the node, this will be made explicit in the encoding of each node.

`Exercise` and `Rollback` nodes both have a `children` field that references other nodes by their `NodeId`.

We'll also define a `find_seed: NodeId => optional bytes` function that will be used in the encoding:

```
fn find_seed(node_id):
    for node_seed in node_seeds:
        if int_to_string(node_seed.node_id) == node_id
            return some(node_seed.seed)
    return none

# There's no need to prefix the seed with its length because it has a fixed length. So its encoding is the identity function
fn encode_seed(seed):
    seed

# Normal optional encoding, except the seed is encoded with `encode_seed`
fn encode_optional_seed(optional_seed):
    if (is_some(optional_seed))
      0x01 || encode_seed(optional_seed.get)
   else
      0x00
```

`some` represents a set optional field, `none` an empty optional field.

### Create

```
fn encode_node(create):
    0x01 || # Node encoding version
    encode(create.lf_version) || # Node LF version
    0x00 || # Create node tag
    encode_optional_seed(find_seed(node.node_id)) ||
    encode(create.contract_id) ||
    encode(create.package_name) ||
    encode(create.template_id) ||
    encode(create.argument) ||
    encode(create.signatories) ||
    encode(create.stakeholders)
```

### Exercise

```
fn encode_node(exercise):
    0x01 || # Node encoding version
    encode(exercise.lf_version) || # Node LF version
    0x01 || # Exercise node tag
    encode_seed(find_seed(node.node_id).get) ||
    encode(exercise.contract_id) ||
    encode(exercise.package_name) ||
    encode(exercise.template_id) ||
    encode(exercise.signatories) ||
    encode(exercise.stakeholders) ||
    encode(exercise.acting_parties) ||
    encode(exercise.interface_id) ||
    encode(exercise.choice_id) ||
    encode(exercise.chosen_value) ||
    encode(exercise.consuming) ||
    encode(exercise.exercise_result) ||
    encode(exercise.choice_observers) ||
    encode(exercise.children)
```

> [!IMPORTANT]
> For Exercise nodes, the node seed **MUST** be defined. Therefore it is encoded as a **non** optional field, as noted via the
> `.get` in `find_seed(node.node_id).get`. If the seed of an exercise node cannot be found in the list of `node_seeds`, encoding must be stopped
> and it should be reported as a bug.

> [!NOTE]
> The last encoded value of the exercise node is its `children` field. This recursively traverses the transaction tree.

### Fetch

```
fn encode_node(fetch):
    0x01 || # Node encoding version
    encode(fetch.lf_version) || # Node LF version
    0x02 || # Fetch node tag
    encode(fetch.contract_id) ||
    encode(fetch.package_name) ||
    encode(fetch.template_id) ||
    encode(fetch.signatories) ||
    encode(fetch.stakeholders) ||
    encode(fetch.acting_parties)
```

### Rollback

```
fn encode_node(rollback):
   0x01 || # Node encoding version
   0x03 || # Rollback node tag
   encode(rollback.children)
```

> [!NOTE]
> Rollback nodes do not have an lf version.

### Transaction Hash

Once the transaction is encoded, the hash is obtained by running `sha_256` over the encoded byte array, with a hash purpose prefix:

```
fn hash(transaction):
    sha_256(
        0x00000030 || # Hash purpose
        encode(transaction)
    )
```


## Metadata

The final part of `PreparedTransaction` is metadata.
Note all fields of the metadata need to be signed. Only some fields contribute to impact the ledger change triggered by the transaction.
The rest of the fields are required by the Canton protocol to correctly process the transactions but either have no impact on the ledger change,
or they already have been signed indirectly by signing the transaction itself.

```
fn encode(metadata, prepare_submission_request):
    0x01 || # Metadata Encoding Version
    encode(metadata.submitter_info.act_as) ||
    encode(metadata.submitter_info.command_id) ||
    encode(metadata.transaction_uuid) ||
    encode(metadata.mediator_group) ||
    encode(metadata.synchronizer_id) ||
    encode(metadata.ledger_effective_time) ||
    encode(metadata.submission_time) ||
    encode(metadata.disclosed_events)
```

### ProcessedDisclosedContract

```
fn encode(processed_disclosed_contract):
    encode(processed_disclosed_contract.created_at) ||
    encode(processed_disclosed_contract.contract)
```

### Metadata Hash

Once the metadata is encoded, the hash is obtained by running `sha_256` over the encoded byte array, with a hash purpose prefix:

```
fn hash(metadata):
    sha_256(
        0x00000030 || # Hash purpose
        encode(metadata)
    )
```

## Final Hash

We're finally ready to compute the hash that needs to be signed to commit to the ledger changes.

```
fn encode(prepared_transaction):
    0x00000030 || # Hash purpose
    hash(transaction) ||
    hash(metadata)
```

```
fn hash(prepared_transaction):
    sha_256(encode(prepared_transaction))
```

This resulting hash must be signed with the key pair used to onboard the external party,
and both the signature along with the `PreparedTransaction` must be sent to the API to submit the transaction to the ledger.

## Trust Model

This section outlines the trust relationships between users of the interactive submission service and the components of the system to ensure secure interactions.
It covers the components of the network involved in an interactive submission, its validation, and eventual commitment to the ledger or rejection.
It is intended to provide a system-level overview of the security properties and is addressed to application developers and operators integrating against the API.

## Definitions

Trust: Confidence in a system component to act securely and reliably.
User: Any individual or entity interacting with the interactive submission API.
External Party: Daml party on behalf of which transactions are being submitted using the interactive submission service.

## Components

The interactive submission feature revolves around three roles assumed by participant nodes. In practice, these roles can be assumed by one or more physical nodes.

- Preparing Participant Node (PPN): generates a Daml transaction from a Ledger API command
- Executing Participant Node (EPN): submits the transaction to the network when provided with a signature of the transaction hash
- Confirming Participant Node (CPN): confirms transactions on behalf of the external party and verifies the party's signature

### Preparing Participant Node (PPN)

#### Trust Relationship
Untrusted

#### User responsibilities
- Users visualize the prepared transaction. They check that it matches their intent. They also validate the data therein to ensure that the transaction can be securely submitted. In particular:
  - The transaction corresponds to the ledger effects the User intends to generate
  - The `submission_time` is not ahead of the current sequencer time on the synchronizer.
    A reliable way to do this is to compare the `submission_time` with the minimum of:
    - Wallclock time of the submitting application
    - Last record time emitted by at least `f+1` participants + the configured `mediatorDeduplicationTimeout` on the synchronizer (`f` being the maximum number of faulty nodes)
  - If `min_ledger_time` is defined in the `PrepareSubmissionRequest`, validate that the `ledger_time` in the `PrepareSubmissionResponse` is either empty or ahead of the `min_ledger_time` requested.
- Users compute the hash of the transaction according to the specification described in this document.
  - The hash provided in the `PrepareSubmissionResponse` must be ignored if the PPN is not trusted.

#### Example Threats
- The PPN tampers with the transaction or even generates an entirely different transaction

**Mitigation**: The user inspects the transaction and verifies it corresponds to their intent.

### Executing Participant Node (EPN)

#### Trust Relationship
- Untrusted on transaction tampering
- Trusted on command completions

#### User responsibilities
- Users sign the transaction hash they computed from the serialized transaction
- Keys used  to sign the transaction are only used for this purpose
- Keys used for signature are stored securely and kept private
- Users do not rely on `workflow_id` in command completions

#### Trust Assumptions

#### Example Threats
- The EPN emits incorrect completion events, leading users to retry a submission thinking it had failed when it had actually succeeded.

**Mitigation:**
No practical general mitigation in Canton 3.2.
Self-conflicting transactions are not subject to this threat, i.e.,
the transaction contains an archive on at least one contract where the submitting party is a signatory of.
In that case the CPN(s) then reject resubmissions of the transaction.
Users can attempt to correlate completion events with the transaction stream from the (trusted) CPN.
Note the transaction stream only emits committed transactions, making this difficult to leverage in practice.

### Confirming Participant Node (CPN)

#### Trust Relationship
Fewer than `PartyToParticipant.threshold` participants are assumed to be malicious

#### Trust Assumptions
- Users trust that at least one out of `PartyToParticipant.threshold` CPNs correctly approve or reject requests on behalf of the party they host as expected according to the Canton protocol

#### User Responsibilities
- Users carefully choose their CPNs.

#### Example Threats
- The CPN attempts to submit transactions on behalf of the party without explicit authorization from the party
- The CPN incorrectly accepts transactions even if the signature is invalid

**Mitigation:**: The external party chooses multiple CPNs to host it jointly in a fault-tolerant mode by setting the threshold on its `PartyToParticipant` topology transaction.
The threats are mitigated if less CPNs than the threshold are faulty. Canton achieves the fault tolerance by requiring approvals from threshold many CPNs for transactions that are to be commmitted to the ledger.

If the external party consumes information from CPNs such as the transaction stream, it must cross-check the information from threshold many different CPNs to achieve fault tolerance.
