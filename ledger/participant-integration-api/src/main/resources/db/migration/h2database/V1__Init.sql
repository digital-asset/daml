-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- PART 1: copied and adapted from postgres/V1__Init.sql

-- Stores the history of the ledger -- mostly transactions. This table
-- is immutable in the sense that rows can never be modified, only
-- added.
CREATE TABLE ledger_entries
(
  -- Every entry is indexed by a monotonically growing integer. That is,
  -- new rows can only have a ledger_offet which is greater than the
  -- larger ledger_offset in ledger_entries. However, note that there
  -- might be gaps in the series formed by all the ledger_offsets in the
  -- table.
  ledger_offset         bigint primary key           not null,
  -- one of 'transaction', 'rejection', or 'checkpoint' -- also see
  -- check_entry below. note that we _could_ store the different entries
  -- in different tables, but as of now we deem more convient having a
  -- single table even if it imposes the constraint below, since this
  -- table represents a single unified stream of events and partitioning
  -- it across tables would be more inconvient. We might revise this in
  -- the future.
  typ                   varchar                      not null,
  -- see ledger API definition for more infos on some of these these fields
  transaction_id        varchar unique,
  command_id            varchar,
  application_id        varchar,
  submitter             varchar,
  workflow_id           varchar,
  effective_at          timestamp, -- with time zone
  recorded_at           timestamp                    not null, -- with time zone
  -- The transaction is stored using the .proto definition in
  -- `daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/transaction.proto`, and
  -- encoded using
  -- `daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/transaction.proto`.
  transaction           bytea,
  rejection_type        varchar,
  rejection_description varchar,

  -- note that this is not supposed to be a complete check, for example we do not check
  -- that fields that are not supposed to be present are indeed null.
  constraint check_entry
  check (
    (typ = 'transaction' and transaction_id != null and command_id != null and application_id != null and
     submitter != null and effective_at != null and transaction != null) or
    (typ = 'rejection' and command_id != null and application_id != null and submitter != null and
     rejection_type != null and rejection_description != null) or
    (typ = 'checkpoint'))

);

-- This embodies the deduplication in the Ledger API.
CREATE UNIQUE INDEX idx_transactions_deduplication
  ON ledger_entries (command_id, application_id);

CREATE TABLE disclosures (
  transaction_id varchar not null,
  event_id       varchar not null,
  party          varchar not null,
  foreign key (transaction_id) references ledger_entries (transaction_id)
);

-- Note that technically this information is all present in `ledger_entries`,
-- but we store it in this form since it would not be viable to traverse
-- the entries every time we need to gain information as a contract. It's essentially
-- a materialized view of the contracts state.
CREATE TABLE contracts (
  id             varchar primary key not null,
  -- this is the transaction id that _originated_ the contract.
  transaction_id varchar             not null,
  -- this is the workflow id of the transaction above. note that this is
  -- a denormalization -- we could simply look up in the transaction table.
  -- we cache it here since we do not want to risk impacting performance
  -- by looking it up in `ledger_entries`, however we should verify this
  -- claim.
  workflow_id    varchar,
  -- This tuple is currently included in `contract`, since we encode
  -- the full value including all the identifiers. However we plan to
  -- move to a more compact representation that would need a pointer to
  -- the "top level" value type, and therefore we store the identifier
  -- here separately.
  package_id     varchar             not null,
  -- using the QualifiedName#toString format
  name           varchar             not null,
  -- this is denormalized much like `transaction_id` -- see comment above.
  create_offset  bigint              not null,
  -- this on the other hand _cannot_ be easily found out by looking into
  -- `ledger_entries` -- you'd have to traverse from `create_offset` which
  -- would be prohibitively expensive.
  archive_offset bigint,
  -- the serialized contract value, using the definition in
  -- `daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/value.proto`
  -- and the encoder in `ContractSerializer.scala`.
  contract       bytea               not null,
  -- only present in contracts for templates that have a contract key definition.
  -- encoded using the definition in
  -- `daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/value.proto`.
  key            bytea,
  foreign key (transaction_id) references ledger_entries (transaction_id),
  foreign key (create_offset) references ledger_entries (ledger_offset),
  foreign key (archive_offset) references ledger_entries (ledger_offset)
);

-- These two indices below could be a source performance bottleneck. Every additional index slows
-- down insertion. The contracts table will grow endlessly and the sole purpose of these indices is
-- to make ACS queries performant, while sacrificing insertion speed.
CREATE INDEX idx_contract_create_offset
  ON contracts (create_offset);
CREATE INDEX idx_contract_archive_offset
  ON contracts (archive_offset);

-- TODO what's the difference between this and `diclosures`? If we can rely on `event_id`
-- being the `contract_id`, isn't `disclosures` enough?
CREATE TABLE contract_witnesses (
  contract_id varchar not null,
  witness     varchar not null,
  foreign key (contract_id) references contracts (id)
);
CREATE UNIQUE INDEX contract_witnesses_idx
  ON contract_witnesses (contract_id, witness);

CREATE TABLE contract_key_maintainers (
  contract_id varchar not null,
  maintainer  varchar not null,
  foreign key (contract_id) references contracts (id)
);

CREATE UNIQUE INDEX contract_key_maintainers_idx
  ON contract_key_maintainers (contract_id, maintainer);

-- this table is meant to have a single row storing all the parameters we have
CREATE TABLE parameters (
  -- the generated or configured id identifying the ledger
  ledger_id varchar not null,
  -- stores the head offset, meant to change with every new ledger entry
  ledger_end bigint not null,
  -- the external ledger offset that corresponds to the index ledger end (added in Postgres V6)
  external_ledger_end varchar
);

-- table to store a mapping from (template_id, contract value) to contract_id
-- contract values are binary blobs of unbounded size, the table therefore only stores a hash of the value
-- and relies for the hash to be collision free
CREATE TABLE contract_keys (
  package_id   varchar not null,
  -- using the QualifiedName#toString format
  name         varchar not null,
  -- stable SHA256 of the protobuf serialized key value, produced using
  -- `KeyHasher.scala`.
  value_hash   varchar not null,
  contract_id  varchar not null,
  PRIMARY KEY (package_id, name, value_hash),
  foreign key (contract_id) references contracts (id)
);

-- PART 2: copied and adapted from postgres/V2_0__Contract_divulgence.sql

---------------------------------------------------------------------------------------------------
-- V2: Contract divulgence
--
-- This schema version adds a table for tracking contract divulgence.
-- This is required for making sure contracts can only be fetched by parties that see the contract.
---------------------------------------------------------------------------------------------------



CREATE TABLE contract_divulgences (
  contract_id   varchar  not null,
  -- The party to which the given contract was divulged
  party         varchar  not null,
  -- The offset at which the contract was divulged to the given party
  ledger_offset bigint   not null,
  -- The transaction ID at which the contract was divulged to the given party
  transaction_id varchar not null,

  foreign key (contract_id) references contracts (id),
  foreign key (ledger_offset) references ledger_entries (ledger_offset),
  foreign key (transaction_id) references ledger_entries (transaction_id),

  CONSTRAINT contract_divulgences_idx UNIQUE(contract_id, party)
);

-- PART 3: n/a scala-migration only, not applicable to newly introduced database types

-- PART 4: adopted unmodified from postgres/V4_0__Add_parties.sql

---------------------------------------------------------------------------------------------------
-- V4: List of parties
--
-- This schema version adds a table for tracking known parties.
-- In the sandbox, parties are added implicitly when they are first mentioned in a transaction,
-- or explicitly through an API call.
---------------------------------------------------------------------------------------------------



CREATE TABLE parties (
  -- The unique identifier of the party
  party varchar primary key not null,
  -- A human readable name of the party, might not be unique
  display_name varchar,
  -- True iff the party was added explicitly through an API call
  explicit bool not null,
  -- For implicitly added parties: the offset of the transaction that introduced the party
  -- For explicitly added parties: the ledger end at the time when the party was added
  ledger_offset bigint
);


-- PART 5: copied and adapted from postgres/V5__Add_packages.sql

---------------------------------------------------------------------------------------------------
-- V5: List of packages
--
-- This schema version adds a table for tracking DAML-LF packages.
-- Previously, packages were only stored in memory and needed to be specified through the CLI.
---------------------------------------------------------------------------------------------------



CREATE TABLE packages (
  -- The unique identifier of the package (the hash of its content)
  package_id         varchar primary key      not null,
  -- Packages are uploaded as DAR files (i.e., in groups)
  -- This field can be used to find out which packages were uploaded together
  upload_id          varchar                  not null,
  -- A human readable description of the package source
  source_description varchar,
  -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
  size               bigint                   not null,
  -- The time when the package was added
  known_since        timestamp                not null, -- with time zone
  -- The ledger end at the time when the package was added
  ledger_offset      bigint                   not null,
  -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
  --  See also `daml-lf/archive/da/daml_lf.proto`.
  package            bytea                    not null
);

-- PART 6: postgres/V6__External_Ledger_Offset.sql not needed as parameters.external_ledger_offset already in PART 1
