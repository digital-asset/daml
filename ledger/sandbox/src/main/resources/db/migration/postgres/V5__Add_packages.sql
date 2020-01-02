-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V5: List of packages
--
-- This schema version adds a table for tracking DAML-LF packages.
-- Previously, packages were only stored in memory and needed to be specified through the CLI.
---------------------------------------------------------------------------------------------------



CREATE TABLE packages (
  -- The unique identifier of the package (the hash of its content)
  package_id         varchar primary key   not null,
  -- Packages are uploaded as DAR files (i.e., in groups)
  -- This field can be used to find out which packages were uploaded together
  upload_id          varchar               not null,
  -- A human readable description of the package source
  source_description varchar,
  -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
  size               bigint                not null,
  -- The time when the package was added
  known_since        timestamptz           not null,
  -- The ledger end at the time when the package was added
  ledger_offset      bigint                not null,
  -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
  --  See also `daml-lf/archive/da/daml_lf.proto`.
  package            bytea                 not null
);

