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
  submission_id      varchar               not null,
  -- A human readable description of the package source
  source_description varchar,
  -- The size of the original uploaded package, in bytes
  size               bigint                not null,
  -- The time when the package was added
  known_since        timestamptz           not null,
  -- The ledger end at the time when the package was added
  ledger_offset      bigint                not null,
  -- The package is stored using the .proto definition in
  -- `daml-lf/archive/da/daml_lf.proto`.
  package            bytea                 not null
);

