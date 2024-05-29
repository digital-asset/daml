-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE active_contracts DROP COLUMN transfer_counter;
ALTER TYPE operation_type ADD VALUE 'add';
ALTER TYPE operation_type ADD VALUE 'purge';

ALTER TABLE daml_packages
    ADD COLUMN package_name varchar(256) collate "C",
    ADD COLUMN package_version varchar(256) collate "C";
