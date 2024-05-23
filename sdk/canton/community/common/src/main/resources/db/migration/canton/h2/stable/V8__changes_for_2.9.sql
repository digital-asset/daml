-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE active_contracts DROP COLUMN transfer_counter;

ALTER TABLE daml_packages
    ADD (
        package_name varchar(256),
        package_version varchar(256)
);
