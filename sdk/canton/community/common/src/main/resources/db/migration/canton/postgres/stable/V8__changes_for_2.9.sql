-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE active_contracts DROP COLUMN transfer_counter;
ALTER TYPE operation_type ADD VALUE 'add';
ALTER TYPE operation_type ADD VALUE 'purge';
