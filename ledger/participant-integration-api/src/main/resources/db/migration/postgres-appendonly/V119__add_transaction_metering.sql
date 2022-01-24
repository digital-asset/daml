--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

SELECT 'Add Transaction Metering: Creating table...';

CREATE TABLE transaction_metering (
    application_id VARCHAR NOT NULL,
    action_count INTEGER NOT NULL,
    from_timestamp BIGINT NOT NULL,
    to_timestamp BIGINT NOT NULL,
    from_ledger_offset VARCHAR NOT NULL,
    to_ledger_offset VARCHAR NOT NULL
);

SELECT 'Add Transaction Metering: Creating indexes...';

CREATE INDEX transaction_metering_from_offset ON transaction_metering(from_ledger_offset);

SELECT 'Add Transaction Metering: Done.';

