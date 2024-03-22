--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

SELECT 'Add Transaction Metering: Creating table...';

CREATE TABLE transaction_metering (
    application_id VARCHAR(512) NOT NULL,
    action_count INTEGER NOT NULL,
    metering_timestamp BIGINT NOT NULL,
    ledger_offset VARCHAR(512) NOT NULL
);

SELECT 'Add Transaction Metering: Creating indexes...';

CREATE INDEX transaction_metering_ledger_offset ON transaction_metering(ledger_offset);

SELECT 'Add Transaction Metering: Done.';

