
--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- Transaction metering alterations
SELECT 'Harmonise the transaction_metering columns with other examples in the db';

ALTER TABLE transaction_metering ALTER application_id TYPE TEXT;
ALTER TABLE transaction_metering ALTER ledger_offset TYPE TEXT;

-- Parameter alterations
SELECT 'Add Metering Parameters: Creating table...';

-- Create metering parameters
CREATE TABLE metering_parameters (
    ledger_metering_end TEXT,
    ledger_metering_timestamp BIGINT NOT NULL
);

-- Create participant metering
SELECT 'Add Participant Metering: Creating table...';

CREATE TABLE participant_metering (
    application_id TEXT NOT NULL,
    from_timestamp BIGINT NOT NULL,
    to_timestamp BIGINT NOT NULL,
    action_count INTEGER NOT NULL,
    ledger_offset TEXT NOT NULL
);

SELECT 'Add Participant Metering: Creating indexes...';

CREATE UNIQUE INDEX participant_metering_from_to_application ON participant_metering(from_timestamp, to_timestamp, application_id);

SELECT 'Add Participant Metering: Done.';
