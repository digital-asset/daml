-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE participant_settings DROP COLUMN unique_contract_keys;

ALTER TABLE sequencer_events DROP COLUMN error_message;
