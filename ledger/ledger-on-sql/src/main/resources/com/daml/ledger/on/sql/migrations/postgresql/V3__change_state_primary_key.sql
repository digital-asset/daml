
-- TODO: backfill here

ALTER TABLE ${table.prefix}state DROP CONSTRAINT ${table.prefix}state_pkey;
ALTER TABLE ${table.prefix}state ADD PRIMARY KEY (key_hash);
