
-- TODO: backfill here

ALTER TABLE ${table.prefix}state DROP PRIMARY KEY;
ALTER TABLE ${table.prefix}state ADD PRIMARY KEY (key_hash);
