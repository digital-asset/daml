CREATE TABLE ${table.prefix}state_new
(
    key_hash VARBINARY(2048) PRIMARY KEY NOT NULL,
    key   VARBINARY(16384) NOT NULL,
    value BLOB                         NOT NULL
);

INSERT INTO ${table.prefix}state_new (key_hash, key, value)
    SELECT key_hash, key, value FROM ${table.prefix}state;

DROP TABLE ${table.prefix}state;
ALTER TABLE ${table.prefix}state_new RENAME TO ${table.prefix}state;
