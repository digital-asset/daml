CREATE TABLE participant_identity_provider_config
(
    identity_provider_id VARCHAR(255) PRIMARY KEY NOT NULL COLLATE "C",
    issuer               VARCHAR             NOT NULL UNIQUE,
    jwks_url             VARCHAR             NOT NULL,
    is_deactivated       BOOLEAN             NOT NULL
);
