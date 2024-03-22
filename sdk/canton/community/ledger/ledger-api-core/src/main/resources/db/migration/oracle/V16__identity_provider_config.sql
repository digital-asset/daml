CREATE TABLE participant_identity_provider_config
(
    identity_provider_id VARCHAR2(255) PRIMARY KEY NOT NULL,
    issuer               VARCHAR2(4000) NOT NULL UNIQUE,
    jwks_url             VARCHAR2(4000) NOT NULL,
    is_deactivated       NUMBER DEFAULT 0 NOT NULL
);
