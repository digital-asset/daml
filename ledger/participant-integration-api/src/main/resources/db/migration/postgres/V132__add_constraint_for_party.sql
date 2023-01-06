ALTER TABLE participant_user_rights
    ADD CONSTRAINT participant_user_rights_for_party_fk
        FOREIGN KEY (for_party)
            REFERENCES participant_party_records (party);


ALTER TABLE participant_users
    ADD CONSTRAINT participant_users_primary_party_fk
        FOREIGN KEY (primary_party)
            REFERENCES participant_party_records (party);
