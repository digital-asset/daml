--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

DROP INDEX participant_events_divulgence_template_id_idx;
DROP INDEX participant_events_create_template_id_idx;
DROP INDEX participant_events_consuming_exercise_template_id_idx;
DROP INDEX participant_events_non_consuming_exercise_template_id_idx;

ALTER TABLE participant_events_divulgence
  MODIFY template_id NUMBER;

ALTER TABLE participant_events_create
  MODIFY template_id NUMBER;

ALTER TABLE participant_events_consuming_exercise
  MODIFY template_id NUMBER;

ALTER TABLE participant_events_non_consuming_exercise
  MODIFY template_id NUMBER;
