// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import { Form, List, Button } from "semantic-ui-react";
import { Party } from "@daml/types";

type Props = {
  parties: Party[];
  partyToAlias: Map<Party, string>;
  onAddParty: (party: Party) => Promise<boolean>;
};

/**
 * React component to edit a list of `Party`s.
 */
const PartyListEdit: React.FC<Props> = ({
  parties,
  partyToAlias,
  onAddParty,
}) => {
  const [newParty, setNewParty] = React.useState<string | undefined>(undefined);
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const aliasToOption = (party: string, alias: string) => {
    return { key: party, text: alias, value: party };
  };
  const options = Array.from(partyToAlias.entries()).map(e =>
    aliasToOption(e[0], e[1]),
  );

  const addParty = async (event?: React.FormEvent) => {
    if (event) {
      event.preventDefault();
    }
    setIsSubmitting(true);
    const success = await onAddParty(newParty ?? "");
    setIsSubmitting(false);
    if (success) {
      setNewParty(undefined);
    }
  };

  return (
    <List relaxed>
      {[...parties]
        .sort((x, y) => x.localeCompare(y))
        .map(party => (
          <List.Item key={party}>
            <List.Icon name="user outline" />
            <List.Content>
              <List.Header className="test-select-following">
                {partyToAlias.get(party) ?? party}
              </List.Header>
            </List.Content>
          </List.Item>
        ))}
      <br />
      <Form onSubmit={addParty}>
        <Form.Select
          fluid
          search
          allowAdditions
          additionLabel="Insert a party identifier: "
          additionPosition="bottom"
          readOnly={isSubmitting}
          loading={isSubmitting}
          className="test-select-follow-input"
          placeholder={newParty ?? "Username to follow"}
          value={newParty}
          options={options}
          onAddItem={(event, { value }) => setNewParty(value?.toString())}
          onChange={(event, { value }) => setNewParty(value?.toString())}
        />
        <Button type="submit" className="test-select-follow-button">
          Follow
        </Button>
      </Form>
    </List>
  );
};

export default PartyListEdit;
