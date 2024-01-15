// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Button from "../Button";
import { UntypedIcon } from "../Icon";
import Popover from "../Popover";
import styled from "../theme";
import Truncate from "../Truncate";

export interface Choice {
  name: string;
  inheritedInterface: string | null;
}

export interface Contract {
  id: string;
  template: {
    choices: Choice[];
  };
  archiveEvent: { id: string } | null;
}

const List = styled.ul`
  min-width: 160px;
  list-style: none;
  padding: 15px;
  margin: 0;
`;

const ListItem = styled.li`
  display: block;
  &:hover {
    color: ${({ theme }) => theme.colorPrimary[0]};
  }
`;

interface ContentProps {
  contract: Contract;
  choices: Choice[];
  renderLink(
    contractId: string,
    choiceName: string,
    inheritedInterface: string | null,
  ): React.ReactNode;
}

const Content = ({ contract, choices, renderLink }: ContentProps) => {
  const isArchived = contract.archiveEvent !== null;
  if (isArchived) {
    return (
      <List>
        <ListItem>
          <Truncate>(Archived)</Truncate>
        </ListItem>
      </List>
    );
  } else if (choices.length > 0) {
    return (
      <List>
        {contract.template.choices.map(choice => (
          <ListItem key={choice.inheritedInterface + choice.name}>
            <Truncate>
              {renderLink(contract.id, choice.name, choice.inheritedInterface)}
            </Truncate>
          </ListItem>
        ))}
      </List>
    );
  } else {
    return (
      <List>
        <ListItem>
          <Truncate>(No choices)</Truncate>
        </ListItem>
      </List>
    );
  }
};

export interface State {
  open: boolean;
}

export interface Props {
  contract: Contract;
  renderLink(
    contractId: string,
    choiceName: string,
    inheritedInterface: string | null,
  ): React.ReactNode;
}

export default class ChoicesButton extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      open: false,
    };
  }

  render(): JSX.Element {
    const target = (
      <Button
        type={"minimal"}
        onClick={e => {
          e.stopPropagation();
        }}>
        <UntypedIcon name="wrench" />
      </Button>
    );

    const content = (
      <Content
        contract={this.props.contract}
        choices={this.props.contract.template.choices}
        renderLink={this.props.renderLink}
      />
    );

    return (
      <span>
        <Popover
          isOpen={this.state.open}
          content={content}
          target={target}
          arrow={true}
          position={"bottom"}
          onInteraction={(_, isOpen) => this.setState({ open: isOpen })}
        />
      </span>
    );
  }
}
