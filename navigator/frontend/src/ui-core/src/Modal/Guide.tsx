// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Button from "../Button";
import { Section } from "../Guide";
import Modal, { Props } from "../Modal";
import styled from "../theme";

const description = `
The content of the modal is specified using the 'content'
property.
The modal is a stateless (uncontrolled component), use the
'isOpen' property to specify whether the modal is visible.
When the user clicks outside of the modal, the 'onHide'
callback is fired to signal intent to close the modal.
`;

export type State = Props;

const ModalContent = styled.div`
  width: 350px;
  min-height: 30px;
  padding: 1px 1.33em;
`;

const CenteredDiv = styled.div`
  display: flex;
  justify-content: center;
  overflow: hidden;
`;

const modalContent = (onClose: () => void) => (
  <ModalContent>
    <div>
      <h4>Modal title</h4>
      <p>
        Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua.
      </p>
      <Button
        onClick={() => {
          onClose();
        }}>
        Close
      </Button>
      <br />
    </div>
  </ModalContent>
);

export default class ModalGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = {
      isOpen: false,
      content: modalContent(() => this.close()),
      onHide: () => this.close(),
    };
  }

  open(): void {
    this.setState({
      isOpen: true,
    });
  }

  close(): void {
    this.setState({
      isOpen: false,
    });
  }

  render(): JSX.Element {
    return (
      <Section title="Modal" description={description}>
        <CenteredDiv>
          <Button onClick={() => this.open()}>Click to open</Button>
        </CenteredDiv>
        <Modal {...this.state} />
      </Section>
    );
  }
}
