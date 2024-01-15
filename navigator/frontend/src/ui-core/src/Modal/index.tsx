// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Modal as ROModal } from "react-overlays";
import styled from "../theme";

const Wrapper = styled.div`
  display: inline;
`;

const PositionContainer = styled.div`
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  z-index: 999;
`;

const ContentOuterContainer = styled.div`
  box-shadow: 0 0 0 1px rgba(16, 22, 26, 0.1), 0 2px 4px rgba(16, 22, 26, 0.2),
    0 8px 24px rgba(16, 22, 26, 0.2);
  transform: scale(1);
  display: inline-block;
  border-radius: ${({ theme }) => theme.tooltipRadius};
`;

const ContentInnerContainer = styled.div`
  padding: 0;
  border-radius: ${({ theme }) => theme.tooltipRadius};
  background-color: ${({ theme }) => theme.colorBackground};
  height: 100%;
  width: 100%;
`;

export interface Props {
  /** Content displayed in the Modal */
  content: JSX.Element;

  /** Whether the modal is open (visible) */
  isOpen: boolean;

  /**
   * A callback fired when either the backdrop is clicked, or the escape key is pressed.
   *
   * The `onHide` callback only signals intent from the Modal,
   * you must actually set the `isOpen` prop to `false` for the Modal to close.
   */
  onHide?(): void;
}

/**
 * A modal dialog.
 */
export default class Modal extends React.Component<Props, {}> {
  constructor(props: Props) {
    super(props);
  }

  render(): JSX.Element {
    const { content, isOpen } = this.props;

    return (
      <Wrapper>
        <ROModal
          show={isOpen}
          backdrop={true}
          container={document.body}
          onHide={this.props.onHide}
          backdropStyle={{
            position: "fixed",
            zIndex: "auto",
            top: "0px",
            bottom: "0px",
            left: "0px",
            right: "0px",
            backgroundColor: "rgb(0, 0, 0)",
            opacity: 0.5,
          }}>
          <PositionContainer>
            <ContentOuterContainer>
              <ContentInnerContainer>{content}</ContentInnerContainer>
            </ContentOuterContainer>
          </PositionContainer>
        </ROModal>
      </Wrapper>
    );
  }
}
