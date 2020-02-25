// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from 'react'
import { List, ListItem } from 'semantic-ui-react';
import { Message } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';
import { useParty, useStreamQuery } from '@daml/react';

/**
 * React component displaying the list of messages for the current user.
 */
const MessageList: React.FC = () => {
  const username = useParty();
  const messagesResult = useStreamQuery(Message, () => ({receiver: username}), []);
  const messages = messagesResult.contracts.map(message => message.payload);

  const showMessage = (message: Message): string => {
    return (message.sender + ": " + message.content);
  }

  return (
    <List relaxed>
      {messages.map(message => <ListItem>{showMessage(message)}</ListItem>)}
    </List>
  );
}

export default MessageList;
