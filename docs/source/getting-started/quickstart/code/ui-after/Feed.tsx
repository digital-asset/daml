import React from 'react'
import { List, ListItem } from 'semantic-ui-react';
import { Message } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';

type Props = {
  messages: Message[];
}

/**
 * React component to show a feed of messages for a particular user.
 */
const Feed: React.FC<Props> = ({messages}) => {
  const showMessage = (message: Message): string => {
    return (message.sender + ": " + message.content);
  }

  return (
    <List relaxed>
      {messages.map((message) => <ListItem>{showMessage(message)}</ListItem>)}
    </List>
  );
}

export default Feed;
