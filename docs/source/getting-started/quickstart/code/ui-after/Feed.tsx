import React from 'react'
import { List, ListItem } from 'semantic-ui-react';
import { Message } from '../daml/create-daml-app/Message';

type Props = {
  messages: Message[];
}

/**
 * React component to show all messages that have been sent to you from your network.
 */
const Feed: React.FC<Props> = ({messages}) => {
  const showMessage = (message: Message): string => {
    const author = message.sender;
    const content = message.content;
    return (author + " says: " + content);
  }

  return (
    <List relaxed>
      {messages.map((message) => <ListItem>{showMessage(message)}</ListItem>)}
    </List>
  );
}

export default Feed;
