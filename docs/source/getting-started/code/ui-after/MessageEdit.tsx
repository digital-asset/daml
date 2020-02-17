import React from 'react'
import { Form, Input, Button } from 'semantic-ui-react';
import { Text } from '@daml/types';

type Props = {
  sendMessage: (content: Text, receiver: string) => Promise<boolean>;
}

/**
 * React component to edit a message to send to a friend.
 */
const MessageEdit: React.FC<Props> = ({sendMessage}) => {
  const [content, setContent] = React.useState('');
  const [receiver, setReceiver] = React.useState('');
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const submitMessage = async (event?: React.FormEvent) => {
    if (event) {
      event.preventDefault();
    }
    setIsSubmitting(true);
    const success = await sendMessage(content, receiver);
    setIsSubmitting(false);
    if (success) {
      setContent('');
      setReceiver('');
    }
  }

  return (
    <Form onSubmit={submitMessage}>
      <Input
        fluid
        transparent
        readOnly={isSubmitting}
        loading={isSubmitting}
        placeholder='Choose a friend'
        value={receiver}
        onChange={(event) => setReceiver(event.currentTarget.value)}
      />
      <br />
      <Input
        fluid
        transparent
        readOnly={isSubmitting}
        loading={isSubmitting}
        placeholder="Write a message"
        value={content}
        onChange={(event) => setContent(event.currentTarget.value)}
      />
      <br />
      <Button type="submit">Send</Button>
    </Form>
  );
};

export default MessageEdit;
