import React from 'react'
import { Form, Input, Button } from 'semantic-ui-react';
import { Text } from '@digitalasset/daml-json-types';

type Props = {
  sendMessage: (content: Text, parties: string) => Promise<boolean>;
}

/**
 * React component to edit a message to send to some friends.
 */
const MessageEdit: React.FC<Props> = ({sendMessage}) => {
  const [content, setContent] = React.useState('');
  const [receivers, setReceivers] = React.useState('');
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const submitMessage = async (event?: React.FormEvent) => {
    if (event) {
      event.preventDefault();
    }
    setIsSubmitting(true);
    const success = await sendMessage(content, receivers);
    setIsSubmitting(false);
    if (success) {
      setContent('');
      setReceivers('');
    }
  }

  return (
    <Form onSubmit={submitMessage}>
      <Input
        fluid
        transparent
        readOnly={isSubmitting}
        loading={isSubmitting}
        placeholder='Choose friends'
        value={receivers}
        onChange={(event) => setReceivers(event.currentTarget.value)}
      />
      <br />
      <Input
        fluid
        transparent
        readOnly={isSubmitting}
        loading={isSubmitting}
        placeholder="Write your message"
        value={content}
        onChange={(event) => setContent(event.currentTarget.value)}
      />
      <br />
      <Button type="submit">Send</Button>
    </Form>
  );
};

export default MessageEdit;
