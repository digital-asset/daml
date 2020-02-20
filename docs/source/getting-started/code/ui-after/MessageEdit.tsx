// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from 'react'
import { Form, Input, Dropdown, Button } from 'semantic-ui-react';
import { Party } from '@daml/types';
import { User } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';
import { useParty, useExerciseByKey } from '@daml/react';

type Props = {
  friends: Party[];
}

/**
 * React component to edit a message to send to a friend.
 */
const MessageEdit: React.FC<Props> = ({friends}) => {
  const sender = useParty();
  const [receiver, setReceiver] = React.useState('');
  const [content, setContent] = React.useState('');
  const [exerciseSendMessage] = useExerciseByKey(User.SendMessage);
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const sendMessage = async (receiver: string, content: string): Promise<boolean> => {
    try {
      await exerciseSendMessage(receiver, {sender, content});
      return true;
    } catch (error) {
      alert("Error sending message:\n" + JSON.stringify(error));
      return false;
    }
  }

  const submitMessage = async (event?: React.FormEvent) => {
    if (event) {
      event.preventDefault();
    }
    setIsSubmitting(true);
    const success = await sendMessage(receiver, content);
    setIsSubmitting(false);
    if (success) {
      // Keep the receiver selected for follow-on messages
      // but clear the message text.
      setContent('');
    }
  }

  // Options for dropdown menu
  const friendOptions = friends.map(f => ({ key: f, text: f, value: f }));

  return (
    <Form onSubmit={submitMessage}>
      <Dropdown
        fluid
        selection
        placeholder='Select friend'
        options={friendOptions}
        value={receiver}
        onChange={(event) => setReceiver(event.currentTarget.textContent ?? '')}
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
