import React from 'react';
import MainView from './MainView';
import { Party } from '@digitalasset/daml-json-types';
import { User } from '../daml/create-daml-app/User';
// -- IMPORT_BEGIN
import { Message } from '../daml/create-daml-app/Message';
// -- IMPORT_END
import { useParty, useReload, usePseudoExerciseByKey, useFetchByKey, useQuery } from '../daml-react-hooks';

/**
 * React component to control the `MainView`.
 */
const MainController: React.FC = () => {
  const party = useParty();
  const myUser = useFetchByKey(User, () => party, [party]);
  const allUsers = useQuery(User, () => ({}), []);
  const reload = useReload();

  const [exerciseAddFriend] = usePseudoExerciseByKey(User.AddFriend);
  const [exerciseRemoveFriend] = usePseudoExerciseByKey(User.RemoveFriend);

// -- HOOKS_BEGIN
  const messages = useQuery(Message, () => ({}), []);
  const [exerciseSendMessage] = useExerciseByKey(User.SendMessage);
// -- HOOKS_END

  const addFriend = async (friend: Party): Promise<boolean> => {
    try {
      await exerciseAddFriend({party}, {friend});
      return true;
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
      return false;
    }
  }

  const removeFriend = async (friend: Party): Promise<void> => {
    try {
      await exerciseRemoveFriend({party}, {friend});
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
    }
  }

// -- SENDMESSAGE_BEGIN
  const sendMessage = async (content: string, parties: string): Promise<boolean> => {
    try {
      const receivers = parties.replace(/\s/g, "").split(",");
      await exerciseSendMessage({party}, {content, receivers});
      return true;
    } catch (error) {
      alert("Error while sending message:\n" + JSON.stringify(error));
      return false;
    }
  }
// -- SENDMESSAGE_END

  React.useEffect(() => {
    const interval = setInterval(reload, 5000);
    return () => clearInterval(interval);
  }, [reload]);

// -- PROPS_BEGIN
  const props = {
    myUser: myUser.contract?.payload,
    allUsers: allUsers.contracts.map((user) => user.payload),
    messages: messages.contracts.map((message) => message.payload),
    onAddFriend: addFriend,
    onRemoveFriend: removeFriend,
    onMessage: sendMessage,
    onReload: reload,
  };

  return MainView(props);
// -- PROPS_END
}

export default MainController;
