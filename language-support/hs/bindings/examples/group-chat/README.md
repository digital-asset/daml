
# `group-chat`

Daml Chat Room model, with support for:

- Multiple Chat Groups.
- Group entry by invitation.
- Messages containing recipient authority.

Currently there is no Ledger-App for this Model, but we should be able to knock one up fairly quickly, by taking the simple chat app (in: ../chat/) as a starting point.


## Features of the Domain

Below we list the features of (Chat) Messages and (Chat) Groups which we wish to model.
In essense we want simple WhatsApp style functionality.
Alternative models and extensions are considered in the next section.

Capitalised words here are the terms being defined, including: Message, Group, Member, Send, Sender, Recipient, Join, Leave, Invite.

Groups:

- A Group is a list of it's Member parties, who exchange Messages.
- As parties Join and Leave, the Members of a Group changes over time.
- To become a Group Member, a party is Invited by an existing Member of that Group.
- A party can Leave a Group at any time.
- No one else can force a party to Leave a Group.
- If a party Leaves a Group, they need to be re-Invited before they can Join again.
- There are no special privileges associated with being the creator of a Group.
- In particular, the creator of a Group may Leave the Group.
- New Groups can be created by any party.
- A Group must have a least one Member.
- When the last Member of a Group Leaves, the Group ceases to exist.
- There is no global operator of the chat rooms.

Messages:

- A Message is a body of text, sent by a party who is a Member of a Group, to all Members of that Group.
- The Recipients of a Message are those parties who are Members of the Group at the time the Message is sent.
- Only the Sender and Recipients of a Message have access to that Message.
- Only a Group Member may Send Messages to a Group.
- A party does not see Messages sent prior to Joining, or after Leaving, a Group.
- A Message is authorised by it's Sender and Recipients. (See discussion below.)


## Modelling with Daml

`GroupChat.daml` formalizes the above features in Daml. The essence of the model is as follows:

- A Message is modelled by a simple `Message` contract.
- A Group is modelled by a series of `Group` contracts, only one being active at a time.
- Group Members are modelled as a field of an active Group contract.
- Join, Leave, Invite and Send are choices of the Group template, with appropriate controllers.

Furthermore:

- A Group tracks it's members and invitees. The members are signatories; the invitees are observers.

- Having invitees as observers is necessary to allow an invited party to see the group which can be joined.

- As the signatories of a group are it's members, we don't support groups with no members.



## Alternative models

Here we consider alternative domain features and extensions not modelled by `GroupChat`.


### Group creator has special rights

For example:

- Be the only party able to invite new members.
- Have the right to evict other members.
- Be able to shutdown the group at any time.

These are all valid choices, but since this is just a demo, we choose the simpler model. In any case, one might argue that equal rights for all members is in some sense a *nicer* model.


### Groups with no members

This would be a slightly awkward, since a contract must have at least one signatory.

We might suport this feature by tracking specially the original creator of the group as a signatory, whilst allowing them not to be member.
Or else we might have some distinguished party representing the operator of the GroupChat framework, and have them be a signatory.

But we choose to avoid this complication by insisting on there being at least one member in every group. This doesn't seem to be a terrible restriction.


### Message recipients as observers, not signatories

The model described has the signatories of a `Message` be it's sender *and* recipients.

We might alternatively choose to have the recipients be merely observers: Everything would work as before, but now it also becomes possible for messages to be sent to any arbitrary set of recipients, without any reference to the groups.

His extra feature destroys much of the point of modelling groups in the first place. Groups become just a convenient collection of parties. Whereas the concept of Group we want to model is the following: When a party joins a group:

- (a) The party is autorized to send messages to the group.
- (b) The party is authorising the receipt of messages from the group.

Without recipient authority on the messages, we get only (a) but not (b).


### Group Identity

Do we need the concept of group identity?

With group identity, we can use Daml contract-keys to ensure every group has a unique identity, allowing discovery.
In addition, we can embed the group identity in each message sent.

The identity might be constructed from the creator party and a group-name.
This would allow different creators to choose the same group name, i.e. "MyGroup", but insist on
different groups from the same creator having different names.

However, it's not obvious that group-identity buys us very much here for all the added complication.
We can add a name or description to a Group without needing it to be a key.
And there is no fundamental need to lookup a group by it key, since all members and invitees are stakeholders of the group and can see the contact anyway.


### Addition data in Messages

- Date/time: This might be nice. We would like to ensure it can't be spoofed.
- Message identity: Not really necessary unless we want to provide additional functionality, such as message access by late joiners, as described in the next section.


### Message access by late joiners

Should we provide message access to parties who join a group after the original message is sent?
This functionality is over and above that provided by message systems such as WhatsApp.

It is a major change to functionality, and things become much more complicated.
A message must now have an identity.
We much track which messages are sent to which group.
Replacement messages must be constructed to allow late-joiners to see old messages.

Overall, this is quite a different concept of groups and messages from the basic `WhatsApp' style functionality we choose to model here.
