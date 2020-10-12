# Review guidelines

> Reviewing code is hard and usually less pleasant than writing it, so first of
> all thanks for engaging in this (necessary) activity!

## Hard rules

1. No code can get into master without having been approved by at least one
   reviewer, who must not be an author of the code. This is enforced by GitHub.
2. Approvers are just as responsible for the consequences of the PR as the
   author.
3. Standard Changes have to be approved by a team lead.
4. External contributions have to be approved by a team lead.

## Guidelines

### Fitness

The most basic thing to check is whether or not the proposed change makes our
product better, i.e. more fit for use by our users. The specific meaning here
can vary depending on the type of work; refactorings should make the code
better without any observable change in behaviour, new features should match
tickets, etc.

### General code quality

Check for overall code quality: is the code unnecessarily reimplementing a
stdlib function? Is the code in the wrong complexity class? Is there a better
name for some function or variable? Do you find it hard to read, and do you
have a better alternative to suggest?

It is left to your better judgement how far to go there; remember that we are
trying to move the product forward overall, and not all code will end up being
written exactly in th way you would have written it.

### Dependencies

Any change in dependencies should be treated with special care. The discussion
on the PR should reflect a reasonable attempt at validating the trustworthiness
of the provider as well as the safety of the new dependency.

Dependencies are not free; sometimes it is better to reimplement the one
function you need than to pull in a whole library.

Removing dependencies is fine and does not necessitate special attention.

### Security

Always ask yourself whether the code you're reviewing can have any security
implication. In particular, but non-exhaustively:

- Think about injection attacks. Is any input evaluated? Don't trust user
  inputs, don't trust database inputs. Any kind of serialized data is also
  "user input" for this purpose.
- Think about availability. If the code you are reviewing is part of a shared,
  running service, is there any way this PR could open it up to a DoS attack?
  Are all resources properly closed, kept for the minimum amount of time? Is
  there some form of backpressure the service can apply?

### Testing

Aiming for 100% test coverage is not always healthhy, but there should
generally be a reason for any given piece of code not to be tested. If the code
is reasonably testable, new code should ome with new tests and changes to
existing code should come with changes to existing tests.

### Backwards compatibility

Always ask yourself: is this going to change any existing behaviour? If so, the
PR should include instructions to migrate (typically in the CHANGELOG block),
or be changed to not change existing behaviour if the change is considered too
breaking.
