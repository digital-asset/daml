// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ArgumentDisplay,
  Breadcrumbs,
  LongIdentifier,
  Strong,
  styled,
  Truncate,
} from "@da/ui-core";
import { DamlLfValue } from "@da/ui-core/lib/api/DamlLfValue";
import {
  shortenContractId,
  shortenPartyId,
  removePkgIdFromContractTypeId,
} from "@da/ui-core/lib/api/IdentifierShortening";
import * as React from "react";
import Link, { OwnProps } from "../../components/Link";
import * as Routes from "../../routes";
import { Contract } from "./";
import Exercise from "./Exercise";

const Wrapper = styled.div`
  width: 100%;
`;

const Header = styled.div`
  height: 5rem;
  display: flex;
  background-color: ${({ theme }) => theme.colorShade};
  font-size: 1.25rem;
  padding: 0;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
  align-items: center;
`;

export const SubHeader = styled.p`
  font-size: 1.25rem;
`;

const Content = styled.div`
  padding-left: 2.5rem;
  padding-right: 2.5rem;
`;

const ColumnContainer = styled.div`
  width: 100%;
  display: flex;
`;

const Column = styled.div`
  width: 50%;
`;

interface ActiveLinkProps extends OwnProps {
  isActive: boolean;
}

const ActiveLink: React.FC<ActiveLinkProps> = props => (
  <Link {...props}>{props.children}</Link>
);

const ChoiceLink = styled(ActiveLink)`
  display: flex;
  justify-content: space-between;
  font-size: 0.85rem;
  padding: 0.5em 1em;
  align-items: center;
  text-decoration: none;
  text-transform: capitalize;
  margin-left: 1rem;
  box-shadow: ${({ isActive }) =>
    isActive
      ? "0 0 0 1px rgba(16,22,26,0.1), 0 2px 4px rgba(16,22,26,0.2)"
      : "none"};
  &,
  &:hover {
    color: ${({ theme }) => theme.colorForeground};
  }
  border-radius: ${_ => "999rem"};
  background-color: ${({ isActive, theme }) =>
    isActive ? theme.colorBackground : theme.colorShade};
  &:hover {
    background-color: ${({ theme }) => theme.colorShade};
  }
`;

const AgreementText = ({ text }: { text: string }) => (
  <span>
    <SubHeader>
      <Strong>Agreement Text</Strong>
    </SubHeader>
    <span>{text}</span>
  </span>
);

const Parties = ({ title, parties }: { title: string; parties: string[] }) => (
  <span>
    <SubHeader>
      <Strong>{title}</Strong>
    </SubHeader>
    {parties.map(party => (
      <LongIdentifier
        key={party}
        text={shortenPartyId(party)}
        identifier={party}
      />
    ))}
  </span>
);

interface Props {
  contract: Contract;
  choice?: string;
  ifc?: string;
  choiceLoading: boolean;
  error?: string;
  exercise(
    e: React.MouseEvent<HTMLButtonElement>,
    argument?: DamlLfValue,
  ): void;
}

export default (props: Props): JSX.Element => {
  const { contract, choice, ifc, exercise, choiceLoading, error } = props;
  const choices = contract.template.choices;
  const isArchived = contract.archiveEvent !== null;
  let exerciseEl;
  let selectedChoice;

  if (choice) {
    selectedChoice = choices.filter(
      ({ name, inheritedInterface }) =>
        name === choice &&
        (ifc ? inheritedInterface === ifc : !inheritedInterface),
    )[0];
    const parameter = selectedChoice.parameter;

    exerciseEl = (
      <Exercise
        parameter={parameter}
        choice={choice}
        onSubmit={exercise}
        isLoading={choiceLoading}
        error={error}
      />
    );
  }

  const choicesEl = choices.map(({ name, inheritedInterface }) => {
    const interfacePrefix = inheritedInterface
      ? removePkgIdFromContractTypeId(inheritedInterface) + ":"
      : "";
    const fullChoiceName = `${interfacePrefix}${name}`;
    const isAnyActive = choice !== undefined;
    const isActive =
      name === choice &&
      (ifc ? inheritedInterface === ifc : !inheritedInterface);
    return (
      <ChoiceLink
        key={fullChoiceName}
        route={Routes.contract}
        params={{
          id: encodeURIComponent(contract.id),
          choice: name,
          ifc: inheritedInterface && encodeURIComponent(inheritedInterface),
        }}
        isActive={isActive || !isAnyActive}>
        <Strong>{interfacePrefix}</Strong> {name}
      </ChoiceLink>
    );
  });

  return (
    <Wrapper>
      <Header>
        <LongIdentifier
          text={`Contract ${shortenContractId(contract.id)}`}
          identifier={contract.id}
        />{" "}
        {choicesEl}
      </Header>
      <Content>
        <div>
          <Breadcrumbs>
            Template
            <Truncate>
              <Link
                route={Routes.template}
                params={{ id: contract.template.id }}>
                {contract.template.id}
              </Link>
            </Truncate>
          </Breadcrumbs>
        </div>
        {choice && selectedChoice && selectedChoice.inheritedInterface && (
          <div>
            <Breadcrumbs>
              Interface
              <Truncate>{selectedChoice.inheritedInterface}</Truncate>
            </Breadcrumbs>
          </div>
        )}
        <p>{isArchived ? "ARCHIVED" : null}</p>
        <ColumnContainer>
          <Column>
            {contract.agreementText && (
              <AgreementText text={contract.agreementText} />
            )}
            {contract.signatories.length > 0 && (
              <Parties title="Signatories" parties={contract.signatories} />
            )}
            {contract.observers.length > 0 && (
              <Parties title="Observers" parties={contract.observers} />
            )}
            {contract.key && (
              <SubHeader>
                <Strong>Contract key</Strong>
              </SubHeader>
            )}
            {contract.key && <ArgumentDisplay argument={contract.key} />}
            <SubHeader>
              <Strong>Contract details</Strong>
            </SubHeader>
            <ArgumentDisplay argument={contract.argument} />
          </Column>
          <Column>{exerciseEl}</Column>
        </ColumnContainer>
      </Content>
    </Wrapper>
  );
};
