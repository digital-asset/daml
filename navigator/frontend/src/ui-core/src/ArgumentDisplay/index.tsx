// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import { DamlLfValue } from '../api/DamlLfValue';
import * as DamlLfValueF from '../api/DamlLfValue';
import { LabeledElement } from '../Label';
import NestedForm from '../NestedForm';
import { hardcodedStyle } from '../theme';
import { NonExhaustiveMatch } from '../util';

// What parameters will actually be allowed is not yet decided so the types here
// are a bit messy and should be cleaned up when that has be clarified in DAML.

export interface Props {
  argument: DamlLfValue;
  level?: number;
  className?: string;
}

const ArgumentDisplay = (props: Props): JSX.Element => {
  const { argument, className, level = 0 } = props;
  switch (argument.type) {
    case 'record': {
      return (
        <NestedForm level={level}>
          {argument.fields.map((f) => (
            <LabeledElement key={f.label} label={f.label} className={className}>
              <ArgumentDisplay
                argument={f.value}
                level={level + 1}
              />
            </LabeledElement>
          ))}
        </NestedForm>
      );
    }
    case 'variant': {
      return (
        <NestedForm level={level}>
          <LabeledElement key={'type'} label={`Type (${argument.id.name})`} className={className}>
            <span>{argument.constructor}</span>
          </LabeledElement>
          <LabeledElement key={'value'} label={'Value'} className={className}>
            <ArgumentDisplay
              argument={argument.value}
              level={level + 1}
            />
          </LabeledElement>
        </NestedForm>
      );
    }
    case 'list': {
      return (
        <NestedForm level={level}>
          {argument.value.length > 0 ? argument.value.map((k, i) => (
            <LabeledElement key={i} label={`[${i}]`} className={className}>
              <ArgumentDisplay
                argument={k}
                level={level + 1}
              />
            </LabeledElement>
          )) : <span>(empty list)</span>}
        </NestedForm>
      );
    }
    case 'optional': {
      if (argument.value === null) {
        return <span>(None)</span>
      } else {
          return (
            <ArgumentDisplay
              argument={argument.value}
              level={level}
            />
        );
      }
    }
    case 'text': return <span>{argument.value}</span>;
    case 'party': return <span>{argument.value}</span>;
    case 'contractid': return <span>{argument.value}</span>;
    case 'decimal': return <span>{argument.value}</span>;
    case 'int64': return <span>{argument.value}</span>;
    case 'timestamp': {
      const moment = DamlLfValueF.toMoment(argument);
      if (moment) {
        return <span>{moment.format(hardcodedStyle.defaultTimeFormat)}</span>;
      } else {
        return <span>{argument.value}</span>;
      }
    }
    case 'date': {
      const moment = DamlLfValueF.toMoment(argument);
      if (moment) {
        return <span>{moment.format(hardcodedStyle.defaultDateFormat)}</span>;
      } else {
        return <span>{argument.value}</span>;
      }
    }
    case 'bool': return <span>{argument.value ? 'TRUE' : 'FALSE'}</span>;
    case 'unit': return <span>unit</span>;
    case 'map' : return (
      <NestedForm level={level}>
        {argument.value.length > 0 ? argument.value.map((entry, _) => (
          <LabeledElement key={entry.key} label={` value for key '${entry.key}'`} className={className}>
            <ArgumentDisplay
              argument={entry.value}
              level={level + 1}
            />
          </LabeledElement>
        )) : <span>(empty map)</span>}
      </NestedForm>
    );
    case 'undefined': return <span>???</span>;
  }
  throw new NonExhaustiveMatch(argument);
};

export default ArgumentDisplay;
