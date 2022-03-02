// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// This DateTimePicker code is based on the input-moment library.
// See https://github.com/wangzuo/input-moment.
// The license of this library is included in the current folder.
//
// The code was copied in order to simplify extensive customization.
// In the future, we may decide to use another library to implement
// this component.
import * as _ from "lodash";
import Moment from "moment";
import * as React from "react";
import Button from "../Button";
import UntypedIcon from "../Icon";
import styled from "../theme";

const weeks = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const isValid = (m: Moment.Moment | undefined): m is Moment.Moment =>
  m !== undefined && m.isValid();

const isSameMonth = (
  m1: Moment.Moment | undefined,
  m2: Moment.Moment | undefined,
) => isValid(m1) && isValid(m2) && m1.format("MMYYYY") === m2.format("MMYYYY");

const CalendarCell = styled.td`
  padding: 8px 0;
  text-align: center;
  cursor: pointer;
`;

const SelectedDay = styled(CalendarCell)`
  color: ${({ theme }) => theme.colorPrimary[1]};
  background-color: ${({ theme }) => theme.colorPrimary[0]};
`;

const DefaultDay = styled(CalendarCell)`
  color: ${({ theme }) => theme.colorForeground};
  font-weight: 700;
`;

const OtherMonthDay = styled(CalendarCell)`
  color: ${({ theme }) => theme.colorFaded};
`;

const ThisMonthDay = styled(CalendarCell)`
  color: ${({ theme }) => theme.colorForeground};
`;

const CalendarHeaderCell = styled.th`
  font-weight: initial;
  width: 14.2857%;
  color: ${({ theme }) => theme.colorFaded};
  padding: 4px 4px;
`;

const CalendarHeaderRow = styled.tr`
  border-bottom: 1px solid ${({ theme }) => theme.colorFaded};
`;
const CalendarFooterRow = styled.tr`
  border-bottom: 1px solid ${({ theme }) => theme.colorFaded};
`;

const CallendarTable = styled.table`
  border-collapse: collapse;
  width: 100%;
`;

const CalendarWrapper = styled.div`
  width: 100%;
`;

const CalendarRow = styled.tr`
  :nth-child(1) {
    margin-top: 4px;
  }
`;

const CurrentMonth = styled.div`
  font-weight: 700;
  text-align: center;
  flex: 1;
`;

const ToolbarWrapper = styled.div`
  display: flex;
  flex-flow: row nowrap;
  align-items: center;
  align-content: flex-start;
  justify-content: center;
  padding-bottom: 15px;
`;

interface DayProps {
  i: number;
  w: number;
  d: number | null;
  f: number | null;
  onClick(): void;
}

const Day = ({ i, w, d, f, ...props }: DayProps) => {
  const prevMonth = w === 0 && i > 7;
  const nextMonth = w >= 4 && i <= 14;

  if (!prevMonth && !nextMonth && i === d) {
    return <SelectedDay {...props}>{i}</SelectedDay>;
  } else if (!prevMonth && !nextMonth && i === f) {
    return <DefaultDay {...props}>{i}</DefaultDay>;
  } else if (prevMonth || nextMonth) {
    return <OtherMonthDay {...props}>{i}</OtherMonthDay>;
  } else {
    return <ThisMonthDay {...props}>{i}</ThisMonthDay>;
  }
};

export interface Props {
  moment: Moment.Moment | undefined;
  defaultMoment: Moment.Moment;
  onChange(moment: Moment.Moment): void;
}

export interface State {
  displayMoment: Moment.Moment;
}

interface ToolbarProps {
  title: string;
  next(): void;
  prev(): void;
}

const Toolbar = ({ title, next, prev }: ToolbarProps) => (
  <ToolbarWrapper>
    <Button
      type="minimal"
      onClick={e => {
        e.preventDefault();
        prev();
      }}>
      <UntypedIcon name="chevron-left" />
    </Button>
    <CurrentMonth>{title}</CurrentMonth>
    <Button
      type="minimal"
      onClick={e => {
        e.preventDefault();
        next();
      }}>
      <UntypedIcon name="chevron-right" />
    </Button>
  </ToolbarWrapper>
);

/**
 * Renders a calendar (date picker)
 *
 * Properties and state
 * - props.moment: The selected date.
 * - props.defaultMoment: The default date to show, if no date is selected.
 * - props.onChange: Callback when the user selects a new date.
 * - state.displayMoment: The displayed date, defines mainly which month
 *   the calendar is showing. This may not be equal to the selected date:
 *   - The calendar could have been openend with no selected date. In this
 *     case, the displayed date defaults to props.defaultMoment.
 *   - The user can navigate to other months without selecting a new date.
 */
export default class Calendar extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      displayMoment: isValid(props.moment) ? props.moment : props.defaultMoment,
    };
    this.prevMonth = this.prevMonth.bind(this);
    this.nextMonth = this.nextMonth.bind(this);
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props): void {
    if (
      isValid(this.props.moment) &&
      isValid(nextProps.moment) &&
      !this.props.moment.isSame(nextProps.moment)
    ) {
      this.setState({ displayMoment: nextProps.moment });
    } else if (!this.props.defaultMoment.isSame(nextProps.defaultMoment)) {
      this.setState({ displayMoment: nextProps.defaultMoment });
    }
  }

  selectDate(i: number, w: number): void {
    const prevMonth = w === 0 && i > 7;
    const nextMonth = w >= 4 && i <= 14;
    const m = Moment(this.state.displayMoment);

    if (prevMonth) {
      m.subtract(1, "month");
    }
    if (nextMonth) {
      m.add(1, "month");
    }

    m.date(i);

    this.props.onChange(m);
  }

  prevMonth(): void {
    const result = Moment(this.state.displayMoment);
    result.subtract(1, "month");
    this.setState({ displayMoment: result });
  }

  nextMonth(): void {
    const result = Moment(this.state.displayMoment);
    result.add(1, "month");
    this.setState({ displayMoment: result });
  }

  render(): JSX.Element {
    const m = this.state.displayMoment;
    const displayDate = isSameMonth(this.props.moment, this.state.displayMoment)
      ? (this.props.moment as Moment.Moment).date()
      : null;
    const defaultDate = isSameMonth(
      this.props.defaultMoment,
      this.state.displayMoment,
    )
      ? this.props.defaultMoment.date()
      : null;
    const d1 = m.clone().subtract(1, "month").endOf("month").date();
    const d2 = m.clone().date(1).day();
    const d3 = m.clone().endOf("month").date();
    const days = [
      // eslint-disable-next-line @typescript-eslint/restrict-plus-operands, , ,
      ..._.range(d1 - d2 + 1, d1 + 1),
      // eslint-disable-next-line @typescript-eslint/restrict-plus-operands
      ..._.range(1, d3 + 1),
      ..._.range(1, 42 - d3 - d2 + 1),
    ];

    return (
      <CalendarWrapper>
        <Toolbar
          title={m.format("MMMM YYYY")}
          prev={this.prevMonth}
          next={this.nextMonth}
        />
        <CallendarTable>
          <thead>
            <CalendarHeaderRow>
              {weeks.map((w, i) => (
                <CalendarHeaderCell key={i}>{w}</CalendarHeaderCell>
              ))}
            </CalendarHeaderRow>
          </thead>

          <tbody>
            {_.chunk(days, 7).map((row, w) => (
              <CalendarRow key={w}>
                {row.map(i => (
                  <Day
                    key={i}
                    i={i}
                    d={displayDate}
                    f={defaultDate}
                    w={w}
                    onClick={() => this.selectDate(i, w)}
                  />
                ))}
              </CalendarRow>
            ))}
          </tbody>
          <tfoot>
            <CalendarFooterRow>
              {weeks.map((_w, i) => (
                <td key={i} />
              ))}
            </CalendarFooterRow>
          </tfoot>
        </CallendarTable>
      </CalendarWrapper>
    );
  }
}
