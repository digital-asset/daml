import type {components} from '../generated/api/ledger-api';
import Table from "cli-table3";

export function showAcs(created: components["schemas"]["CreatedEvent"][],
                 headers: string[],
                 values: (arg: any) => any[],
) {
    const table = new Table({
        head: headers,
    });
    created.map(c => c.createArgument as any).forEach(c => table.push(values(c)));
    console.log(table.toString());
}
