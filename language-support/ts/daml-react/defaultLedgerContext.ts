
import createLedgerContext from "./createLedgerContext";

export const { DamlLedger, useParty, useLedger, useQuery, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload } = createLedgerContext();