
import createLedgerContext from "./createLedgerContext";

/**
 * The default DamlLedger context and hooks to interact with it.
 */
export const { DamlLedger, useParty, useLedger, useQuery, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload } = createLedgerContext();