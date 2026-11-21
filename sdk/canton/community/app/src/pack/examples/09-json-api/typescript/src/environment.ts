const DEFAULT_PARTICIPANT_HOST = "localhost";
const DEFAULT_PARTICIPANT_PORT = "7575";

function envOrDefault(name: string, def: string): string {
    return process?.env?.[name] || def
}

export const PARTICIPANT_HOST = envOrDefault("PARTICIPANT_HOST", DEFAULT_PARTICIPANT_HOST);
export const PARTICIPANT_PORT = envOrDefault("PARTICIPANT_PORT", DEFAULT_PARTICIPANT_PORT);
