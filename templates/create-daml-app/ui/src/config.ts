export enum DeploymentMode {
  DEV,
  PROD_DABL,
  PROD_OTHER,
}

export const deploymentMode: DeploymentMode =
  process.env.NODE_ENV === 'development'
  ? DeploymentMode.DEV
  : window.location.hostname.endsWith('.projectdabl.com')
  ? DeploymentMode.PROD_DABL
  : DeploymentMode.PROD_OTHER;

export const ledgerId =
  deploymentMode === DeploymentMode.PROD_DABL
  ? window.location.hostname.split('.')[0]
  : 'create-daml-app-sandbox';

export const httpBaseUrl =
  deploymentMode === DeploymentMode.PROD_DABL
  ? `https://api.projectdabl.com/data/${ledgerId}/`
  : undefined;

// Unfortunately, the development server of `create-react-app` does not proxy
// websockets properly. Thus, we need to bypass it and talk to the JSON API
// directly in development mode.
export const wsBaseUrl =
  deploymentMode === DeploymentMode.DEV
  ? 'ws://localhost:7575/'
  : undefined;
