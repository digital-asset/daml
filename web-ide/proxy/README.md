# DAML WEB IDE proxy
This proxies the docker hosted web ide. It spins up a docker instance for each user and forwards http and websocket connections.
Session state is managed by cookie (webide.connect.sid by default)

### Running
node proxy.js