### DAML WEB IDE
This is a project for managing multi user web ide. The main components are
* daml web ide docker image: this hosts a [web ide code-server](https://github.com/codercom/code-server) bundled with the sdk
* proxy server and image: this manages multiple daml web ide docker containers and proxies requests into them

Further details can be found in `proxy/README.md` and `ide-server/README.md`
