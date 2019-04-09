ARG VERSION=0.11.19

FROM "digitalasset/daml-sdk:${VERSION}-master"

RUN da list

USER root
# Install VS Code's deps, libxkbfile-dev and libsecret-1-dev
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash - &&\
    apt-get update && apt-get install -y \
        nodejs \
        libxkbfile-dev \
        libsecret-1-dev

USER sdk
ENV VERSION=0.11.19 \
    CODESERVER_SHA256=355463f35b9c355cf99ea2f1d87367194daae8aa4fdabaace4a9ea2f1b0490f6
RUN mkdir -p /home/sdk/.code-server/extensions/ &&\
    mkdir /home/sdk/workspace &&\
    curl -Lo - "https://github.com/codercom/code-server/releases/download/1.408-vsc1.32.0/code-server1.408-vsc1.32.0-linux-x64.tar.gz" | tar xzvf - --strip-components 1 "code-server1.408-vsc1.32.0-linux-x64" &&\
    echo "${CODESERVER_SHA256} code-server" | sha256sum -c &&\
    mv ./code-server /home/sdk/.da/bin/ &&\
    ln -s /home/sdk/.da/packages/daml-extension/10${VERSION} /home/sdk/.code-server/extensions/da-vscode-daml-extension &&\
    cp -R /home/sdk/.da/packages/quickstart-java/10${VERSION}/daml/* /home/sdk/workspace/ 

WORKDIR /home/sdk/workspace
EXPOSE 8443
LABEL WEB-IDE=""
CMD ["code-server", "--no-auth", "--allow-http"]
