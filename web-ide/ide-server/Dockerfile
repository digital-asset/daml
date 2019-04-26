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
    CODESERVER_SHA256=4fe5b4d10d3048a5e5aa3e0772c09ece56241b91e76eeaa7404b4da292442881\
    CODESERVER_VERSION=1.939-vsc1.33.1
RUN mkdir -p /home/sdk/.local/share/code-server/extensions/ &&\
    mkdir /home/sdk/workspace &&\
    curl -Lo - "https://github.com/codercom/code-server/releases/download/${CODESERVER_VERSION}/code-server${CODESERVER_VERSION}-linux-x64.tar.gz" | tar xzvf - --strip-components 1 "code-server${CODESERVER_VERSION}-linux-x64" &&\
    echo "${CODESERVER_SHA256} code-server" | sha256sum -c &&\
    mv ./code-server /home/sdk/.da/bin/ &&\
    ln -s /home/sdk/.da/packages/daml-extension/10${VERSION} /home/sdk/.local/share/code-server/extensions/da-vscode-daml-extension &&\
    cp -R /home/sdk/.da/packages/quickstart-java/10${VERSION}/daml/* /home/sdk/workspace/ 

WORKDIR /home/sdk/workspace
EXPOSE 8443
LABEL WEB-IDE=""
CMD ["code-server", "--no-auth", "--allow-http", "--disable-telemetry"]
