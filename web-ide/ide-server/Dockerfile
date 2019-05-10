ARG DAML_VERSION=0.12.16

FROM "digitalasset/daml-sdk:${DAML_VERSION}-master"

#strange but true...if we simply redeclare the arg defined before FROM it is visible after FROM
ARG DAML_VERSION

RUN da list
USER root
# Install VS Code's deps, libxkbfile-dev and libsecret-1-dev
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash - &&\
    apt-get update && apt-get install -y \
        procps \
        nodejs \
        libxkbfile-dev \
        libsecret-1-dev &&\
    apt-get remove -y --allow-remove-essential apt

USER sdk
ENV CODESERVER_SHA256=4fe5b4d10d3048a5e5aa3e0772c09ece56241b91e76eeaa7404b4da292442881\
    CODESERVER_VERSION=1.939-vsc1.33.1
RUN mkdir -p /home/sdk/.local/share/code-server/extensions/ &&\
    mkdir -p /home/sdk/.local/share/code-server/User/ &&\
    mkdir /home/sdk/workspace &&\
    curl -Lo - "https://github.com/codercom/code-server/releases/download/${CODESERVER_VERSION}/code-server${CODESERVER_VERSION}-linux-x64.tar.gz" | tar xzvf - --strip-components 1 "code-server${CODESERVER_VERSION}-linux-x64/code-server" &&\
    echo "${CODESERVER_SHA256} code-server" | sha256sum -c &&\
    mv ./code-server /home/sdk/.da/bin/ &&\
    ln -s /home/sdk/.da/packages/daml-extension/10${DAML_VERSION} /home/sdk/.local/share/code-server/extensions/da-vscode-daml-extension &&\
    cp -R /home/sdk/.da/packages/quickstart-java/10${DAML_VERSION}/daml/* /home/sdk/workspace/ 

COPY ./settings.json /home/sdk/.local/share/code-server/User/settings.json
# Don't let the user change the keybindings file
COPY --chown=root:root ./keybindings.json /home/sdk/.local/share/code-server/User/keybindings.json

WORKDIR /home/sdk/workspace
EXPOSE 8443
LABEL WEB-IDE=""
CMD ["code-server", "--no-auth", "--allow-http", "--disable-telemetry"]
