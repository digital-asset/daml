FROM openjdk:8-slim

ARG DAML_VERSION=0.12.21
ARG CODESERVER_SHA256=4fe5b4d10d3048a5e5aa3e0772c09ece56241b91e76eeaa7404b4da292442881
ARG CODESERVER_VERSION=1.939-vsc1.33.1

USER root

RUN addgroup --gid 1000 sdk && \
    adduser --gecos "" --uid 1000 --gid 1000 \
            --home /home/sdk --shell /bin/bash \
            --disabled-password \
            sdk

RUN apt-get update -y && \
        apt-get -y install \
        curl \
        procps \
        nodejs \
        # Install VS Code's deps, libxkbfile-dev and libsecret-1-dev
        libxkbfile-dev \ 
        libsecret-1-dev &&\
    curl -sL https://deb.nodesource.com/setup_8.x | bash - &&\
    apt-get clean &&\
    apt-get remove -y --allow-remove-essential apt
 
USER sdk
ENV PATH="${PATH}:/home/sdk/.daml/bin"
    
RUN cd /home/sdk/ &&\
    curl -Lo - "https://github.com/digital-asset/daml/releases/download/v${DAML_VERSION}/daml-sdk-${DAML_VERSION}-linux.tar.gz" | tar xzvf - --strip-components 1 &&\
    bash install.sh &&\
    mkdir -p /home/sdk/.local/share/code-server/extensions/ &&\
    mkdir -p /home/sdk/.local/share/code-server/User/ &&\
    mkdir /home/sdk/workspace &&\
    curl -Lo - "https://github.com/codercom/code-server/releases/download/${CODESERVER_VERSION}/code-server${CODESERVER_VERSION}-linux-x64.tar.gz" | tar xzvf - --strip-components 1 "code-server${CODESERVER_VERSION}-linux-x64/code-server" &&\
    echo "${CODESERVER_SHA256} code-server" | sha256sum -c &&\
    mv ./code-server /home/sdk/.daml/bin/ &&\
    ln -s /home/sdk/.daml/sdk/${DAML_VERSION}/studio /home/sdk/.local/share/code-server/extensions/da-vscode-daml-extension &&\
    cp -R /home/sdk/.daml/sdk/${DAML_VERSION}/templates/quickstart-java/daml/* /home/sdk/workspace/ 

COPY ./settings.json /home/sdk/.local/share/code-server/User/settings.json
# Don't let the user change the keybindings file
COPY --chown=root:root ./keybindings.json /home/sdk/.local/share/code-server/User/keybindings.json

WORKDIR /home/sdk/workspace
EXPOSE 8443
LABEL WEB-IDE=""
CMD ["code-server", "--no-auth", "--allow-http", "--disable-telemetry"]
