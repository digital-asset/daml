ARG VERSION=0.11.19

FROM "digitalasset/daml-sdk:${VERSION}-master"

RUN da list

USER root
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash - &&\
    # Install VS Code's deps. These are the only two it seems we need.
    apt-get update && apt-get install -y \
        nodejs \
	libxkbfile-dev \
        libsecret-1-dev \
	net-tools 

USER sdk
ENV VERSION=0.11.19 \
    CODESERVER_SHA256=902b2b56ff9e41bb3d9f5dafd23a9f1a09655a1a112cf3d583fe5d92e63b718a
RUN mkdir -p /home/sdk/.code-server/extensions/ &&\
    mkdir /home/sdk/workspace &&\
    curl -Lo - "https://github.com/codercom/code-server/releases/download/1.32.0-310/code-server-1.32.0-310-linux-x64.tar.gz" | tar xzvf - --strip-components 1 "code-server-1.32.0-310-linux-x64/code-server" &&\
    echo "${CODESERVER_SHA256} code-server" | sha256sum -c &&\
    mv ./code-server /home/sdk/.da/bin/ &&\
    ln -s /home/sdk/.da/packages/daml-extension/10${VERSION} /home/sdk/.code-server/extensions/da-vscode-daml-extension &&\
    cp -R /home/sdk/.da/packages/quickstart-java/10${VERSION}/daml/* /home/sdk/workspace/ 

WORKDIR /home/sdk/workspace
EXPOSE 8443
LABEL WEB-IDE=""
CMD ["code-server", "--no-auth", "--allow-http"]
