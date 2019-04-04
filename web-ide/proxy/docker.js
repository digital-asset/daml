// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const Docker    = require('dockerode'),
      fs        = require('fs')
      docker    = new Docker(),

module.exports = {
    getOde: getOde,
    startContainer: startContainer,
    stopContainer: stopContainer,
    getImage: getImage
}

ensureDocker()

function getOde() {
    return docker
}

function getImage(imageId) {
    return docker.getImage(imageId).inspect()
}

/**
 * Starts the container and returns a promise when the container is initialized
 * @param {*} imageId 
 */
function startContainer (imageId) {
    return new Promise((resolve, reject) => {
        //TODO open ports
        const createOptions = { 
            HostConfig: { PublishAllPorts : true }
        }
        docker.run(imageId, ["code-server", "--no-auth", "--allow-http"], process.stdout, createOptions, function (err, data, container) {
            if (err) reject(err) 
        })
        .on('start', function(container) {
            console.log(`started container ${container.id} ... waiting for coder-server to come up`)
            //TODO remove static wait (perhaps scrape logs for "Connected to shared process")
            const wait = setTimeout(() => {
                clearTimeout(wait);
                resolve(container.inspect())
            }, 6000)
        })
        .on('container', function (container) {
            console.log(`created container ${container.id}`)       
        })
        .on('stream', function(stream) {
            //TODO create context aware log messages per container (including the user session info)
            console.log(`attached stream to container`)
        })
        .on('data', function(data) {
            //this occurs upon termination of container
            console.log("container stopped: %j",data);
        })
        .on('error', function(err) { reject(err) })
    })
}

function stopContainer(containerId) {
    docker.getContainer(containerId).remove({force: true})
}

function ensureDocker() {
    const socket = process.env.DOCKER_SOCKET || '/var/run/docker.sock'
    const stats  = fs.statSync(socket)

    if (!stats.isSocket()) {
        throw new Error('Are you sure the docker is running?')
    }
}
