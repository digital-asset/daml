// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const Docker    = require('dockerode'),
      fs        = require('fs'),
      { URL } = require('url'),
      config = require('./config.json')

const docker    = new Docker()

module.exports = {
    getImage: getImage,
    getOde: getOde,
    init : init,
    getContainerUrl : getContainerUrl,
    startContainer: startContainer,
    stopContainer: stopContainer
}

ensureDocker()

function getOde() {
    return docker
}

function getImage(imageId) {
    return docker.getImage(imageId).inspect()
}

function init() {
    const webIdeNetwork = config.docker.hostConfig.NetworkMode ? config.docker.hostConfig.NetworkMode : 'bridge'
    if (!onInternalNetwork()) {
        console.log("running web ide containers on network[%s], this is a non-internal network and is only suitable for local development", webIdeNetwork)
        return Promise.resolve()
    }

    const initNetworksP = docker.listNetworks()
    .then(networks => {
        //create networks if they don't exist
        const internalName = config.docker.internalNetwork,
              externalName = config.docker.externalNetwork,
              hasInternal = networks.some(n => n.Name === internalName),
              hasExternal = networks.some(n => n.Name === externalName),
              internalNetworkP = hasInternal ? getNetwork(networks, internalName) : createNetwork(internalName, true),
              externalNetworkP = hasExternal ? getNetwork(networks, externalName) : createNetwork(externalName, false)
        return Promise.all([internalNetworkP, externalNetworkP])
    })
    const proxyIdP = docker.listContainers({all: false, filters: { label: [config.docker.proxyLabel] }})
    .then(containers => { 
        if (containers.length !== 1) return new Error(`Problems finding web ide proxy. Found ${containers.length} instances labelled with ${config.docker.proxyLabel}`)
        return containers[0].Id
    })

    return Promise.all([initNetworksP, proxyIdP])
    .then(all => {
        const networkIds = all[0]
        const proxyContainerId = all[1]
        networkIds.forEach(id => console.log("connecting container[%s] to network[%s]", proxyContainerId, id))
        return Promise.all(networkIds.map(id => {
            return docker.getNetwork(id).connect({Container: proxyContainerId})
        }))
    })
}

function onInternalNetwork() {
    const webIdeNetwork = config.docker.hostConfig.NetworkMode ? config.docker.hostConfig.NetworkMode : 'bridge'
    return config.docker.internalNetwork === webIdeNetwork
}

function getNetwork(networks, name) {
    return Promise.resolve(networks.find(n => n.Name === name).Id)
}

function createNetwork(name, internal) {
    console.log("creating %s network %s", internal ? 'internal' : 'external', name)
    return docker.createNetwork({
        Name: name,
        Internal: internal
    }).then(n => n.id)
}

/**
 * Starts the container and returns a promise when the container is initialized
 * @param {*} imageId 
 */
function startContainer (imageId) {
    return new Promise((resolve, reject) => {
        const createOptions = { HostConfig: config.docker.hostConfig }
        if (!onInternalNetwork() && config.devMode) createOptions.HostConfig.PublishAllPorts=true

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
        throw new Error('Are you sure the docker daemon is running?')
    }
}

function getContainerUrl(containerInfo, protocol) {
    if (onInternalNetwork()) {
        const ip = containerInfo.NetworkSettings.Networks[`${config.docker.internalNetwork}`].IPAddress
        return new URL(`${protocol}://${ip}:8443`)
    } else {
        const containerPort = containerInfo.NetworkSettings.Ports['8443/tcp']
        if (containerPort === undefined) throw "Missing coder-server port[8443] mapping on container"
        return new URL(`${protocol}://0.0.0.0:${containerPort[0].HostPort}`)
    }

    
}
