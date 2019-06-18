// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import DockerOde, { NetworkInspectInfo, ContainerInspectInfo, Container } from "dockerode"
import fs  from "fs"
import * as URL from "url"
import { Stream } from "stream";

const config = require('./config').read(),
      debug = require('debug')('webide:docker')

ensureDocker()

export default class Docker {
    public api: DockerOde    
    onInternalNetwork: boolean
    constructor() {
        this.api  = new DockerOde()    
        const webIdeNetwork = config.docker.hostConfig.NetworkMode ? config.docker.hostConfig.NetworkMode : 'bridge'
        this.onInternalNetwork = config.docker.internalNetwork === webIdeNetwork
    }

    getImage(imageId :string) {
        return this.api.getImage(imageId).inspect()
    }

    init() : Promise<any[]>{
        debug("initializing docker")
        const webIdeNetwork = config.docker.hostConfig.NetworkMode ? config.docker.hostConfig.NetworkMode : 'bridge'
        if (!this.onInternalNetwork) {
            console.log("INFO running web ide containers on network[%s], this is a non-internal network and is only suitable for local development", webIdeNetwork)
            return Promise.resolve([])
        }
    
        const initNetworksP = this.api.listNetworks()
        .then(networks => {
            //create networks if they don't exist
            const internalName = config.docker.internalNetwork,
                  externalName = config.docker.externalNetwork,
                  hasInternal = networks.some(n => n.Name === internalName),
                  hasExternal = networks.some(n => n.Name === externalName),
                  internalNetworkP = hasInternal ? this.getNetwork(networks, internalName) : this.createNetwork(internalName, true),
                  externalNetworkP = hasExternal ? this.getNetwork(networks, externalName) : this.createNetwork(externalName, false)
            return Promise.all([internalNetworkP, externalNetworkP])
        })
        debug("getting proxy by image %s", config.docker.proxyReference)
        const proxyIdP = this.api.listContainers({all: false, filters: { ancestor: [config.docker.proxyReference] }})
        .then(containers => { 
            if (containers.length !== 1) throw new Error(`Found ${containers.length} instances referenced by ${config.docker.proxyReference}. Make sure you're running a single docker instance of the proxy`)
            return containers[0].Id
        })
    
        return Promise.all([initNetworksP, proxyIdP])
        .then(all => {
            const networkIds = all[0]
            const proxyContainerId = all[1]
            networkIds.forEach(id => debug("connecting container[%s] to network[%s]", proxyContainerId, id))
            return Promise.all(networkIds.map(id => {
                return this.api.getNetwork(id).connect({Container: proxyContainerId})
            }))
        })
    }

    getContainerUrl(containerInfo :ContainerInspectInfo, protocol :string) {
        if (this.onInternalNetwork) {
            const ip = containerInfo.NetworkSettings.Networks[`${config.docker.internalNetwork}`].IPAddress
            return URL.parse(`${protocol}://${ip}:8443`)
        } else {
            const containerPort = containerInfo.NetworkSettings.Ports['8443/tcp']
            if (containerPort === undefined) throw "Missing coder-server port[8443] mapping on container"
            return URL.parse(`${protocol}://0.0.0.0:${containerPort[0].HostPort}`)
        }
    }

    startContainer (imageId :string) :Promise<ContainerInspectInfo>{
        return new Promise((resolve, reject) => {
            const createOptions = { HostConfig: config.docker.hostConfig }
            if (!this.onInternalNetwork && config.devMode) createOptions.HostConfig.PublishAllPorts=true

            debug("sending run api command")
            const runner = this.api.run(imageId, ["code-server", "--no-auth", "--allow-http", "--disable-telemetry"], [], createOptions, (err :any, result :any) => {
                if (err) reject(err) 
            })
            const containerP :Promise<Container> = new Promise((cResolve, _) => {
                runner.on('start', (container :Container) => {
                    console.log(`INFO started container ${container.id} ... waiting for coder-server to come up`)
                    cResolve(container)
                })
            })
            runner.on('container', (container :Container) => { debug(`created container ${container.id}`) })
            runner.on('stream', (stream :Stream) => {
                let started = false
                stream.pipe(process.stdout) //TODO should we create context aware log messages per container (including the user session info)??
                stream.on('data', (chunk :any) => {
                    if (started) return
                    const line = chunk instanceof Buffer ? chunk.toString() : (chunk instanceof String ? chunk : typeof chunk)
                    if (line.includes("Connected to shared process")) {
                        containerP.then(container => {
                            started = true
                            resolve(container.inspect())
                        })    
                    }
                });
                stream.on('error', (error :any) => reject(error));
            })
            runner.on('error', (err :any) => reject(err))
        })
    }

    stopContainer(containerId :string) {
        this.api.getContainer(containerId).remove({force: true})
    }
    
    private getNetwork(networks :NetworkInspectInfo[], name :string) {
        const n = networks.find(n => n.Name === name)
        return n ? Promise.resolve(n.Id) : Promise.reject(new Error(`No network found with name: "${name}"`))
    }
    
    private createNetwork(name :string, internal :boolean) {
        debug("creating %s network %s", internal ? 'internal' : 'external', name)
        return this.api.createNetwork({
            Name: name,
            Internal: internal
        }).then(n => n.id)
    }
}

function ensureDocker() {
    const socket = process.env.DOCKER_SOCKET || '/var/run/docker.sock'
    const stats  = fs.statSync(socket)

    if (!stats.isSocket()) {
        throw new Error(`Are you sure the docker daemon is running? Could not find ${socket}`)
    }
}
