import * as http from "http";
import * as prom from "prom-client";
import * as io from "socket.io";

export function metrics(ioServer: io.Server, options?: IMetricsOptions) {
    return new SocketIOMetrics(ioServer, options);
}

export interface IMetricsOptions {
    port?: number | string;
    path?: string;
    collectDefaultMetrics?: boolean
    checkForNewNamespaces?: boolean
}

export interface IMetrics {
    connectedSockets: prom.Gauge;
    connectTotal: prom.Counter;
    disconnectTotal: prom.Counter;
    eventsReceivedTotal: prom.Counter;
    eventsSentTotal: prom.Counter;
    bytesReceived: prom.Counter;
    bytesTransmitted: prom.Counter;
    errorsTotal: prom.Counter;
}

export class SocketIOMetrics {
    public register: prom.Registry;
    public metrics: IMetrics;

    private ioServer: io.Server;

    private options: IMetricsOptions;

    private boundNamespaces = new Set();

    private defaultOptions: IMetricsOptions = {
        port: 9090,
        path: '/metrics',
        collectDefaultMetrics: false,
        checkForNewNamespaces: true
    };

    constructor(ioServer: io.Server, options?: IMetricsOptions) {
        this.options = { ...this.defaultOptions, ...options };
        this.ioServer = ioServer;
        this.register = prom.register;

        this.initMetrics();
        this.bindMetrics();

        if (this.options.collectDefaultMetrics) {
            prom.collectDefaultMetrics({
                register: this.register
            });
        }

    }

    /*
    * Metrics logic
    */

    private initMetrics() {
        this.metrics = {
            connectedSockets: new prom.Gauge({
                name: 'socket_io_connected',
                help: 'Number of currently connected sockets'
            }),

            connectTotal: new prom.Counter({
                name: 'socket_io_connect_total',
                help: 'Total count of socket.io connection requests',
                labelNames: ['namespace']
            }),

            disconnectTotal: new prom.Counter({
                name: 'socket_io_disconnect_total',
                help: 'Total count of socket.io disconnections',
                labelNames: ['namespace']
            }),

            eventsReceivedTotal: new prom.Counter({
                name: 'socket_io_events_received_total',
                help: 'Total count of socket.io received events',
                labelNames: ['event', 'namespace']
            }),

            eventsSentTotal: new prom.Counter({
                name: 'socket_io_events_sent_total',
                help: 'Total count of socket.io sent events',
                labelNames: ['event', 'namespace']
            }),

            bytesReceived: new prom.Counter({
                name: 'socket_io_receive_bytes',
                help: 'Total socket.io bytes received',
                labelNames: ['event', 'namespace']
            }),

            bytesTransmitted: new prom.Counter({
                name: 'socket_io_transmit_bytes',
                help: 'Total socket.io bytes transmitted',
                labelNames: ['event', 'namespace']
            }),

            errorsTotal: new prom.Counter({
                name: 'socket_io_errors_total',
                help: 'Total socket.io errors',
                labelNames: ['namespace']
            })
        };
    }

    private bindMetricsOnEmitter(namespaceServer: io.Namespace, labels: prom.labelValues) {
        const blacklisted_events = new Set([
            'error',
            'connect',
            'disconnect',
            'disconnecting',
            'newListener',
            'removeListener'
        ]);

        namespaceServer.on('connection', (socket: any) => {
            // Connect events
            this.metrics.connectTotal.inc(labels);
            this.metrics.connectedSockets.set((this.ioServer.engine as any).clientsCount);

            // Disconnect events
            socket.on('disconnect', () => {
                this.metrics.disconnectTotal.inc(labels);
                this.metrics.connectedSockets.set((this.ioServer.engine as any).clientsCount);
            });

            // Hook into emit (outgoing event)
            const org_emit = socket.emit;
            socket.emit = (event: string, ...data: any[]) => {
                if (!blacklisted_events.has(event)) {
                    let labelsWithEvent = { event: event, ...labels };
                    this.metrics.bytesTransmitted.inc(labelsWithEvent, this.dataToBytes(data));
                    this.metrics.eventsSentTotal.inc(labelsWithEvent);
                }

                return org_emit.apply(socket, [event, ...data]);
            };

            // Hook into onevent (incoming event)
            const org_onevent = socket.onevent;
            socket.onevent = (packet: any) => {
                if (packet && packet.data) {
                    const [event, data] = packet.data;

                    if (event === 'error') {
                        this.metrics.connectedSockets.set((this.ioServer.engine as any).clientsCount);
                        this.metrics.errorsTotal.inc(labels);
                    } else if (!blacklisted_events.has(event)) {
                        let labelsWithEvent = { event: event, ...labels };
                        this.metrics.bytesReceived.inc(labelsWithEvent, this.dataToBytes(data));
                        this.metrics.eventsReceivedTotal.inc(labelsWithEvent);
                    }
                }

                return org_onevent.call(socket, packet);
            };
        });
    }

    private bindNamespaceMetrics(nsp: io.Namespace, namespace: string) {
        if (this.boundNamespaces.has(namespace)) {
            return;
        }
        this.bindMetricsOnEmitter(nsp, { namespace: namespace });
        this.boundNamespaces.add(namespace);
    }

    private bindMetrics() {
        this.ioServer._nsps.forEach((nsp, nspName) =>
            this.bindNamespaceMetrics(nsp, nspName)
        );

        if (this.options.checkForNewNamespaces) {
            setInterval(() => {
                this.ioServer._nsps.forEach((nsp, nspName) =>
                    this.bindNamespaceMetrics(nsp, nspName)
                );
            }, 2000);
        }
    }

    /*
    * Helping methods
    */

    private dataToBytes(data: any) {
        try {
            return Buffer.byteLength((typeof data === 'string') ? data : JSON.stringify(data) || '', 'utf8');
        } catch (e) {
            return 0;
        }
    }
}
