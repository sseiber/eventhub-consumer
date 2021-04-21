import { IAppConfig } from '../index';
import {
    EventHubConsumerClient,
    Subscription as EventHubConsumerSubscription
} from '@azure/event-hubs';
import { ContainerClient } from '@azure/storage-blob';
import { BlobCheckpointStore } from '@azure/eventhubs-checkpointstore-blob';
import { bind } from '../utils';

const moduleName = 'EventHubConsumer';

export class EventHubConsumer {
    private app: IAppConfig;
    private ehConsumerClient: EventHubConsumerClient;
    private ehSubscription: EventHubConsumerSubscription;
    private saContainerClient: ContainerClient;
    private saBlobCheckpointStore: BlobCheckpointStore;

    constructor(app: IAppConfig) {
        this.app = app;
    }

    public initialize(): void {
        this.saContainerClient = new ContainerClient(this.app.config.saConnectionString, this.app.config.saContainerName);
        this.saBlobCheckpointStore = new BlobCheckpointStore(this.saContainerClient);
        this.ehConsumerClient = new EventHubConsumerClient(this.app.config.ehConsumerGroup, this.app.config.ehConnectionString, this.app.config.ehName, this.saBlobCheckpointStore);
    }

    public startSubscription(): void {
        try {
            this.ehSubscription = this.ehConsumerClient.subscribe({
                processEvents: this.processEvent,
                processError: this.processError
            });
        }
        catch (ex) {
            this.app.log([moduleName, 'error'], `Error creating eventhub consumer subscription: ${ex.message}`);
        }
    }

    public async stopSubscription(): Promise<void> {
        try {
            await this.ehSubscription.close();
            await this.ehConsumerClient.close();
        }
        catch (ex) {
            this.app.log([moduleName, 'error'], `Error stopping eventhub consumer subscription: ${ex.message}`);
        }
    }

    @bind
    private async processEvent(events: any, context: any) {
        try {
            if (events.length === 0) {
                this.app.log([moduleName, 'info'], `No events received`);
                return;
            }

            for (const event of events) {
                const { result, matches } = this.processEachAvroProperty(event, [...this.app.config.matchValues]);

                if (matches.length === 0) {
                    // eslint-disable-next-line max-len
                    this.app.log([moduleName, 'info'], `Received event from partition: '${context.partitionId}' and consumer group: ${context.consumerGroup}\n${JSON.stringify(result, null, 4)}`);
                }
            }

            await context.updateCheckpoint(events[events.length - 1]);
        }
        catch (ex) {
            this.app.log([moduleName, 'error'], `Error processing eventhub event: ${ex.message}`);
        }
    }

    @bind
    // @ts-ignore
    private async processError(error: any, context: any) {
        this.app.log([moduleName, 'info'], `Error: ${error.code} - ${error.message}`);
    }

    private convertAvroProperty(obj: any, prop: string): { status: boolean; key: string; value: any } {
        const firstEntry = obj[prop] && Object.entries(obj[prop])?.[0];

        if (!firstEntry) {
            return {
                status: false,
                key: undefined,
                value: undefined
            };
        }

        const propType = Buffer.isBuffer(obj[prop]) ? 'buffer' : firstEntry[0];

        switch (propType) {
            case 'int':
            case 'long':
            case 'float':
            case 'double':
            case 'string':
            case 'boolean':
            case 'bool':
                return {
                    status: true,
                    key: prop,
                    value: firstEntry[1]
                };

            case 'bytes': {
                const bytesData = (firstEntry[1] as Buffer).toString('utf-8');
                let bytesMessage = bytesData;

                if (typeof bytesMessage !== 'string') {
                    try {
                        bytesMessage = JSON.parse(bytesData);
                    }
                    catch (ex) {
                        this.app.log([moduleName, 'warning'], `Failed to parse bytes property - returning string: ${ex.message}`);
                    }
                }

                return {
                    status: true,
                    key: prop,
                    value: bytesMessage
                };
            }

            case 'buffer': {
                const bufferData = obj[prop].toString('utf-8');
                let bufferMessage = bufferData;
                try {
                    bufferMessage = JSON.parse(bufferData);
                }
                catch (ex) {
                    this.app.log([moduleName, 'warning'], `Failed to parse buffer property - returning string: ${ex.message}`);
                }

                return {
                    status: true,
                    key: prop,
                    value: bufferMessage
                };
            }

            default:
                return {
                    status: false,
                    key: undefined,
                    value: undefined
                };
        }
    }

    // private processEachAvroProperty(obj: any): any {
    //     const newObj = {};

    //     for (const prop in obj) {
    //         if (!Object.prototype.hasOwnProperty.call(obj, prop)) {
    //             continue;
    //         }

    //         const { status, key, value } = this.convertAvroProperty(obj, prop);

    //         if (status) {
    //             newObj[key] = value;
    //         }
    //         else {
    //             if (obj[prop] !== null && typeof obj[prop] === 'object' && Object.keys(obj[prop]).length > 0) {
    //                 newObj[prop] = this.processEachAvroProperty(obj[prop]);
    //             }
    //             else {
    //                 newObj[prop] = obj[prop];
    //             }
    //         }
    //     }

    //     return newObj;
    // }

    private processEachAvroProperty(obj: any, matchValues: any[]): { result: any; matches: any[] } {
        const newObj = {};

        for (const prop in obj) {
            if (!Object.prototype.hasOwnProperty.call(obj, prop)) {
                continue;
            }

            const { status, key, value } = this.convertAvroProperty(obj, prop);

            if (status) {
                let matchElementIndex = matchValues.indexOf(value);
                if (matchElementIndex > -1) {
                    matchValues.splice(matchElementIndex, 1);
                }

                matchElementIndex = matchValues.indexOf(key);
                if (matchElementIndex > -1) {
                    matchValues.splice(matchElementIndex, 1);
                }

                newObj[key] = value;
            }
            else {
                if (obj[prop] !== null && typeof obj[prop] === 'object' && Object.keys(obj[prop]).length > 0) {
                    const { result, matches } = this.processEachAvroProperty(obj[prop], matchValues);

                    newObj[prop] = result;
                    matchValues = matches;
                }
                else {
                    newObj[prop] = obj[prop];
                }
            }
        }

        return {
            result: newObj,
            matches: matchValues
        };
    }
}
