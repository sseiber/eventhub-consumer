import './env';
import * as chalk from 'chalk';
import * as moment from 'moment';
import { EventHubConsumer } from './service/eventeHubConsumer';
import { sleep } from './utils';

import {
    platform as osPlatform,
    cpus as osCpus,
    freemem as osFreeMem,
    totalmem as osTotalMem
} from 'os';

export interface IAppConfig {
    config: {
        ehConnectionString: string;
        ehConsumerGroup: string;
        ehName: string;
        saConnectionString: string;
        saContainerName: string;
        matchValues: any[];
    };
    eventHubConsumer: EventHubConsumer;
    log: (tags: string[], message: string) => void;
}

const app: IAppConfig = {
    config: {
        ehConnectionString: '',
        ehConsumerGroup: '',
        ehName: '',
        saConnectionString: '',
        saContainerName: '',
        matchValues: []
    },
    eventHubConsumer: null,
    log: (tags: string[], message: string) => {
        const time = moment().format();
        const severity = 'INFO';
        const tagsMessage = (tags && Array.isArray(tags)) ? `[${tags.join(', ')}]` : '[]';

        // eslint-disable-next-line no-console
        console.log(`${chalk.white(`[${time}]`)} ${chalk.green(`${severity}:`)} ${chalk.cyan(`${tagsMessage} ${message}`)}`);
    }
};

process.on('unhandledRejection', (e: any) => {
    app.log(['startup', 'error'], `Excepction on startup...${e.message}`);
    app.log(['startup', 'error'], e.stack);
});

async function start() {
    try {
        app.log(['startup', 'info'], `✅ started`);
        app.log(['startup', 'info'], ` > Machine: ${osPlatform()}, ${osCpus().length} core, ` +
            `freemem = ${(osFreeMem() / 1024 / 1024).toFixed(0)} mb, totalmem = ${(osTotalMem() / 1024 / 1024).toFixed(0)} mb`);

        const {
            ehConnectionString,
            ehConsumerGroup,
            ehName,
            saConnectionString,
            saContainerName,
            matchValues
        } = process.env;

        if (!ehConnectionString || !ehConsumerGroup || !ehName || !saConnectionString || !saContainerName) {
            app.log(['startup', 'error'], 'Error - missing required environment variables');
            return;
        }

        app.config.ehConnectionString = ehConnectionString;
        app.config.ehConsumerGroup = ehConsumerGroup;
        app.config.ehName = ehName;
        app.config.saConnectionString = saConnectionString;
        app.config.saContainerName = saContainerName;
        app.config.matchValues = matchValues ? matchValues.split(',') : [];

        app.eventHubConsumer = new EventHubConsumer(app);
        app.eventHubConsumer.initialize();
        app.eventHubConsumer.startSubscription();

        await sleep((60 * 1000) * 15);

        await app.eventHubConsumer.stopSubscription();

        app.log(['startup', 'info'], 'Subscription stopped...');
    }
    catch (error) {
        app.log(['startup', 'error'], `👹 Error starting server: ${error.message} `);
    }
}

void (async () => {
    await start();
})().catch();
