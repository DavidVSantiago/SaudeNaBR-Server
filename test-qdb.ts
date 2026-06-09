import { Sender } from '@questdb/nodejs-client';

async function run() {
    try {
        const sender = await Sender.fromConfig('tcp::addr=127.0.0.1:9009');
        console.log("Connected via TCP");
        sender.table('telemetria_test')
              .symbol('id', '123')
              .intColumn('val', 10)
              .at(Date.now(), 'ms');
        await sender.flush();
        console.log("Flushed via TCP");
        await sender.close();
    } catch (err) {
        console.error("TCP failed:", err);
    }
}
run();
