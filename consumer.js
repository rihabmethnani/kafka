const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });

// Configuration de la connexion à MongoDB
const mongoUri = 'mongodb://localhost:27017';
const mongoClient = new MongoClient(mongoUri);
const dbName = 'kafkaMessages';
const collectionName = 'messages';

const run = async () => {
    await mongoClient.connect();
    console.log("Connecté à MongoDB");
    const db = mongoClient.db(dbName);
    const collection = db.collection(collectionName);

    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const messageContent = {
                value: message.value.toString(),
                topic,
                partition,
                offset: message.offset,
                timestamp: message.timestamp,
            };
            console.log(messageContent);

            // Insérer le message dans MongoDB
            await collection.insertOne(messageContent);
            console.log('Message enregistré dans MongoDB');
        },
    });
};

run().catch(async (error) => {
    console.error("Erreur dans le consommateur", error);
    await mongoClient.close();
    process.exit(1);
});
