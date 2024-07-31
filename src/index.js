import express from 'express';
import cors from 'cors';
import amqp from 'amqplib';
import swaggerUi from 'swagger-ui-express';
import swaggerJsDoc from 'swagger-jsdoc';
import AWS from 'aws-sdk';

// AWS region and Lambda function configuration
const region = "us-east-2";
const lambdaFunctionName = "fetchSecretsFunction_gr8";

// Function to invoke Lambda and fetch secrets
async function getSecretFromLambda() {
  const lambda = new AWS.Lambda({ region: region });
  const params = {
    FunctionName: lambdaFunctionName,
  };

  try {
    const response = await lambda.invoke(params).promise();
    const payload = JSON.parse(response.Payload);
    if (payload.errorMessage) {
      throw new Error(payload.errorMessage);
    }
    const body = JSON.parse(payload.body);
    return JSON.parse(body.secret);
  } catch (error) {
    console.error('Error invoking Lambda function:', error);
    throw error;
  }
}

// Function to start the service
async function startService() {
  let secrets;
  try {
    secrets = await getSecretFromLambda();
  } catch (error) {
    console.error(`Error starting service: ${error}`);
    return;
  }

  const app = express();
  const port = 8098;

  app.use(cors());
  app.use(express.json());

  // Configure AWS DynamoDB
  AWS.config.update({
    region: region,
    accessKeyId: secrets.AWS_ACCESS_KEY_ID,
    secretAccessKey: secrets.AWS_SECRET_ACCESS_KEY,
  });

  const dynamoDB = new AWS.DynamoDB.DocumentClient();

  // Swagger setup
  const swaggerOptions = {
    swaggerDefinition: {
      info: {
        title: 'Create Sale Service API',
        version: '1.0.0',
        description: 'API for creating sales',
      },
    },
    apis: ['./src/index.js'],
  };

  const swaggerDocs = swaggerJsDoc(swaggerOptions);
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

  // Connect to RabbitMQ
  let channel;
  async function connectRabbitMQ() {
    try {
      const connection = await amqp.connect('amqp://3.136.72.14:5672/');
      channel = await connection.createChannel();
      await channel.assertQueue('sale-events', { durable: true });
      console.log('Connected to RabbitMQ');
    } catch (error) {
      console.error('Error connecting to RabbitMQ:', error);
    }
  }

  await connectRabbitMQ();

  /**
   * @swagger
   * /sales:
   *   post:
   *     summary: Create a new sale
   *     description: Create a new sale with a date, product, client, and quantity
   *     requestBody:
   *       description: Sale object that needs to be created
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               date:
   *                 type: string
   *                 example: "2024-08-01"
   *               productId:
   *                 type: string
   *                 example: "prod-1628073549123"
   *               clientId:
   *                 type: string
   *                 example: "ci-1234567890"
   *               quantity:
   *                 type: integer
   *                 example: 5
   *     responses:
   *       201:
   *         description: Sale created
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 saleId:
   *                   type: string
   *                   example: "sale-1628073549123"
   *                 date:
   *                   type: string
   *                   example: "2024-08-01"
   *                 productId:
   *                   type: string
   *                   example: "prod-1628073549123"
   *                 clientId:
   *                   type: string
   *                   example: "ci-1234567890"
   *                 quantity:
   *                   type: integer
   *                   example: 5
   *       500:
   *         description: Error creating sale
   */
  app.post('/sales', async (req, res) => {
    const { date, productId, clientId, quantity } = req.body;
    console.log('Received request to create sale:', req.body);

    try {
      // Verify product exists and check available quantity in DynamoDB
      const getProductParams = {
        TableName: 'Products_gr8',
        Key: { productId },
      };

      const productData = await dynamoDB.get(getProductParams).promise();

      if (!productData.Item) {
        return res.status(400).send({ message: 'Product does not exist' });
      }

      if (productData.Item.quantity < quantity) {
        return res.status(400).send({ message: 'Insufficient product quantity available' });
      }

      // Verify client exists in DynamoDB
      const getClientParams = {
        TableName: 'Clients_gr8',
        Key: { ci: clientId },
      };

      const clientData = await dynamoDB.get(getClientParams).promise();

      if (!clientData.Item) {
        return res.status(400).send({ message: 'Client does not exist' });
      }

      // Save sale to DynamoDB
      const saleId = `sale-${Date.now()}`;
      const params = {
        TableName: 'Sales_gr8',
        Item: {
          saleId,
          date,
          productId,
          clientId,
          quantity,
        },
      };

      await dynamoDB.put(params).promise();

      // Update product quantity in DynamoDB
      const updateProductParams = {
        TableName: 'Products_gr8',
        Key: { productId },
        UpdateExpression: 'set quantity = quantity - :q',
        ExpressionAttributeValues: {
          ':q': quantity,
        },
        ReturnValues: 'UPDATED_NEW',
      };

      await dynamoDB.update(updateProductParams).promise();

      // Publish sale created event to RabbitMQ
      const event = {
        eventType: 'SaleCreated',
        data: { saleId, date, productId, clientId, quantity },
      };
      channel.sendToQueue('sale-events', Buffer.from(JSON.stringify(event)));
      console.log('Event published to RabbitMQ:', event);

      res.status(201).send({ saleId, date, productId, clientId, quantity });
    } catch (error) {
      console.error('Error creating sale:', error);
      res.status(500).send({ message: 'Error creating sale', error: error });
    }
  });

  app.get('/', (req, res) => {
    res.send('Create Sale Service Running');
  });

  app.listen(port, () => {
    console.log(`Create Sale service listening at http://localhost:${port}`);
  });
}

startService();
