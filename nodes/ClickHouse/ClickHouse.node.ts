import type {
	IExecuteFunctions,
	ICredentialsDecrypted,
	ICredentialTestFunctions,
	IDataObject,
	INodeCredentialTestResult,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';

import { NodeOperationError } from 'n8n-workflow';

import { createClient, ClickHouseClientConfigOptions } from '@clickhouse/client'

export class ClickHouse implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ClickHouse',
		name: 'clickhouse',
		icon: 'file:clickhouse.svg',
		group: ['input'],
		version: 1,
		description: 'Query and ingest data into ClickHouse',
		defaults: {
			name: 'clickhouse',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'clickhouse',
				required: true,
				testedBy: 'clickhouseConnectionTest',
			},
		],
		usableAsTool: {
			replacements: {
				codex: {
					subcategories: {
						Tools: ['Recommended Tools'],
					},
				},
			},
		},
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Query',
						value: 'query',
						description: 'Execute an SQL query',
						action: 'Execute a SQL query',
					},
					{
						name: 'Insert',
						value: 'insert',
						description: 'Insert rows in database',
						action: 'Insert rows in database',
					},
				],
				default: 'insert',
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['query'],
					},
				},
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE quantity > {quantity:Int32} AND price <= {price:Int32}',
				required: true,
				description:
					'The SQL query to execute. You can use n8n expressions or ClickHouse query parameters.',
			},
			{
				displayName: 'Table Name',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['insert'],
					},
				},
				default: '',
				placeholder: 'product',
				required: true,
				description:
					'The table name to insert data. You can use n8n expressions.',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						operation: ['query'],
					},
				},
				options: [
					{
						displayName: 'Schema Description',
						name: 'schemaDescription',
						type: 'string',
						default: '',
						placeholder: 'e.g., Available tables: users (ID, name, email), orders (ID, user_id, amount, date)',
						description: 'Describe your database schema to help AI agents generate better queries',
						typeOptions: {
							rows: 3,
						},
					},
					{
						displayName: 'Read-Only Mode',
						name: 'readOnlyMode',
						type: 'boolean',
						default: false,
						description: 'Whether to allow only SELECT queries (recommended for AI tools to prevent accidental data modification)',
					},
					{
						displayName: 'Max Results',
						name: 'maxResults',
						type: 'number',
						default: 0,
						description: 'Maximum number of rows to return (0 = no limit)',
					},
				],
			},
		],
	};

	methods = {
		credentialTest: {
			async clickhouseConnectionTest(
				this: ICredentialTestFunctions,
				credential: ICredentialsDecrypted,
			): Promise<INodeCredentialTestResult> {
				const credentials = credential.data as IDataObject;
				try {
					const config: ClickHouseClientConfigOptions = {
						host: credentials.url as string,
						database: credentials.database as string,
						username: credentials.user as string,
						password: credentials.password as string,
					};

					const client = createClient(config);

					// Actually test the connection by running a simple query
					await client.query({
						query: 'SELECT 1',
						format: 'JSONEachRow',
					});

					await client.close();
				} catch (error) {
					return {
						status: 'Error',
						message: error.message,
					};
				}
				return {
					status: 'OK',
					message: 'Connection successful!',
				};
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const credentials = await this.getCredentials('clickhouse');

		const config: ClickHouseClientConfigOptions = {
			host: credentials.url as string,
			database: credentials.database as string,
			username: credentials.user as string,
			password: credentials.password as string,
		};

		const client = createClient(config);

		const operation = this.getNodeParameter('operation', 0);
		const queryParams = {} as Record<string, unknown>;

		let returnItems: INodeExecutionData[] = [];

		try {
			if (operation === 'query') {
				let query = this.getNodeParameter('query', 0) as string;
				const options = this.getNodeParameter('options', 0, {}) as IDataObject;
				const readOnlyMode = options.readOnlyMode as boolean || false;
				const maxResults = options.maxResults as number || 0;

				// Validate read-only mode
				if (readOnlyMode) {
					const normalizedQuery = query.trim().toUpperCase();
					if (!normalizedQuery.startsWith('SELECT') &&
						!normalizedQuery.startsWith('SHOW') &&
						!normalizedQuery.startsWith('DESCRIBE') &&
						!normalizedQuery.startsWith('DESC')) {
						throw new NodeOperationError(
							this.getNode(),
							'Only SELECT, SHOW, and DESCRIBE queries are allowed in read-only mode',
						);
					}
				}

				// Add LIMIT if not present and maxResults is set
				if (maxResults > 0 && !query.toUpperCase().includes('LIMIT')) {
					query = `${query} LIMIT ${maxResults}`;
				}

				const result = await client.query({
					query: query,
					format: 'JSONEachRow',
					query_params: queryParams,
				});

				const rows = (await result.json()) as object[];
				console.log('received CH rows', rows);

				returnItems = rows.map(row => ({json: row} as INodeExecutionData));
			} else if (operation === 'insert') {
				const items = this.getInputData().map(value => value.json);
				const table = this.getNodeParameter('table', 0) as string;

				console.log('insert CH rows', items);

				await client.insert({
					table: table,
					format: 'JSONEachRow',
					values: items,
					query_params: queryParams,
				});
			}
		} catch (error) {
			await client.close();
			throw error;
		}

		await client.close();

		return this.prepareOutputData(returnItems);
	}
}
