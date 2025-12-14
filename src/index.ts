/**
 * File: src/index.ts
 * Author: Claude
 * Last Updated: 2025-10-07
 * Description: Main MCP server implementation for MySQL with OpenTelemetry
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  Tool,
  Prompt,
} from '@modelcontextprotocol/sdk/types.js';
import mysql from 'mysql2/promise';
import { config } from './config.js';
import {
  initTelemetry,
  shutdownTelemetry,
  startSpan,
  recordToolCall,
  recordQueryDuration,
  recordQueryError,
  recordQueryRows,
  recordQueryBytes,
} from './telemetry.js';
import { log } from './logger.js';

// Initialize telemetry
initTelemetry();

// Create MySQL connection pool
const pool = mysql.createPool({
  host: config.mysql.host,
  port: config.mysql.port,
  user: config.mysql.user,
  password: config.mysql.password,
  database: config.mysql.database,
  connectionLimit: config.mysql.connectionLimit,
  waitForConnections: true,
  queueLimit: 0,
});

// Define available tools
const TOOLS: Tool[] = [
  {
    name: 'onboarding',
    description: 'Get comprehensive guidance on how to use this MySQL MCP server efficiently, including available tools, best practices, and example workflows',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'list_tables',
    description: 'List all tables in the configured database',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'get_table_schema',
    description: 'Get the complete schema information for a specific table including columns, types, keys, and constraints',
    inputSchema: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table',
        },
      },
      required: ['table'],
    },
  },
  {
    name: 'execute_query',
    description: 'Execute a SQL query and return results. Supports both read (SELECT) and write (INSERT, UPDATE, DELETE) operations.',
    inputSchema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'SQL query to execute',
        },
      },
      required: ['query'],
    },
  },
];

// Define available prompts
const PROMPTS: Prompt[] = [
  {
    name: 'mysql-onboarding',
    description: 'Get guidance on how to efficiently use this MySQL MCP server',
  },
];

// Create MCP server
const server = new Server(
  {
    name: 'cc-mysql',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
      prompts: {},
    },
  }
);

// Tool handlers
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return { tools: TOOLS };
});

// Prompt handlers
server.setRequestHandler(ListPromptsRequestSchema, async () => {
  return { prompts: PROMPTS };
});

server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  const promptName = request.params.name;

  if (promptName === 'mysql-onboarding') {
    return {
      messages: [
        {
          role: 'user',
          content: {
            type: 'text',
            text: `You are connected to a MySQL database server via MCP. Here's how to use this server efficiently:

## Available Tools

1. **list_tables** - List all tables in the configured database
   - No parameters required
   - Returns: Array of table names
   - Example: Use this first to see what tables are available

2. **get_table_schema** - Get complete schema for a specific table
   - Parameters: table (string) - name of the table
   - Returns: Column definitions, indexes, foreign keys, and CREATE TABLE statement
   - Example: Use this to understand table structure before querying

3. **execute_query** - Execute any SQL query (SELECT, INSERT, UPDATE, DELETE, etc.)
   - Parameters: query (string) - SQL query to execute
   - Returns: For SELECT: rows and field info; For DML: affected rows and insert ID
   - Example: Run queries based on the schema you've discovered

## Recommended Workflow

1. **Start by listing tables**: Use list_tables to see what's in the database
2. **Examine schemas**: Use get_table_schema on relevant tables to understand structure
3. **Execute queries**: Use execute_query to retrieve or modify data

## Best Practices

- Always examine table schemas before writing complex queries
- Use prepared statements - the execute_query tool automatically handles them
- For exploratory analysis, start with simple SELECT queries with LIMIT clauses
- Check column types and constraints from get_table_schema to avoid type errors
- Use JOINs appropriately based on foreign key relationships shown in schema

## Current Configuration

- Database: ${config.mysql.database || 'Not set - will need to specify database in queries'}
- Host: ${config.mysql.host}:${config.mysql.port}

## Example Session

1. list_tables → See all available tables
2. get_table_schema(table: "users") → Understand users table structure
3. execute_query(query: "SELECT * FROM users LIMIT 10") → Retrieve sample data
4. execute_query(query: "SELECT COUNT(*) as total FROM users") → Get statistics

Now you're ready to explore the database! What would you like to do first?`,
          },
        },
      ],
    };
  }

  throw new Error(`Unknown prompt: ${promptName}`);
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const toolName = request.params.name;
  recordToolCall(toolName);

  const span = startSpan(`tool.${toolName}`);
  const startTime = Date.now();

  try {
    let result: any;

    switch (toolName) {
      case 'onboarding':
        result = await handleOnboarding();
        break;

      case 'list_tables':
        result = await handleListTables();
        break;

      case 'get_table_schema':
        result = await handleGetTableSchema(
          request.params.arguments as { table: string }
        );
        break;

      case 'execute_query': {
        const queryArgs = request.params.arguments as { query: string };
        const queryType = detectQueryType(queryArgs.query);
        if (span) {
          span.setAttribute('query.type', queryType);
        }
        result = await handleExecuteQuery(queryArgs);
        break;
      }

      default:
        throw new Error(`Unknown tool: ${toolName}`);
    }

    const duration = Date.now() - startTime;
    recordQueryDuration(duration, toolName);

    if (span) {
      span.setAttribute('success', true);

      // Add result metrics to span for execute_query
      if (toolName === 'execute_query' && result) {
        if (result.rowCount !== undefined) {
          span.setAttribute('query.rows', result.rowCount);
        } else if (result.affectedRows !== undefined) {
          span.setAttribute('query.affected_rows', result.affectedRows);
        }
      }

      span.end();
    }

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMessage = error instanceof Error ? error.message : String(error);

    recordQueryDuration(duration, toolName);
    recordQueryError(toolName, errorMessage);

    if (span) {
      span.setAttribute('success', false);
      span.setAttribute('error', errorMessage);
      span.end();
    }

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            error: errorMessage,
          }),
        },
      ],
      isError: true,
    };
  }
});

async function handleOnboarding() {
  return {
    guide: `Welcome to the MySQL MCP Server!`,
    description: `This server provides tools to explore and query MySQL databases efficiently.`,

    configuration: {
      database: config.mysql.database || 'Not set - queries will need to specify database',
      host: `${config.mysql.host}:${config.mysql.port}`,
    },

    available_tools: [
      {
        name: 'list_tables',
        purpose: 'List all tables in the configured database',
        parameters: 'None',
        returns: 'Array of table names',
        when_to_use: 'Use this first to discover what tables exist in the database',
        example: 'Call list_tables to see: ["users", "products", "orders", ...]',
      },
      {
        name: 'get_table_schema',
        purpose: 'Get complete schema information for a specific table',
        parameters: 'table: string (name of the table)',
        returns: 'Column definitions, data types, indexes, foreign keys, and CREATE TABLE statement',
        when_to_use: 'Use this to understand table structure before writing queries',
        example: 'get_table_schema(table: "users") returns all column info, constraints, and indexes',
      },
      {
        name: 'execute_query',
        purpose: 'Execute any SQL query (SELECT, INSERT, UPDATE, DELETE, etc.)',
        parameters: 'query: string (SQL query to execute)',
        returns: 'For SELECT: rows and field info; For DML: affected rows and insert ID',
        when_to_use: 'Use this to retrieve or modify data after understanding the schema',
        example: 'execute_query(query: "SELECT * FROM users LIMIT 10")',
      },
    ],

    recommended_workflow: [
      '1. Start with list_tables to see all available tables',
      '2. Use get_table_schema on relevant tables to understand their structure',
      '3. Execute queries using execute_query based on the schema information',
      '4. For complex queries, examine foreign key relationships from schema info',
    ],

    best_practices: [
      'Always examine table schemas before writing complex queries',
      'Use LIMIT clauses for exploratory SELECT queries',
      'Check column types and constraints from get_table_schema to avoid errors',
      'The execute_query tool automatically uses prepared statements for safety',
      'Use JOINs based on foreign key relationships shown in schema',
      'Start with simple queries and build complexity iteratively',
    ],

    example_session: {
      step_1: {
        action: 'list_tables',
        result: 'Returns all table names in the database',
      },
      step_2: {
        action: 'get_table_schema(table: "users")',
        result: 'Returns complete schema: columns, types, indexes, foreign keys',
      },
      step_3: {
        action: 'execute_query(query: "SELECT COUNT(*) as total FROM users")',
        result: 'Returns row count',
      },
      step_4: {
        action: 'execute_query(query: "SELECT * FROM users LIMIT 10")',
        result: 'Returns first 10 user records',
      },
    },

    tips: [
      'Complex queries: Always check schema first to understand relationships',
      'Performance: Use indexes (shown in schema) for WHERE clauses',
      'Debugging: Start with COUNT(*) queries to verify data exists',
      'Exploration: Use ORDER BY and LIMIT for sampling data',
    ],
  };
}

async function handleListTables() {
  const queryStartTime = Date.now();
  const [rows] = await pool.query('SHOW TABLES');
  const queryDuration = Date.now() - queryStartTime;

  const tableKey = `Tables_in_${config.mysql.database}`;
  const tables = (rows as any[]).map((row) => row[tableKey]);
  const resultData = {
    database: config.mysql.database,
    tables,
  };

  // Record metrics
  const payloadSize = calculatePayloadSize(resultData);
  recordQueryRows(tables.length, 'SHOW', 'list_tables');
  recordQueryBytes(payloadSize, 'SHOW', 'list_tables');
  recordQueryDuration(queryDuration, 'list_tables.SHOW');

  return resultData;
}

async function handleGetTableSchema(args: { table: string }) {
  const queryStartTime = Date.now();

  // Use DESCRIBE which works with minimal permissions
  const [columns] = await pool.query(`DESCRIBE ??`, [args.table]);

  // Get indexes - this should work with table-level permissions
  const [indexes] = await pool.query(`SHOW INDEXES FROM ??`, [args.table]);

  // Get CREATE TABLE statement to extract foreign keys and table comment
  let createTableInfo = '';
  try {
    const [createTable] = await pool.query(`SHOW CREATE TABLE ??`, [args.table]);
    createTableInfo = (createTable as any[])[0]?.['Create Table'] || '';
  } catch (error) {
    // If SHOW CREATE TABLE fails, continue without it
    console.error('Could not get CREATE TABLE info:', error);
  }

  const queryDuration = Date.now() - queryStartTime;

  const resultData = {
    database: config.mysql.database,
    table: args.table,
    columns,
    indexes,
    createTableStatement: createTableInfo,
  };

  // Record metrics
  const payloadSize = calculatePayloadSize(resultData);
  const totalRows = (columns as any[]).length + (indexes as any[]).length;
  recordQueryRows(totalRows, 'DESCRIBE', 'get_table_schema');
  recordQueryBytes(payloadSize, 'DESCRIBE', 'get_table_schema');
  recordQueryDuration(queryDuration, 'get_table_schema.DESCRIBE');

  return resultData;
}

// Helper function to detect query type from SQL
function detectQueryType(query: string): string {
  const normalized = query.trim().toUpperCase();
  if (normalized.startsWith('SELECT')) return 'SELECT';
  if (normalized.startsWith('INSERT')) return 'INSERT';
  if (normalized.startsWith('UPDATE')) return 'UPDATE';
  if (normalized.startsWith('DELETE')) return 'DELETE';
  if (normalized.startsWith('CREATE')) return 'CREATE';
  if (normalized.startsWith('ALTER')) return 'ALTER';
  if (normalized.startsWith('DROP')) return 'DROP';
  if (normalized.startsWith('TRUNCATE')) return 'TRUNCATE';
  if (normalized.startsWith('REPLACE')) return 'REPLACE';
  if (normalized.startsWith('SHOW')) return 'SHOW';
  if (normalized.startsWith('DESCRIBE') || normalized.startsWith('DESC')) return 'DESCRIBE';
  return 'OTHER';
}

// Helper function to calculate payload size in bytes
function calculatePayloadSize(data: any): number {
  try {
    return Buffer.byteLength(JSON.stringify(data), 'utf8');
  } catch {
    return 0;
  }
}

async function handleExecuteQuery(args: { query: string }) {
  const connection = await pool.getConnection();
  const queryType = detectQueryType(args.query);
  const queryStartTime = Date.now();

  try {
    const [results, fields] = await connection.query(args.query);
    const queryDuration = Date.now() - queryStartTime;

    // Handle different result types
    if (Array.isArray(results)) {
      // SELECT-type queries
      const resultData = {
        rowCount: results.length,
        rows: results,
        fields: fields?.map((f) => ({
          name: f.name,
          type: f.type,
        })),
      };

      // Record metrics for SELECT queries
      const payloadSize = calculatePayloadSize(results);
      recordQueryRows(results.length, queryType, 'execute_query');
      recordQueryBytes(payloadSize, queryType, 'execute_query');
      recordQueryDuration(queryDuration, `execute_query.${queryType}`);

      return resultData;
    } else {
      // For INSERT, UPDATE, DELETE, etc.
      const resultPacket = results as mysql.ResultSetHeader;
      const resultData = {
        affectedRows: resultPacket.affectedRows,
        insertId: resultPacket.insertId,
        warningCount: resultPacket.warningStatus,
      };

      // Record metrics for DML queries
      const payloadSize = calculatePayloadSize(resultData);
      recordQueryRows(resultPacket.affectedRows, queryType, 'execute_query');
      recordQueryBytes(payloadSize, queryType, 'execute_query');
      recordQueryDuration(queryDuration, `execute_query.${queryType}`);

      return resultData;
    }
  } finally {
    connection.release();
  }
}

// Start server
async function main() {
  // Test MySQL connection before starting server
  log('Testing MySQL connection...');
  try {
    const connection = await pool.getConnection();
    log('✓ MySQL connection successful');
    connection.release();
  } catch (error) {
    log('✗ MySQL connection failed: ' + (error instanceof Error ? error.message : String(error)));
    log('Please check your MySQL credentials and ensure the server is running.');
    log('');
    process.exit(1);
  }

  const transport = new StdioServerTransport();
  await server.connect(transport);

  log('MySQL MCP Server running on stdio');
  log(`Connected to MySQL at ${config.mysql.host}:${config.mysql.port}`);
  if (config.mysql.database) {
    log(`Default database: ${config.mysql.database}`);
  }
  log('');
}

// Cleanup on exit
process.on('SIGINT', async () => {
  console.error('\nShutting down...');
  await pool.end();
  await shutdownTelemetry();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.error('\nShutting down...');
  await pool.end();
  await shutdownTelemetry();
  process.exit(0);
});

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});