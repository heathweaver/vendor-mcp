import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import express from 'express';
import cors from 'cors';
import { DatabaseManager } from './database.js';
import dotenv from 'dotenv';
import { randomUUID } from 'crypto';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

dotenv.config({ path: '../.env' });

const db = new DatabaseManager();

// Store transports by session ID
const transports: Record<string, StreamableHTTPServerTransport> = {};

class VendorMCPServer {
  private server: Server;
  private app: express.Application;

  constructor() {
    this.server = new Server(
      {
        name: 'vendor-mcp-server',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
          resources: {},
        },
      }
    );

    this.app = express();
    this.setupHandlers();
    this.setupExpress();
  }

  private setupHandlers() {
    // List available tools
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: 'query_spend',
            description: 'Run arbitrary read-only SQL queries against the vendor spend database. Use normalized_vendors and other views for analysis.',
            inputSchema: {
              type: 'object',
              required: ['query'],
              properties: {
                query: { type: 'string', description: 'SQL SELECT query' },
                params: { type: 'array', description: 'Query parameters' }
              }
            }
          },
          {
            name: 'list_tables',
            description: 'List all tables and views in the database.',
            inputSchema: { type: 'object' }
          },
          {
            name: 'describe_table',
            description: 'Get schema information for a specific table.',
            inputSchema: {
              type: 'object',
              required: ['table_name'],
              properties: {
                table_name: { type: 'string' }
              }
            }
          }
        ],
      };
    });

    // Handle tool calls
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      try {
        switch (request.params.name) {
          case 'query_spend': {
            const query = request.params.arguments?.query as string;
            const params = request.params.arguments?.params as any[];
            if (!query.trim().toUpperCase().startsWith('SELECT')) {
              throw new Error('Only SELECT queries are allowed');
            }
            const result = await db.executeQuery(query, params);
            return {
              content: [{ type: 'text', text: JSON.stringify({ rows: result.rows, rowCount: result.rowCount }, null, 2) }]
            };
          }
          case 'list_tables': {
            const tables = await db.getTables();
            return {
              content: [{ type: 'text', text: JSON.stringify(tables, null, 2) }]
            };
          }
          case 'describe_table': {
            const tableName = request.params.arguments?.table_name as string;
            const schema = await db.getTableSchema(tableName);
            return {
              content: [{ type: 'text', text: JSON.stringify(schema, null, 2) }]
            };
          }
          default:
            throw new Error(`Unknown tool: ${request.params.name}`);
        }
      } catch (error) {
        return {
          content: [{ type: 'text', text: `Error: ${error instanceof Error ? error.message : String(error)}` }],
          isError: true
        };
      }
    });

    // List available resources
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      return {
        resources: [
          {
            uri: 'spend://summary',
            name: 'Spend Summary',
            description: 'Top-level aggregation of spend by vendor and category.',
            mimeType: 'application/json'
          },
          {
            uri: 'spend://memo',
            name: 'Latest Analysis Memo',
            description: 'Retrieves metadata about the latest generated PDF memo.',
            mimeType: 'application/json'
          }
        ]
      };
    });

    // Read resources
    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      const uri = request.params.uri;
      if (uri === 'spend://summary') {
        const result = await db.executeQuery('SELECT vendor_name, total_spend, category FROM vendor_spend_summary ORDER BY total_spend DESC LIMIT 20');
        return {
          contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(result.rows, null, 2) }]
        };
      }
      if (uri === 'spend://memo') {
        const result = await db.executeQuery('SELECT file_name, created_at FROM analysis_runs WHERE status = \'completed\' ORDER BY created_at DESC LIMIT 1');
        return {
          contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(result.rows[0] || {}, null, 2) }]
        };
      }
      throw new Error(`Unknown resource: ${uri}`);
    });
  }

  private setupExpress() {
    this.app.use(cors({ origin: '*', exposedHeaders: ['Mcp-Session-Id'] }));
    this.app.use(express.json());

    this.app.get('/health', (_req: express.Request, res: express.Response) => {
      res.json({ status: 'ok', server: 'vendor-mcp-server' });
    });

    this.app.all('/mcp', async (req: express.Request, res: express.Response) => {
      const sessionId = req.headers['mcp-session-id'] as string | undefined;
      try {
        let transport: StreamableHTTPServerTransport;
        if (sessionId && transports[sessionId]) {
          transport = transports[sessionId]!;
        } else if (!sessionId && req.method === 'POST' && isInitializeRequest(req.body)) {
          transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: () => randomUUID(),
            onsessioninitialized: (sid) => {
              transports[sid] = transport;
            },
          });
          transport.onclose = () => {
            if (transport.sessionId) delete transports[transport.sessionId];
          };
          // Use any cast if SDK types are fighting exactOptionalPropertyTypes
          await this.server.connect(transport as any);
        } else {
          res.status(400).json({ error: 'Invalid session' });
          return;
        }
        await transport.handleRequest(req, res, req.body);
      } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
      }
    });
  }

  async start() {
    const port = parseInt(process.env.PORT || '8765');
    this.app.listen(port, '0.0.0.0', () => {
      console.log(`Vendor MCP Server running on http://0.0.0.0:${port}`);
    });
  }
}

const mcpServer = new VendorMCPServer();
mcpServer.start().catch(console.error);

process.on('SIGINT', async () => {
  await db.close();
  process.exit(0);
});
