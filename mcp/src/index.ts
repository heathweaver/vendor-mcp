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
import { randomUUID, createHash } from 'crypto';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';
import { execFile } from 'child_process';
import { promisify } from 'util';
import path from 'path';
import { readFileSync, writeFileSync, existsSync } from 'fs';

dotenv.config({ path: process.env.DOTENV_PATH || '.env' });

const execFileAsync = promisify(execFile);
const db = new DatabaseManager();

// Store transports by session ID
const transports: Record<string, StreamableHTTPServerTransport> = {};

// ---------------------------------------------------------------------------
// PKCE OAuth 2.0 — full implementation matching asana-mcp pattern
// ---------------------------------------------------------------------------

const SCOPES = ['vendor.read'] as const;
const DEFAULT_SCOPE = SCOPES.join(' ');
const AUTH_CODE_TTL_MS = 10 * 60 * 1000;
const ACCESS_TOKEN_TTL_MS = 24 * 60 * 60 * 1000;
const OAUTH_STATE_FILE = '/root/.cloudflared/oauth-state.json';

type TokenInfo = {
  user: string;
  createdAt: number;
  scope: string;
  expiresAt: number | null;
};

type AuthorizationCodeRecord = {
  clientId: string;
  redirectUri: string;
  codeChallenge: string;
  codeChallengeMethod: 'S256';
  scope: string;
  user: string;
  createdAt: number;
  expiresAt: number;
  resource?: string;
};

type PendingAuthState =
  | { type: 'manual'; createdAt: number; scope: string }
  | {
      type: 'oauth';
      clientId: string;
      redirectUri: string;
      codeChallenge: string;
      codeChallengeMethod: 'S256';
      scope: string;
      originalState: string;
      createdAt: number;
      resource?: string;
    };

type RegisteredClient = {
  clientId: string;
  clientName?: string;
  redirectUris: string[];
  scope: string;
  tokenEndpointAuthMethod: 'none';
  clientIdIssuedAt: number;
  clientSecretExpiresAt: number;
  registrationAccessToken: string;
};

const pendingAuthStates = new Map<string, PendingAuthState>();
const authorizationCodes = new Map<string, AuthorizationCodeRecord>();
const issuedTokens = new Map<string, TokenInfo>();
const registeredClients = new Map<string, RegisteredClient>();

function base64UrlEncode(buffer: Buffer): string {
  return buffer.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function sha256Base64Url(value: string): string {
  return base64UrlEncode(createHash('sha256').update(value).digest());
}

function sanitizeScope(scope?: string): string {
  if (!scope || !scope.trim()) return DEFAULT_SCOPE;
  const scopes = scope.split(/\s+/).filter(Boolean).filter(s => (SCOPES as readonly string[]).includes(s));
  return scopes.length ? scopes.join(' ') : DEFAULT_SCOPE;
}

function getQueryParam(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function isTokenAuthorized(token: string): boolean {
  const t = issuedTokens.get(token);
  if (!t) return false;
  if (t.expiresAt !== null && Date.now() > t.expiresAt) {
    issuedTokens.delete(token);
    return false;
  }
  return true;
}

function saveOAuthState() {
  try {
    const state = {
      pendingAuthStates: Array.from(pendingAuthStates.entries()),
      authorizationCodes: Array.from(authorizationCodes.entries()),
      issuedTokens: Array.from(issuedTokens.entries()),
      registeredClients: Array.from(registeredClients.entries()),
    };
    writeFileSync(OAUTH_STATE_FILE, JSON.stringify(state), 'utf8');
  } catch (e) {
    // volume may not be mounted yet on first start
  }
}

function loadOAuthState() {
  try {
    if (!existsSync(OAUTH_STATE_FILE)) return;
    const state = JSON.parse(readFileSync(OAUTH_STATE_FILE, 'utf8'));
    const now = Date.now();
    if (state.pendingAuthStates) {
      for (const [k, v] of state.pendingAuthStates) {
        if (now - v.createdAt < 15 * 60 * 1000) pendingAuthStates.set(k, v);
      }
    }
    if (state.authorizationCodes) {
      for (const [k, v] of state.authorizationCodes) {
        if (now < v.expiresAt) authorizationCodes.set(k, v);
      }
    }
    if (state.issuedTokens) {
      for (const [k, v] of state.issuedTokens) {
        if (v.expiresAt === null || now < v.expiresAt) issuedTokens.set(k, v);
      }
    }
    if (state.registeredClients) {
      for (const [k, v] of state.registeredClients) registeredClients.set(k, v);
    }
  } catch (e) {
    // corrupt state file — start fresh
  }
}

loadOAuthState();

// ---------------------------------------------------------------------------
// Helpers — resolve the latest run_id if not explicitly provided
// ---------------------------------------------------------------------------

async function resolveRunId(raw: unknown): Promise<number> {
  if (raw !== undefined && raw !== null) return Number(raw);
  const result = await db.executeQuery(
    "SELECT id FROM analysis_runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
  );
  if (!result.rows.length) throw new Error('No completed analysis run found. Upload a file to /incoming to start a run.');
  return result.rows[0].id as number;
}

// ---------------------------------------------------------------------------
// Main server class
// ---------------------------------------------------------------------------

class VendorMCPServer {
  private server: Server;
  private app: express.Application;

  constructor() {
    this.server = new Server(
      { name: 'vendor-mcp-server', version: '2.0.0' },
      { capabilities: { tools: {}, resources: {} } }
    );
    this.app = express();
    this.setupHandlers();
    this.setupExpress();
  }

  private setupHandlers() {
    // -----------------------------------------------------------------------
    // Tools
    // -----------------------------------------------------------------------
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'get_run_summary',
          description: 'Returns a high-level summary of a vendor spend analysis run: total spend, vendor count, top-10 concentration, total savings opportunity range, and run metadata.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit to use the latest completed run.' }
            }
          }
        },
        {
          name: 'list_top_vendors',
          description: 'Returns the top vendors by spend for a run, ordered by total spend descending.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' },
              limit: { type: 'number', description: 'Number of vendors to return (default 10, max 50).' }
            }
          }
        },
        {
          name: 'list_opportunities',
          description: 'Returns all savings opportunities identified for a run, including action type, target vendor/category, rationale, and dollar impact estimate.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' },
              action_type: { type: 'string', description: 'Filter by action type: renegotiate, consolidate, eliminate, automate.' }
            }
          }
        },
        {
          name: 'list_fragmented_categories',
          description: 'Returns categories with high vendor fragmentation — many suppliers sharing spend that should be consolidated.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' }
            }
          }
        },
        {
          name: 'get_tail_summary',
          description: 'Returns tail spend statistics: how many low-value vendors exist, what they collectively spend, and the estimated administrative cost reduction from consolidation.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' }
            }
          }
        },
        {
          name: 'get_memo',
          description: 'Returns the full text of the executive memo generated for a run.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' }
            }
          }
        },
        {
          name: 'generate_pdf',
          description: 'Generates (or regenerates) the executive memo PDF for a run and returns the file path. Use this when the user asks for a PDF report.',
          inputSchema: {
            type: 'object',
            properties: {
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' }
            }
          }
        },
        {
          name: 'ask_run',
          description: 'Ask a natural language question about a specific analysis run. The tool queries the database and returns a structured answer. Examples: "Which vendors should be renegotiated first?", "How much is in the long tail?", "What are the top consolidation opportunities?"',
          inputSchema: {
            type: 'object',
            required: ['question'],
            properties: {
              question: { type: 'string', description: 'Natural language question about the vendor spend data.' },
              run_id: { type: 'number', description: 'Analysis run ID. Omit for latest.' }
            }
          }
        },
        {
          name: 'list_runs',
          description: 'Lists all analysis runs with their status, file name, and creation date.',
          inputSchema: { type: 'object' }
        }
      ]
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      try {
        const args = request.params.arguments || {};

        switch (request.params.name) {

          case 'list_runs': {
            const result = await db.executeQuery(
              "SELECT id, file_name, status, created_at FROM analysis_runs ORDER BY created_at DESC LIMIT 20"
            );
            return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
          }

          case 'get_run_summary': {
            const runId = await resolveRunId(args.run_id);

            const [runRow, totalRow, vendorCount, top10Row, oppsRow] = await Promise.all([
              db.executeQuery("SELECT id, file_name, status, created_at FROM analysis_runs WHERE id = $1", [runId]),
              db.executeQuery("SELECT SUM(total_spend) as total, COUNT(*) as vendors FROM vendor_spend_summary WHERE run_id = $1", [runId]),
              db.executeQuery("SELECT COUNT(*) as cnt FROM vendor_spend_summary WHERE run_id = $1", [runId]),
              db.executeQuery("SELECT SUM(total_spend) as top10_spend FROM (SELECT total_spend FROM vendor_spend_summary WHERE run_id = $1 ORDER BY total_spend DESC LIMIT 10) t", [runId]),
              db.executeQuery("SELECT COUNT(*) as cnt, SUM(CAST(REGEXP_REPLACE(REPLACE(REPLACE(impact_estimate, '$', ''), 'k', '000'), '[^0-9.]', '', 'g') AS NUMERIC)) as rough_total FROM savings_opportunities WHERE run_id = $1", [runId])
            ]);

            const total = parseFloat(totalRow.rows[0]?.total || '0');
            const top10 = parseFloat(top10Row.rows[0]?.top10_spend || '0');
            const top10pct = total > 0 ? ((top10 / total) * 100).toFixed(1) : '0';

            const summary = {
              run_id: runId,
              file_name: runRow.rows[0]?.file_name,
              status: runRow.rows[0]?.status,
              created_at: runRow.rows[0]?.created_at,
              total_spend: `$${total.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
              total_vendors: parseInt(vendorCount.rows[0]?.cnt || '0'),
              top10_concentration: `${top10pct}% of spend in top 10 vendors`,
              top10_spend: `$${top10.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
              opportunities_identified: parseInt(oppsRow.rows[0]?.cnt || '0'),
            };

            return { content: [{ type: 'text', text: JSON.stringify(summary, null, 2) }] };
          }

          case 'list_top_vendors': {
            const runId = await resolveRunId(args.run_id);
            const limit = Math.min(parseInt(String(args.limit || '10')), 50);
            const result = await db.executeQuery(
              "SELECT canonical_vendor, total_spend, transaction_count, category_count FROM vendor_spend_summary WHERE run_id = $1 ORDER BY total_spend DESC LIMIT $2",
              [runId, limit]
            );
            const totalRow = await db.executeQuery(
              "SELECT SUM(total_spend) as total FROM vendor_spend_summary WHERE run_id = $1",
              [runId]
            );
            const grand = parseFloat(totalRow.rows[0]?.total || '1');
            const rows = result.rows.map((r: any) => ({
              vendor: r.canonical_vendor,
              spend: `$${parseFloat(r.total_spend).toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
              pct_of_total: `${((parseFloat(r.total_spend) / grand) * 100).toFixed(1)}%`,
              transactions: r.transaction_count,
            }));
            return { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] };
          }

          case 'list_opportunities': {
            const runId = await resolveRunId(args.run_id);
            let query = "SELECT target, action_type, rationale, impact_estimate FROM savings_opportunities WHERE run_id = $1";
            const params: any[] = [runId];
            if (args.action_type) {
              query += " AND action_type = $2";
              params.push(args.action_type);
            }
            query += " ORDER BY id";
            const result = await db.executeQuery(query, params);
            return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
          }

          case 'list_fragmented_categories': {
            const runId = await resolveRunId(args.run_id);
            const result = await db.executeQuery(
              "SELECT category, vendor_count, total_spend, fragmentation_score FROM fragmented_categories WHERE run_id = $1 ORDER BY fragmentation_score DESC",
              [runId]
            );
            const rows = result.rows.map((r: any) => ({
              category: r.category,
              vendor_count: r.vendor_count,
              total_spend: `$${parseFloat(r.total_spend).toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
              fragmentation_score: parseFloat(r.fragmentation_score).toFixed(2),
              recommended_vendors: Math.max(2, Math.round(r.vendor_count * 0.2)),
            }));
            return { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] };
          }

          case 'get_tail_summary': {
            const runId = await resolveRunId(args.run_id);
            const [tailRow, totalRow, totalVendors] = await Promise.all([
              db.executeQuery("SELECT COUNT(*) as cnt, SUM(total_spend) as spend FROM tail_spend_summary WHERE run_id = $1", [runId]),
              db.executeQuery("SELECT SUM(total_spend) as total FROM vendor_spend_summary WHERE run_id = $1", [runId]),
              db.executeQuery("SELECT COUNT(*) as cnt FROM vendor_spend_summary WHERE run_id = $1", [runId])
            ]);
            const tailCount = parseInt(tailRow.rows[0]?.cnt || '0');
            const tailSpend = parseFloat(tailRow.rows[0]?.spend || '0');
            const grand = parseFloat(totalRow.rows[0]?.total || '1');
            const totalV = parseInt(totalVendors.rows[0]?.cnt || '0');
            const targetReduction = Math.round(tailCount * 0.6);
            const adminLow = targetReduction * 2500 * 0.7;
            const adminHigh = targetReduction * 2500 * 1.2;

            return {
              content: [{
                type: 'text', text: JSON.stringify({
                  tail_vendor_count: tailCount,
                  total_vendors: totalV,
                  tail_vendors_pct: `${((tailCount / totalV) * 100).toFixed(0)}%`,
                  tail_spend: `$${tailSpend.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
                  tail_spend_pct: `${((tailSpend / grand) * 100).toFixed(1)}%`,
                  target_reduction: targetReduction,
                  estimated_admin_savings: `$${adminLow.toLocaleString('en-US', { maximumFractionDigits: 0 })}–$${adminHigh.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
                }, null, 2)
              }]
            };
          }

          case 'get_memo': {
            const runId = await resolveRunId(args.run_id);
            const result = await db.executeQuery(
              "SELECT markdown_content, pdf_path, created_at FROM memo_outputs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
              [runId]
            );
            if (!result.rows.length) {
              return { content: [{ type: 'text', text: `No memo found for run ${runId}. The pipeline may still be processing.` }] };
            }
            const row = result.rows[0];
            return {
              content: [{
                type: 'text',
                text: `PDF available at: ${row.pdf_path}\nGenerated: ${row.created_at}\n\n---\n\n${row.markdown_content}`
              }]
            };
          }

          case 'generate_pdf': {
            const runId = await resolveRunId(args.run_id);
            // Check if memo exists; if so, return existing path
            const existing = await db.executeQuery(
              "SELECT pdf_path FROM memo_outputs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
              [runId]
            );
            if (existing.rows.length && existing.rows[0].pdf_path) {
              return {
                content: [{
                  type: 'text',
                  text: JSON.stringify({ run_id: runId, pdf_path: existing.rows[0].pdf_path, status: 'existing' }, null, 2)
                }]
              };
            }
            // Trigger memo generation via Python
            try {
              const projectRoot = path.resolve('../');
              await execFileAsync('python3', ['-c',
                `import asyncio; import sys; sys.path.insert(0, '.'); from activities.generate_memo import generate_memo; result = asyncio.run(generate_memo(${runId})); print(result)`
              ], { cwd: projectRoot, timeout: 120000 });
              const newRow = await db.executeQuery(
                "SELECT pdf_path FROM memo_outputs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
                [runId]
              );
              return {
                content: [{
                  type: 'text',
                  text: JSON.stringify({ run_id: runId, pdf_path: newRow.rows[0]?.pdf_path, status: 'generated' }, null, 2)
                }]
              };
            } catch (e) {
              throw new Error(`PDF generation failed: ${e instanceof Error ? e.message : String(e)}`);
            }
          }

          case 'ask_run': {
            const runId = await resolveRunId(args.run_id);
            const question = String(args.question || '').toLowerCase();

            // Route common question patterns to the right data
            if (question.includes('renegotiat') || question.includes('top vendor') || question.includes('biggest')) {
              const result = await db.executeQuery(
                "SELECT canonical_vendor, total_spend FROM vendor_spend_summary WHERE run_id = $1 ORDER BY total_spend DESC LIMIT 10",
                [runId]
              );
              const vendors = result.rows.map((r: any, i: number) =>
                `${i + 1}. ${r.canonical_vendor} — $${parseFloat(r.total_spend).toLocaleString('en-US', { maximumFractionDigits: 0 })}`
              ).join('\n');
              return { content: [{ type: 'text', text: `Top renegotiation targets (by spend):\n${vendors}` }] };
            }

            if (question.includes('tail') || question.includes('long tail') || question.includes('small vendor')) {
              const tailRow = await db.executeQuery(
                "SELECT COUNT(*) as cnt, SUM(total_spend) as spend FROM tail_spend_summary WHERE run_id = $1",
                [runId]
              );
              const targetReduction = Math.round(parseInt(tailRow.rows[0]?.cnt || '0') * 0.6);
              const savings = targetReduction * 2500;
              return {
                content: [{
                  type: 'text',
                  text: `Long-tail summary for run ${runId}:\n` +
                    `• ${tailRow.rows[0]?.cnt} tail vendors with $${parseFloat(tailRow.rows[0]?.spend || '0').toLocaleString('en-US', { maximumFractionDigits: 0 })} total spend\n` +
                    `• Reducing by 60% (${targetReduction} vendors) saves an estimated $${savings.toLocaleString('en-US', { maximumFractionDigits: 0 })} in admin overhead`
                }]
              };
            }

            if (question.includes('consolidat') || question.includes('fragment')) {
              const result = await db.executeQuery(
                "SELECT category, vendor_count, total_spend FROM fragmented_categories WHERE run_id = $1 ORDER BY fragmentation_score DESC LIMIT 6",
                [runId]
              );
              const cats = result.rows.map((r: any) =>
                `• ${r.category}: ${r.vendor_count} vendors, $${parseFloat(r.total_spend).toLocaleString('en-US', { maximumFractionDigits: 0 })} spend`
              ).join('\n');
              return { content: [{ type: 'text', text: `Most fragmented categories:\n${cats}` }] };
            }

            if (question.includes('saving') || question.includes('opportunit') || question.includes('how much')) {
              const result = await db.executeQuery(
                "SELECT action_type, target, impact_estimate FROM savings_opportunities WHERE run_id = $1 ORDER BY id",
                [runId]
              );
              const opps = result.rows.map((r: any) =>
                `• [${r.action_type.toUpperCase()}] ${r.target} — ${r.impact_estimate}`
              ).join('\n');
              return { content: [{ type: 'text', text: `Savings opportunities for run ${runId}:\n${opps}` }] };
            }

            if (question.includes('memo') || question.includes('report') || question.includes('summary')) {
              const result = await db.executeQuery(
                "SELECT markdown_content, pdf_path FROM memo_outputs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
                [runId]
              );
              if (result.rows.length) {
                return { content: [{ type: 'text', text: result.rows[0].markdown_content }] };
              }
              return { content: [{ type: 'text', text: 'No memo found for this run yet.' }] };
            }

            // Fallback: return run summary
            const summaryResult = await db.executeQuery(
              "SELECT SUM(total_spend) as total, COUNT(*) as vendors FROM vendor_spend_summary WHERE run_id = $1",
              [runId]
            );
            const oppsResult = await db.executeQuery(
              "SELECT COUNT(*) as cnt FROM savings_opportunities WHERE run_id = $1",
              [runId]
            );
            return {
              content: [{
                type: 'text',
                text: `Run ${runId} overview:\n` +
                  `• Total spend: $${parseFloat(summaryResult.rows[0]?.total || '0').toLocaleString('en-US', { maximumFractionDigits: 0 })}\n` +
                  `• Vendors: ${summaryResult.rows[0]?.vendors}\n` +
                  `• Opportunities identified: ${oppsResult.rows[0]?.cnt}\n\n` +
                  `Try asking: "Which vendors should be renegotiated?", "What's in the long tail?", "Show consolidation opportunities", "What are the savings opportunities?"`
              }]
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

    // -----------------------------------------------------------------------
    // Resources
    // -----------------------------------------------------------------------
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => ({
      resources: [
        {
          uri: 'run://latest/summary',
          name: 'Latest Run Summary',
          description: 'High-level summary of the most recent completed analysis run.',
          mimeType: 'application/json'
        },
        {
          uri: 'run://latest/opportunities',
          name: 'Latest Savings Opportunities',
          description: 'All savings opportunities from the most recent run.',
          mimeType: 'application/json'
        },
        {
          uri: 'run://latest/memo',
          name: 'Latest Executive Memo',
          description: 'Full text of the executive memo from the most recent run.',
          mimeType: 'text/markdown'
        }
      ]
    }));

    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      const uri = request.params.uri;

      if (uri === 'run://latest/summary') {
        const runRow = await db.executeQuery(
          "SELECT id FROM analysis_runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
        );
        if (!runRow.rows.length) {
          return { contents: [{ uri, mimeType: 'application/json', text: '{"error":"No completed runs found"}' }] };
        }
        const runId = runRow.rows[0].id;
        const [totalRow, vendorCount, top10Row] = await Promise.all([
          db.executeQuery("SELECT SUM(total_spend) as total FROM vendor_spend_summary WHERE run_id = $1", [runId]),
          db.executeQuery("SELECT COUNT(*) as cnt FROM vendor_spend_summary WHERE run_id = $1", [runId]),
          db.executeQuery("SELECT SUM(total_spend) as top10 FROM (SELECT total_spend FROM vendor_spend_summary WHERE run_id = $1 ORDER BY total_spend DESC LIMIT 10) t", [runId]),
        ]);
        const total = parseFloat(totalRow.rows[0]?.total || '0');
        const top10 = parseFloat(top10Row.rows[0]?.top10 || '0');
        return {
          contents: [{
            uri, mimeType: 'application/json',
            text: JSON.stringify({
              run_id: runId,
              total_spend: `$${total.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
              total_vendors: parseInt(vendorCount.rows[0]?.cnt || '0'),
              top10_concentration_pct: total > 0 ? `${((top10 / total) * 100).toFixed(1)}%` : '0%',
            }, null, 2)
          }]
        };
      }

      if (uri === 'run://latest/opportunities') {
        const runRow = await db.executeQuery(
          "SELECT id FROM analysis_runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
        );
        if (!runRow.rows.length) {
          return { contents: [{ uri, mimeType: 'application/json', text: '[]' }] };
        }
        const runId = runRow.rows[0].id;
        const result = await db.executeQuery(
          "SELECT target, action_type, rationale, impact_estimate FROM savings_opportunities WHERE run_id = $1 ORDER BY id",
          [runId]
        );
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(result.rows, null, 2) }] };
      }

      if (uri === 'run://latest/memo') {
        const runRow = await db.executeQuery(
          "SELECT id FROM analysis_runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
        );
        if (!runRow.rows.length) {
          return { contents: [{ uri, mimeType: 'text/markdown', text: 'No completed runs found.' }] };
        }
        const runId = runRow.rows[0].id;
        const result = await db.executeQuery(
          "SELECT markdown_content FROM memo_outputs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
          [runId]
        );
        return {
          contents: [{
            uri, mimeType: 'text/markdown',
            text: result.rows[0]?.markdown_content || 'Memo not yet generated for this run.'
          }]
        };
      }

      throw new Error(`Unknown resource: ${uri}`);
    });
  }

  private setupExpress() {
    this.app.use(cors({
      origin: '*',
      exposedHeaders: ['Mcp-Session-Id'],
      allowedHeaders: ['Content-Type', 'Authorization', 'Mcp-Session-Id']
    }));
    this.app.use(express.json());

    // ------------------------------------------------------------------
    // Health check
    // ------------------------------------------------------------------
    this.app.get('/health', (_req: express.Request, res: express.Response) => {
      res.json({ status: 'ok', server: 'vendor-mcp-server', version: '2.0.0' });
    });

    // ------------------------------------------------------------------
    // OAuth 2.0 — PKCE with dynamic client registration (RFC 7591)
    // Required for Claude.ai and OpenAI to authenticate via MCP
    // ------------------------------------------------------------------

    const getBaseUrl = (req: express.Request): string => {
      const proto = (getQueryParam(req.headers['x-forwarded-proto'] as any) || req.protocol).split(',')[0].trim();
      const host = getQueryParam(req.headers['x-forwarded-host'] as any) || req.headers.host || `localhost:${process.env.PORT || 8765}`;
      return `${proto}://${host}`;
    };

    const sendUnauthorized = (req: express.Request, res: express.Response, message: string): void => {
      const base = getBaseUrl(req);
      res.setHeader('WWW-Authenticate', `Bearer resource_metadata="${base}/.well-known/oauth-protected-resource", scope="${DEFAULT_SCOPE}"`);
      res.status(401).json({ jsonrpc: '2.0', error: { code: -32001, message }, id: null });
    };

    // Protected resource metadata (RFC 9728)
    this.app.get('/.well-known/oauth-protected-resource', (req: express.Request, res: express.Response) => {
      const base = getBaseUrl(req);
      res.json({ resource: base, authorization_servers: [base], scopes_supported: Array.from(SCOPES) });
    });

    // Authorization server metadata (RFC 8414)
    this.app.get('/.well-known/oauth-authorization-server', (req: express.Request, res: express.Response) => {
      const base = getBaseUrl(req);
      res.json({
        issuer: base,
        authorization_endpoint: `${base}/oauth/authorize`,
        token_endpoint: `${base}/oauth/token`,
        registration_endpoint: `${base}/oauth/register`,
        response_types_supported: ['code'],
        grant_types_supported: ['authorization_code'],
        code_challenge_methods_supported: ['S256'],
        token_endpoint_auth_methods_supported: ['none'],
        scopes_supported: Array.from(SCOPES),
      });
    });

    // Dynamic client registration (RFC 7591)
    this.app.post('/oauth/register', (req: express.Request, res: express.Response) => {
      const base = getBaseUrl(req);
      const body = req.body || {};
      const redirectUris = (Array.isArray(body.redirect_uris) ? body.redirect_uris : [])
        .map((u: unknown) => (typeof u === 'string' ? u.trim() : ''))
        .filter((u: string) => u.length > 0);

      if (redirectUris.length === 0) {
        res.status(400).json({ error: 'invalid_client_metadata', error_description: 'redirect_uris must be a non-empty array' });
        return;
      }

      const authMethod = typeof body.token_endpoint_auth_method === 'string' ? body.token_endpoint_auth_method.toLowerCase() : 'none';
      if (authMethod !== 'none') {
        res.status(400).json({ error: 'invalid_client_metadata', error_description: 'Only public clients with PKCE are supported (token_endpoint_auth_method="none")' });
        return;
      }

      const clientId = `mcp-client-${randomUUID()}`;
      const registrationAccessToken = `reg_${randomUUID()}`;
      const issuedAt = Math.floor(Date.now() / 1000);
      const normalizedScope = sanitizeScope(body.scope);

      registeredClients.set(clientId, {
        clientId,
        clientName: typeof body.client_name === 'string' ? body.client_name.trim() : undefined,
        redirectUris,
        scope: normalizedScope,
        tokenEndpointAuthMethod: 'none',
        clientIdIssuedAt: issuedAt,
        clientSecretExpiresAt: 0,
        registrationAccessToken,
      });
      saveOAuthState();

      res.status(201).json({
        client_id: clientId,
        client_id_issued_at: issuedAt,
        client_secret_expires_at: 0,
        token_endpoint_auth_method: 'none',
        registration_access_token: registrationAccessToken,
        registration_client_uri: `${base}/oauth/register/${clientId}`,
        redirect_uris: redirectUris,
        scope: normalizedScope,
        grant_types: ['authorization_code'],
        response_types: ['code'],
        client_name: body.client_name,
      });
    });

    // Authorization endpoint — validates PKCE params then redirects to GitHub
    this.app.get('/oauth/authorize', (req: express.Request, res: express.Response) => {
      const clientId = getQueryParam(req.query.client_id as any);
      const redirectUri = getQueryParam(req.query.redirect_uri as any);
      const state = getQueryParam(req.query.state as any);
      const codeChallenge = getQueryParam(req.query.code_challenge as any);
      const codeChallengeMethod = (getQueryParam(req.query.code_challenge_method as any) || 'S256').toUpperCase();
      const requestedScope = sanitizeScope(getQueryParam(req.query.scope as any));

      if (!clientId || !redirectUri || !state || !codeChallenge) {
        res.status(400).json({ error: 'invalid_request', error_description: 'client_id, redirect_uri, state, and code_challenge are required' });
        return;
      }
      if (codeChallengeMethod !== 'S256') {
        res.status(400).json({ error: 'invalid_request', error_description: 'Only S256 code_challenge_method is supported' });
        return;
      }

      // ChatGPT fixed redirect URI — always allow
      const CHATGPT_REDIRECT = 'https://chatgpt.com/connector_platform_oauth_redirect';
      const OPENAI_REVIEW_REDIRECT = 'https://platform.openai.com/apps-manage/oauth';
      const isKnownPlatformUri = redirectUri === CHATGPT_REDIRECT || redirectUri === OPENAI_REVIEW_REDIRECT;

      // Auto-register unknown clients (Claude/OpenAI register dynamically per session)
      if (!registeredClients.has(clientId)) {
        registeredClients.set(clientId, {
          clientId, redirectUris: [redirectUri], scope: requestedScope,
          tokenEndpointAuthMethod: 'none', clientIdIssuedAt: Math.floor(Date.now() / 1000),
          clientSecretExpiresAt: 0, registrationAccessToken: `auto_${randomUUID()}`,
        });
      } else if (isKnownPlatformUri) {
        // Ensure ChatGPT redirect is always in the allowed list
        const existing = registeredClients.get(clientId)!;
        if (!existing.redirectUris.includes(redirectUri)) {
          existing.redirectUris.push(redirectUri);
        }
      }

      const githubClientId = process.env.GITHUB_OAUTH_CLIENT_ID;
      const githubCallbackUrl = process.env.GITHUB_OAUTH_CALLBACK_URL || `${process.env.MCP_PUBLIC_URL}/auth/github/callback`;

      if (!githubClientId) {
        res.status(500).json({ error: 'server_error', error_description: 'GitHub OAuth not configured' });
        return;
      }

      // resource parameter — echo throughout flow per RFC 9728 / OpenAI requirement
      const resourceParam = getQueryParam(req.query.resource as any);

      const githubState = randomUUID();
      pendingAuthStates.set(githubState, {
        type: 'oauth', clientId, redirectUri, codeChallenge, codeChallengeMethod: 'S256',
        scope: requestedScope, originalState: state, createdAt: Date.now(),
        resource: resourceParam,
      });
      saveOAuthState();

      const params = new URLSearchParams({
        client_id: githubClientId,
        redirect_uri: githubCallbackUrl,
        scope: 'read:user',
        state: githubState,
        allow_signup: 'false',
      });
      res.redirect(`https://github.com/login/oauth/authorize?${params.toString()}`);
    });

    // Token endpoint — PKCE verification + issue access token
    this.app.post('/oauth/token', (req: express.Request, res: express.Response) => {
      const { grant_type, code, redirect_uri, client_id, code_verifier } = req.body || {};

      if (grant_type !== 'authorization_code') {
        res.status(400).json({ error: 'unsupported_grant_type', error_description: 'Only authorization_code is supported' });
        return;
      }
      if (!code || !redirect_uri || !client_id || !code_verifier) {
        res.status(400).json({ error: 'invalid_request', error_description: 'code, redirect_uri, client_id, and code_verifier are required' });
        return;
      }

      const record = authorizationCodes.get(code);
      if (!record) {
        const base = getBaseUrl(req);
        res.setHeader('WWW-Authenticate', `Bearer resource_metadata="${base}/.well-known/oauth-protected-resource"`);
        res.status(401).json({ error: 'invalid_grant', error_description: 'Unknown authorization code — please re-authorize' });
        return;
      }
      if (record.clientId !== client_id || record.redirectUri !== redirect_uri) {
        authorizationCodes.delete(code);
        res.status(400).json({ error: 'invalid_grant', error_description: 'Client or redirect URI mismatch' });
        return;
      }
      if (Date.now() > record.expiresAt) {
        authorizationCodes.delete(code);
        res.status(400).json({ error: 'invalid_grant', error_description: 'Authorization code expired' });
        return;
      }
      if (sha256Base64Url(code_verifier) !== record.codeChallenge) {
        authorizationCodes.delete(code);
        res.status(400).json({ error: 'invalid_grant', error_description: 'PKCE verification failed' });
        return;
      }

      authorizationCodes.delete(code);
      const accessToken = `mcp_${randomUUID()}`;
      issuedTokens.set(accessToken, {
        user: record.user, createdAt: Date.now(), scope: record.scope,
        expiresAt: Date.now() + ACCESS_TOKEN_TTL_MS,
      });
      saveOAuthState();

      const tokenResponse: Record<string, unknown> = {
        access_token: accessToken,
        token_type: 'Bearer',
        expires_in: Math.floor(ACCESS_TOKEN_TTL_MS / 1000),
        scope: record.scope,
      };
      if (record.resource) tokenResponse.resource = record.resource;
      res.json(tokenResponse);
    });

    // GitHub OAuth callback — exchanges GitHub code, mints internal token
    this.app.get('/auth/github/callback', async (req: express.Request, res: express.Response) => {
      try {
        const errorParam = getQueryParam(req.query.error as any);
        if (errorParam) { res.status(400).send(`GitHub OAuth declined: ${errorParam}`); return; }

        const codeParam = getQueryParam(req.query.code as any);
        const stateParam = getQueryParam(req.query.state as any);
        if (!codeParam || !stateParam) { res.status(400).send('OAuth callback missing code or state'); return; }

        const authState = pendingAuthStates.get(stateParam);
        pendingAuthStates.delete(stateParam);
        saveOAuthState();

        if (!authState || Date.now() - authState.createdAt > 15 * 60 * 1000) {
          res.status(400).send('Invalid or expired OAuth state');
          return;
        }

        const ghClientId = process.env.GITHUB_OAUTH_CLIENT_ID;
        const ghClientSecret = process.env.GITHUB_OAUTH_SECRET;
        const callbackUrl = process.env.GITHUB_OAUTH_CALLBACK_URL || `${process.env.MCP_PUBLIC_URL}/auth/github/callback`;

        const tokenResp = await fetch('https://github.com/login/oauth/access_token', {
          method: 'POST',
          headers: { 'content-type': 'application/json', accept: 'application/json' },
          body: JSON.stringify({ client_id: ghClientId, client_secret: ghClientSecret, code: codeParam, redirect_uri: callbackUrl }),
        });
        const tokenJson: any = await tokenResp.json();
        const ghToken = tokenJson.access_token as string | undefined;
        if (!ghToken) { res.status(401).send('No GitHub access token received'); return; }

        const userResp = await fetch('https://api.github.com/user', {
          headers: { Authorization: `Bearer ${ghToken}`, 'user-agent': 'vendor-mcp-server' },
        });
        const userJson: any = userResp.ok ? await userResp.json() : {};
        const login = userJson.login || 'github-user';

        if (authState.type === 'manual') {
          const apiToken = `mcp_${randomUUID()}`;
          issuedTokens.set(apiToken, { user: login, createdAt: Date.now(), scope: authState.scope, expiresAt: null });
          saveOAuthState();
          res.setHeader('content-type', 'text/html; charset=utf-8');
          res.send(`<!DOCTYPE html><html><head><title>Vendor MCP - API Token</title></head><body>
            <h1>Authentication complete</h1>
            <p>Signed in as: <strong>${login}</strong></p>
            <p>Use this token in your MCP client as an HTTP header:</p>
            <pre>Authorization: Bearer ${apiToken}</pre>
          </body></html>`);
          return;
        }

        if (authState.type === 'oauth') {
          const authCode = randomUUID();
          authorizationCodes.set(authCode, {
            clientId: authState.clientId, redirectUri: authState.redirectUri,
            codeChallenge: authState.codeChallenge, codeChallengeMethod: 'S256',
            scope: authState.scope, user: login,
            createdAt: Date.now(), expiresAt: Date.now() + AUTH_CODE_TTL_MS,
            resource: authState.resource,
          });
          saveOAuthState();
          const redirectTarget = new URL(authState.redirectUri);
          redirectTarget.searchParams.set('code', authCode);
          redirectTarget.searchParams.set('state', authState.originalState);
          res.redirect(redirectTarget.toString());
          return;
        }

        res.status(500).send('Unexpected auth flow');
      } catch (e) {
        res.status(500).send(`OAuth callback error: ${e instanceof Error ? e.message : String(e)}`);
      }
    });

    // Manual login (browser flow for getting a raw API token)
    this.app.get('/auth/github/login', (req: express.Request, res: express.Response) => {
      const ghClientId = process.env.GITHUB_OAUTH_CLIENT_ID;
      const callbackUrl = process.env.GITHUB_OAUTH_CALLBACK_URL || `${process.env.MCP_PUBLIC_URL}/auth/github/callback`;
      if (!ghClientId) { res.status(500).send('GitHub OAuth not configured'); return; }
      const state = randomUUID();
      pendingAuthStates.set(state, { type: 'manual', createdAt: Date.now(), scope: DEFAULT_SCOPE });
      saveOAuthState();
      const params = new URLSearchParams({ client_id: ghClientId, redirect_uri: callbackUrl, scope: 'read:user', state });
      res.redirect(`https://github.com/login/oauth/authorize?${params.toString()}`);
    });

    // ------------------------------------------------------------------
    // Auth middleware for MCP endpoint
    // ------------------------------------------------------------------
    const requireAuth = (req: express.Request, res: express.Response, next: express.NextFunction): void => {
      if (process.env.REQUIRE_AUTH !== 'true') { next(); return; }
      const auth = req.headers.authorization || '';
      if (!auth.startsWith('Bearer ')) { sendUnauthorized(req, res, 'Missing bearer token'); return; }
      const token = auth.slice(7).trim();
      if (!isTokenAuthorized(token)) { sendUnauthorized(req, res, 'Invalid or expired bearer token'); return; }
      next();
    };

    // ------------------------------------------------------------------
    // MCP endpoint
    // ------------------------------------------------------------------
    this.app.all('/mcp', requireAuth, async (req: express.Request, res: express.Response) => {
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
      console.log(`OAuth: ${process.env.REQUIRE_AUTH === 'true' ? 'ENABLED' : 'DISABLED (set REQUIRE_AUTH=true to enable)'}`);
      if (process.env.REQUIRE_AUTH === 'true') {
        console.log(`  Authorize at: ${process.env.MCP_PUBLIC_URL || `http://localhost:${port}`}/oauth/authorize`);
      }
    });
  }
}

const mcpServer = new VendorMCPServer();
mcpServer.start().catch(console.error);

process.on('SIGINT', async () => {
  await db.close();
  process.exit(0);
});
