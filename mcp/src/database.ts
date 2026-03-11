import pg from 'pg';
const { Pool } = pg;

export class DatabaseManager {
  private pool: pg.Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.PGHOST || 'localhost',
      port: parseInt(process.env.PGPORT || '5432'),
      database: process.env.PGDATABASE || 'vendor_mcp',
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      query_timeout: 30000,
      statement_timeout: 30000,
    });

    this.pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
    });
    
    console.log(`Database configured: ${process.env.PGHOST}:${process.env.PGPORT}/${process.env.PGDATABASE}`);
  }

  async query(text: string, params?: any[]) {
    const start = Date.now();
    try {
      const res = await this.pool.query(text, params);
      const duration = Date.now() - start;
      console.log('Executed query', { text, duration, rows: res.rowCount });
      return res;
    } catch (error) {
      console.error('Database query error:', error);
      throw error;
    }
  }

  async getTables(): Promise<string[]> {
    const result = await this.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      ORDER BY table_name
    `);
    return result.rows.map(row => row.table_name);
  }

  async getTableSchema(tableName: string) {
    const result = await this.query(`
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns
      WHERE table_schema = 'public' 
        AND table_name = $1
      ORDER BY ordinal_position
    `, [tableName]);
    return result.rows;
  }

  async executeQuery(query: string, params?: any[]) {
    return await this.query(query, params);
  }

  async close() {
    await this.pool.end();
  }
}
