export declare class DatabaseManager {
    private pool;
    constructor();
    query(text: string, params?: any[]): Promise<import("pg").QueryResult<any>>;
    getTables(): Promise<string[]>;
    getTableSchema(tableName: string): Promise<any[]>;
    executeQuery(query: string, params?: any[]): Promise<import("pg").QueryResult<any>>;
    close(): Promise<void>;
}
