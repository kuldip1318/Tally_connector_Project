import { syncTallyData } from '../scripts/tally-sync';

interface DbParams {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export class TallyLedgerSync {
  private dbParams: DbParams;
  private tallyUrl: string;

  constructor(dbParams: DbParams, tallyUrl: string) {
    this.dbParams = dbParams;
    this.tallyUrl = tallyUrl;
  }

  public async syncLedgers(): Promise<string> {
    try {
      console.log("Starting ledger synchronization...");
      process.env.DB_HOST = this.dbParams.host;
      process.env.DB_PORT = this.dbParams.port.toString();
      process.env.DB_NAME = this.dbParams.database;
      process.env.DB_USER = this.dbParams.user;
      process.env.DB_PASSWORD = this.dbParams.password;
      process.env.TALLY_URL = this.tallyUrl;
      const result = await syncTallyData();
      console.log("Ledger synchronization completed successfully");
      return result;
    } catch (error) {
      console.error("Ledger synchronization failed:", error);
      throw error;
    }
  }
}

