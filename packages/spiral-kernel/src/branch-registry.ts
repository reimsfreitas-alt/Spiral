import { Pool } from "pg";

export interface BranchRegistration {
  branchName: string;
  endpointUrl?: string;
  capabilities?: Record<string, unknown>;
}

export class BranchRegistry {
  constructor(private readonly pool: Pool) {}

  async register(tenantId: string, branch: BranchRegistration): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT set_config('app.current_tenant_id',$1,true)", [tenantId]);
      await client.query(
        `INSERT INTO branch_registry
          (tenant_id, branch_name, endpoint_url, capabilities)
         VALUES ($1,$2,$3,$4::jsonb)
         ON CONFLICT (tenant_id,branch_name)
         DO UPDATE SET endpoint_url=EXCLUDED.endpoint_url,
                       capabilities=EXCLUDED.capabilities,
                       status='ACTIVE',
                       updated_at=NOW()`,
        [tenantId, branch.branchName, branch.endpointUrl ?? null, JSON.stringify(branch.capabilities ?? {})]
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async list(tenantId: string): Promise<BranchRegistration[]> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT set_config('app.current_tenant_id',$1,true)", [tenantId]);
      const result = await client.query(
        `SELECT branch_name, endpoint_url, capabilities
           FROM branch_registry
          WHERE tenant_id=$1 AND status='ACTIVE'
          ORDER BY branch_name`,
        [tenantId]
      );
      await client.query("COMMIT");
      return result.rows.map(row => ({
        branchName: row.branch_name,
        endpointUrl: row.endpoint_url ?? undefined,
        capabilities: row.capabilities ?? {}
      }));
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
