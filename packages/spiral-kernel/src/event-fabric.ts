import { Pool, PoolClient } from "pg";

export interface SpiralEventRecord {
  event_id: string;
  tenant_id: string;
  source_module: string;
  event_type: string;
  correlation_id: string;
  causation_id: string | null;
  payload: Record<string, unknown>;
  confidence: number;
  schema_version: number;
  created_at: Date;
}

export class EventFabric {
  constructor(private readonly pool: Pool) {}

  public async append(
    tenantId: string,
    sourceModule: string,
    eventType: string,
    correlationId: string,
    payload: Record<string, unknown>,
    confidence = 1.0,
    causationId?: string
  ): Promise<string> {
    if (!tenantId?.trim()) {
      throw new Error("[EVENT FABRIC SECURITY] tenant_id is required");
    }
    if (confidence < 0 || confidence > 1) {
      throw new Error("[EVENT FABRIC VALIDATION] confidence must be between 0 and 1");
    }

    const client: PoolClient = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        [tenantId]
      );

      const result = await client.query<{ event_id: string }>(
        `INSERT INTO event_log
          (tenant_id, source_module, event_type, correlation_id, causation_id, payload, confidence)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
         RETURNING event_id`,
        [
          tenantId,
          sourceModule,
          eventType,
          correlationId,
          causationId ?? null,
          JSON.stringify(payload),
          confidence
        ]
      );

      await client.query("COMMIT");
      return result.rows[0].event_id;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  public async replayByCorrelation(
    tenantId: string,
    correlationId: string
  ): Promise<SpiralEventRecord[]> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        [tenantId]
      );

      const result = await client.query<SpiralEventRecord>(
        `SELECT event_id, tenant_id, source_module, event_type,
                correlation_id, causation_id, payload, confidence,
                schema_version, created_at
           FROM event_log
          WHERE tenant_id = $1 AND correlation_id = $2
          ORDER BY created_at ASC, event_id ASC`,
        [tenantId, correlationId]
      );

      await client.query("COMMIT");
      return result.rows;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
