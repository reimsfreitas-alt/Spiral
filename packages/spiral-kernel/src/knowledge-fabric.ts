import { Pool, PoolClient } from "pg";

export interface KnowledgeRecord {
  knowledge_id: string;
  tenant_id: string;
  knowledge_type: string;
  status: string;
  subject: string;
  pattern: Record<string, unknown>;
  confidence: number;
  support_count: number;
  freshness_score: number;
  governance_level: number;
  version: number;
  lineage: {
    discovered_by: string;
    discovery_timestamp: string;
    source_event_ids: string[];
    algorithm_version: string;
  };
}

export class KnowledgeFabric {
  constructor(private readonly pool: Pool) {}

  public async aggregateAndUpsert(
    tenantId: string,
    subject: string,
    knowledgeType: string,
    eventId: string,
    observedPattern: Record<string, unknown>
  ): Promise<KnowledgeRecord> {
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('app.current_tenant_id', $1, true)",
        [tenantId]
      );

      const existing = await client.query<KnowledgeRecord>(
        `SELECT * FROM knowledge_repository
          WHERE tenant_id = $1 AND subject = $2
          FOR UPDATE`,
        [tenantId, subject]
      );

      let record: KnowledgeRecord;

      if (existing.rows.length) {
        const current = existing.rows[0];
        const sourceIds = Array.from(
          new Set([...(current.lineage?.source_event_ids ?? []), eventId])
        );

        const support = current.support_count + 1;
        const priorConfidence = Number(current.confidence);
        const confidence = Math.min(
          0.99,
          priorConfidence + (1 - priorConfidence) * 0.1
        );

        const updated = await client.query<KnowledgeRecord>(
          `UPDATE knowledge_repository
              SET support_count = $1,
                  confidence = $2,
                  version = version + 1,
                  pattern = $3::jsonb,
                  lineage = $4::jsonb,
                  freshness_score = 1.0,
                  updated_at = NOW()
            WHERE knowledge_id = $5
            RETURNING *`,
          [
            support,
            confidence,
            JSON.stringify(observedPattern),
            JSON.stringify({
              ...current.lineage,
              source_event_ids: sourceIds,
              last_updated_at: new Date().toISOString()
            }),
            current.knowledge_id
          ]
        );

        record = updated.rows[0];
      } else {
        const inserted = await client.query<KnowledgeRecord>(
          `INSERT INTO knowledge_repository
            (tenant_id, knowledge_type, status, subject, pattern,
             confidence, support_count, freshness_score, governance_level,
             version, lineage)
           VALUES
            ($1, $2, 'ACTIVE', $3, $4::jsonb,
             0.80, 1, 1.0, 1, 1, $5::jsonb)
           RETURNING *`,
          [
            tenantId,
            knowledgeType,
            subject,
            JSON.stringify(observedPattern),
            JSON.stringify({
              discovered_by: "spiral-knowledge-fabric",
              discovery_timestamp: new Date().toISOString(),
              source_event_ids: [eventId],
              algorithm_version: "1.0"
            })
          ]
        );

        record = inserted.rows[0];
      }

      await client.query("COMMIT");
      return record;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
