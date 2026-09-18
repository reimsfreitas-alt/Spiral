import { Pool } from "pg";
import { SpiralOS } from "../../packages/spiral-kernel/src/orchestrator";

describe("Spiral OS kernel ingest", () => {
  const databaseUrl = process.env.DATABASE_URL;
  const run = databaseUrl ? describe : describe.skip;

  run("governs and persists an allowed event", async () => {
    const pool = new Pool({ connectionString: databaseUrl });
    const os = new SpiralOS(pool);
    const tenantId = "44444444-4444-4444-4444-444444444444";
    const correlationId = "99999999-9999-9999-9999-999999999999";

    try {
      await pool.query(
        "INSERT INTO tenants(tenant_id,name) VALUES($1,$2) ON CONFLICT (tenant_id) DO NOTHING",
        [tenantId, "Kernel Ingest E2E"]
      );

      const result = await os.ingest({
        tenantId,
        sourceModule: "spiral-lite",
        eventType: "opportunity.detected",
        correlationId,
        payload: { signal: "visited_pricing" },
        confidence: 0.95
      });

      expect(result.allowed).toBe(true);
      expect(result.eventId).toEqual(expect.any(String));

      const row = await pool.query(
        "SELECT event_type, source_module FROM event_log WHERE event_id=$1 AND tenant_id=$2",
        [result.eventId, tenantId]
      );
      expect(row.rows).toHaveLength(1);
      expect(row.rows[0]).toEqual({
        event_type: "opportunity.detected",
        source_module: "spiral-lite"
      });
    } finally {
      await pool.end();
    }
  });

  run("blocks obvious PII before persistence", async () => {
    const pool = new Pool({ connectionString: databaseUrl });
    const os = new SpiralOS(pool);
    const tenantId = "55555555-5555-5555-5555-555555555555";
    const correlationId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

    try {
      await pool.query(
        "INSERT INTO tenants(tenant_id,name) VALUES($1,$2) ON CONFLICT (tenant_id) DO NOTHING",
        [tenantId, "Kernel Governance E2E"]
      );

      const result = await os.ingest({
        tenantId,
        sourceModule: "spiral-lite",
        eventType: "opportunity.detected",
        correlationId,
        payload: { phone: "123.456.789-09" },
        confidence: 1
      });

      expect(result.allowed).toBe(false);
      expect(result.eventId).toBeUndefined();

      const row = await pool.query(
        "SELECT COUNT(*)::int AS count FROM event_log WHERE tenant_id=$1 AND correlation_id=$2",
        [tenantId, correlationId]
      );
      expect(row.rows[0].count).toBe(0);
    } finally {
      await pool.end();
    }
  });
});
