import { Pool } from "pg";
import { EventFabric } from "../../packages/spiral-kernel/src/event-fabric";
import { KnowledgeFabric } from "../../packages/spiral-kernel/src/knowledge-fabric";
import { LiteBranchAdapter, SpiralOS } from "../../packages/spiral-kernel/src/orchestrator";

describe("Spiral OS branch orchestration", () => {
  const url = process.env.DATABASE_URL;
  const run = url ? describe : describe.skip;

  run("routes real knowledge into a real Lite branch and records the mutation", async () => {
    const pool = new Pool({ connectionString: url });
    const tenantId = "33333333-3333-3333-3333-333333333333";
    const correlationId = "88888888-8888-8888-8888-888888888888";

    try {
      await pool.query(
        "INSERT INTO tenants(tenant_id,name) VALUES($1,$2) ON CONFLICT DO NOTHING",
        [tenantId, "Spiral OS Orchestration E2E"]
      );

      const events = new EventFabric(pool);
      const knowledge = new KnowledgeFabric(pool);
      const os = new SpiralOS(pool);
      os.registerBranch(new LiteBranchAdapter(events));

      const event1 = await events.append(
        tenantId, "spiral-lite", "execution.completed", correlationId,
        { signal: "visited_pricing", applied_weight: 1 }, 0.98
      );

      const knowledgeV1 = await knowledge.aggregateAndUpsert(
        tenantId, "signal:visited_pricing", "signal_efficacy", event1,
        { weight_multiplier: 1.5 }
      );

      const results = await os.processKnowledge(tenantId, correlationId, knowledgeV1);

      expect(results).toHaveLength(1);
      expect(results[0].branch).toBe("spiral-lite");
      expect(results[0].weight).toBe(1.5);

      const eventsAfter = await events.replayByCorrelation(tenantId, correlationId);
      expect(eventsAfter.some(e => e.event_type === "configuration.updated")).toBe(true);
      expect(eventsAfter.some(e => e.event_type === "branch.execution.accepted")).toBe(true);
    } finally {
      await pool.end();
    }
  });
});
