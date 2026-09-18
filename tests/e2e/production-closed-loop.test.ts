import { Pool } from "pg";
import { EventFabric } from "../../packages/spiral-kernel/src/event-fabric";
import { KnowledgeFabric } from "../../packages/spiral-kernel/src/knowledge-fabric";
import { KnowledgeRouter } from "../../packages/spiral-kernel/src/router";

class LiteBranch {
  public state = { signalWeights: { visited_pricing: 1 } };
  constructor(private readonly events: EventFabric, private readonly tenantId: string) {}
  async execute(correlationId: string): Promise<string> {
    return this.events.append(
      this.tenantId, "spiral-lite", "execution.completed", correlationId,
      { signal: "visited_pricing", applied_weight: this.state.signalWeights.visited_pricing, converted: true }, 0.98
    );
  }
  applyKnowledge(pattern: Record<string, unknown>): void {
    const multiplier = pattern.weight_multiplier;
    if (typeof multiplier !== "number" || multiplier <= 0) throw new Error("Invalid signal multiplier");
    this.state.signalWeights.visited_pricing *= multiplier;
  }
}

const databaseUrl = process.env.DATABASE_URL;

describe("Spiral OS production closed loop", () => {
  if (!databaseUrl) return;

  test("event -> knowledge -> mutation -> execution -> Truth -> knowledge V2", async () => {
    const pool = new Pool({ connectionString: databaseUrl });
    const tenantId = "22222222-2222-2222-2222-222222222222";
    const correlationId = "77777777-7777-7777-7777-777777777777";

    try {
      await pool.query(
        "INSERT INTO tenants(tenant_id,name) VALUES($1,$2) ON CONFLICT (tenant_id) DO NOTHING",
        [tenantId, "Spiral E2E Tenant"]
      );

      const events = new EventFabric(pool);
      const knowledge = new KnowledgeFabric(pool);
      const router = new KnowledgeRouter();
      const lite = new LiteBranch(events, tenantId);

      const event1 = await lite.execute(correlationId);
      const knowledgeV1 = await knowledge.aggregateAndUpsert(
        tenantId, "signal:visited_pricing", "signal_efficacy", event1,
        { weight_multiplier: 1.5 }
      );

      expect(knowledgeV1.version).toBe(1);
      expect(knowledgeV1.support_count).toBe(1);

      const routes = router.dispatch(knowledgeV1);
      expect(routes).toHaveLength(1);
      expect(routes[0].targetBranch).toBe("spiral-lite");

      lite.applyKnowledge(routes[0].payload);
      expect(lite.state.signalWeights.visited_pricing).toBe(1.5);

      const event2 = await lite.execute(correlationId);

      const truthEvent = await events.append(
        tenantId, "spiral-truth", "truth.verified", correlationId,
        {
          observed_execution_id: event2,
          applied_weight: lite.state.signalWeights.visited_pricing,
          verdict: "CONFIRMED"
        }, 0.99, { causationId: event2 }
      );

      const knowledgeV2 = await knowledge.aggregateAndUpsert(
        tenantId, "signal:visited_pricing", "signal_efficacy", truthEvent,
        { weight_multiplier: 1.5 }
      );

      expect(knowledgeV2.version).toBe(2);
      expect(knowledgeV2.support_count).toBe(2);
      expect(knowledgeV2.lineage.source_event_ids).toEqual(expect.arrayContaining([event1, truthEvent]));
      expect(lite.state.signalWeights.visited_pricing).toBeGreaterThan(1);
    } finally {
      await pool.end();
    }
  });
});
