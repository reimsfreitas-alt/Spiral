import { KnowledgeRecord } from "./knowledge-fabric";

export interface RouteInstruction {
  targetBranch: string;
  action: string;
  payload: Record<string, unknown>;
}

export class KnowledgeRouter {
  public dispatch(knowledge: KnowledgeRecord): RouteInstruction[] {
    if (knowledge.status !== "ACTIVE") return [];

    const routes: RouteInstruction[] = [];

    if (
      knowledge.knowledge_type === "signal_efficacy" &&
      knowledge.governance_level <= 2
    ) {
      routes.push({
        targetBranch: "spiral-lite",
        action: "apply-signal-optimization",
        payload: knowledge.pattern
      });
    }

    if (
      knowledge.knowledge_type === "provider_latency_pattern" &&
      knowledge.governance_level <= 2
    ) {
      routes.push({
        targetBranch: "spiral-distribution",
        action: "adjust-retry-backoff",
        payload: knowledge.pattern
      });
    }

    return routes;
  }
}
