import { Pool } from "pg";
import { EventFabric } from "./event-fabric";
import { KnowledgeRecord } from "./knowledge-fabric";
import { KnowledgeRouter } from "./router";

export interface BranchAdapter {
  branchName: string;
  canHandle(action: string): boolean;
  execute(input: {
    tenantId: string;
    correlationId: string;
    action: string;
    payload: Record<string, unknown>;
    causationId?: string;
  }): Promise<Record<string, unknown>>;
}

export class SpiralOS {
  private readonly events: EventFabric;
  private readonly router = new KnowledgeRouter();
  private readonly branches = new Map<string, BranchAdapter>();

  constructor(private readonly pool: Pool) {
    this.events = new EventFabric(pool);
  }

  registerBranch(branch: BranchAdapter): void {
    this.branches.set(branch.branchName, branch);
  }

  async processKnowledge(
    tenantId: string,
    correlationId: string,
    knowledge: KnowledgeRecord
  ): Promise<Record<string, unknown>[]> {
    const instructions = this.router.dispatch(knowledge);
    const results: Record<string, unknown>[] = [];

    for (const instruction of instructions) {
      const branch = this.branches.get(instruction.targetBranch);
      if (!branch || !branch.canHandle(instruction.action)) {
        await this.events.append(
          tenantId,
          "spiral-os",
          "route.blocked",
          correlationId,
          { target_branch: instruction.targetBranch, action: instruction.action },
          1,
          { causationId: knowledge.knowledge_id }
        );
        continue;
      }

      const result = await branch.execute({
        tenantId,
        correlationId,
        action: instruction.action,
        payload: instruction.payload,
        causationId: knowledge.knowledge_id
      });

      await this.events.append(
        tenantId,
        "spiral-os",
        "branch.execution.accepted",
        correlationId,
        {
          target_branch: instruction.targetBranch,
          action: instruction.action,
          result
        },
        1,
        { causationId: knowledge.knowledge_id }
      );

      results.push(result);
    }

    return results;
  }
}

export class LiteBranchAdapter implements BranchAdapter {
  branchName = "spiral-lite";
  private weight = 1;

  constructor(private readonly events: EventFabric) {}

  canHandle(action: string): boolean {
    return action === "apply-signal-optimization";
  }

  async execute(input: {
    tenantId: string;
    correlationId: string;
    action: string;
    payload: Record<string, unknown>;
    causationId?: string;
  }): Promise<Record<string, unknown>> {
    const multiplier = input.payload.weight_multiplier;
    if (typeof multiplier !== "number" || multiplier <= 0) {
      throw new Error("Invalid Lite signal multiplier");
    }

    this.weight *= multiplier;

    const eventId = await this.events.append(
      input.tenantId,
      this.branchName,
      "configuration.updated",
      input.correlationId,
      { signal: "visited_pricing", weight: this.weight },
      1,
      { causationId: input.causationId }
    );

    return { branch: this.branchName, weight: this.weight, eventId };
  }
}
