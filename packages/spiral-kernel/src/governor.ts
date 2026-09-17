export interface GovernanceEvaluation {
  allowed: boolean;
  sanitizedPayload: Record<string, unknown>;
  reason?: string;
}

export class MemoryGovernor {
  public evaluateIngestion(
    tenantId: string,
    payload: Record<string, unknown>
  ): GovernanceEvaluation {
    if (!tenantId?.trim()) {
      return {
        allowed: false,
        sanitizedPayload: {},
        reason: "VIOLATION: TENANT_ID_REQUIRED"
      };
    }

    const raw = JSON.stringify(payload);

    // Defense-in-depth only. PostgreSQL RLS remains the tenant boundary.
    const secretOrPii =
      /(\d{3}\.\d{3}\.\d{3}-\d{2})|(sk_live_[0-9a-zA-Z]{16,})/;

    if (secretOrPii.test(raw)) {
      return {
        allowed: false,
        sanitizedPayload: {},
        reason: "VIOLATION: PII_OR_SECRET_EXPOSURE_BLOCKED"
      };
    }

    return { allowed: true, sanitizedPayload: payload };
  }

  public validateBranchCapability(
    _branchName: string,
    requestedAction: string,
    governanceLevel: number
  ): boolean {
    if (governanceLevel >= 3 && requestedAction.includes("auto-apply-policy")) {
      return false;
    }
    return true;
  }
}
