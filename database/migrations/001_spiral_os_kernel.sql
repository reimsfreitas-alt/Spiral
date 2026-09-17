CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_log (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
    source_module VARCHAR(100) NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    payload JSONB NOT NULL,
    confidence NUMERIC(3,2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    schema_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_log_tenant_type
    ON event_log(tenant_id, event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_correlation
    ON event_log(tenant_id, correlation_id);

CREATE TABLE IF NOT EXISTS knowledge_repository (
    knowledge_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
    knowledge_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    subject VARCHAR(255) NOT NULL,
    pattern JSONB NOT NULL,
    confidence NUMERIC(3,2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    support_count INT NOT NULL DEFAULT 1 CHECK (support_count >= 1),
    freshness_score NUMERIC(3,2) NOT NULL DEFAULT 1.0 CHECK (freshness_score >= 0 AND freshness_score <= 1),
    governance_level INT NOT NULL DEFAULT 1 CHECK (governance_level >= 0 AND governance_level <= 4),
    version INT NOT NULL DEFAULT 1 CHECK (version >= 1),
    lineage JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, subject)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_tenant_subject
    ON knowledge_repository(tenant_id, subject);

CREATE TABLE IF NOT EXISTS idempotency_ledger (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
    authorization_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE event_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_repository ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_ledger ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy_events ON event_log;
CREATE POLICY tenant_isolation_policy_events ON event_log
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    );

DROP POLICY IF EXISTS tenant_isolation_policy_knowledge ON knowledge_repository;
CREATE POLICY tenant_isolation_policy_knowledge ON knowledge_repository
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    );

DROP POLICY IF EXISTS tenant_isolation_policy_idempotency ON idempotency_ledger;
CREATE POLICY tenant_isolation_policy_idempotency ON idempotency_ledger
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::UUID
    );
