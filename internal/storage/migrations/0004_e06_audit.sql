CREATE TABLE IF NOT EXISTS storage_download_authorizations (
 event_id uuid PRIMARY KEY,
 operation_id uuid NOT NULL UNIQUE,
 object_id text NOT NULL,
 object_version text NOT NULL,
 organization_id text NOT NULL,
 workspace_id text NOT NULL,
 project_id text NOT NULL,
 actor_subject text NOT NULL,
 actor_domain text NOT NULL,
 authority_kind text NOT NULL,
 authority_id text NOT NULL,
 authority_version bigint NOT NULL CHECK(authority_version>0),
 authorized_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS storage_download_authorizations_retention ON storage_download_authorizations(authorized_at);
