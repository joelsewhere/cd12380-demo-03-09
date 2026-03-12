-- =============================================================================
-- STEP 1 — Set table naming conventions
-- =============================================================================

{% set staging_suffix = "_staging" %}
{% set backup_suffix  = "_backup"  %}

{% set prod_table    =  params.database ~ "." ~ params.schema ~ "." ~ params.table %}
{% set staging_table =  params.database ~ "." ~ params.schema ~ "." ~ params.table ~ staging_suffix %}
{% set backup_table  =  params.database ~ "." ~ params.schema ~ "." ~ params.table ~ backup_suffix %}

-- =============================================================================
-- STEP 2 — Build the staging table from the query defined in the child file
-- =============================================================================

DROP TABLE IF EXISTS {{ staging_table }}



CREATE TABLE {{ staging_table }} AS (
{% block query %}
    /*
        ----------------------------------------------------------------
        OVERRIDE THIS BLOCK in the extending file, e.g.:

        {% extends "rotate_table.sql" %}
        {% block query %}
        SELECT
            id,
            name,
            updated_at
        FROM source_schema.source_table
        WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
        {% endblock %}
        ----------------------------------------------------------------
    */
{% endblock %};
)

-- =============================================================================
-- STEP 3 — Rotate: drop backup → rename prod → rename staging
-- =============================================================================

DROP TABLE IF EXISTS {{ backup_table }};


ALTER TABLE {{ prod_table }}
    RENAME TO {{ params.table ~ backup_suffix }};


ALTER TABLE {{ staging_table }}
    RENAME TO {{ params.table }};

-- =============================================================================
-- STEP 4 — Post-rotation - Update table statistics for query planner
-- =============================================================================

ANALYZE {{ prod_table }};

-- =============================================================================
-- STEP 5 — Cleanup
-- =============================================================================

DROP TABLE IF EXISTS {{ backup_table }};
