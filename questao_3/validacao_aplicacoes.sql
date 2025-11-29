CREATE OR REPLACE PROCEDURE `elviscsprot-desafio.engenharia_dados.prc_load_validacao_aplicacoes`(
    id_inicio INT64,
    id_fim INT64
)
BEGIN

    CREATE OR REPLACE TABLE `elviscsprot-desafio.engenharia_dados.tb_inconsistencias_aplicacoes` AS

    WITH base_local AS (
        SELECT
            *,
            TO_JSON_STRING(t) AS json_completo
        FROM `elviscsprot-desafio.engenharia_dados.application_record_local` t
        WHERE ID BETWEEN id_inicio AND id_fim
    ),

    base_gcp AS (
        SELECT
            *,
            TO_JSON_STRING(t) AS json_completo
        FROM `elviscsprot-desafio.engenharia_dados.application_record_gcp` t
        WHERE ID BETWEEN id_inicio AND id_fim
    ),
    
    ausentes_gcp AS (
        SELECT
            'AUSENTE_GCP' AS tipo_inconsistencia,
            l.id AS id_registro,
            CONCAT('Registro ID ', CAST(l.ID AS STRING), ' existe no local mas não foi encontrado no GCP') AS descricao,
            l.json_completo AS dados_local,
            NULL AS dados_gcp,
            CURRENT_DATE() AS data_validacao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM base_local l
        LEFT JOIN base_gcp g ON l.id = g.id
        WHERE g.id IS NULL
    ),
    
    ausentes_local AS (
        SELECT
            'AUSENTE_LOCAL' AS tipo_inconsistencia,
            g.id AS id_registro,
            CONCAT('Registro ID ', CAST(g.ID AS STRING), ' existe no GCP mas não foi encontrado no local') AS descricao,
            NULL AS dados_local,
            g.json_completo AS dados_gcp,
            CURRENT_DATE() AS data_validacao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM base_gcp g
        LEFT JOIN base_local l ON g.id = l.id
        WHERE l.id IS NULL
    ),
    
    divergencias AS (
        SELECT
            'DIVERGENCIA_VALORES' AS tipo_inconsistencia,
            l.id AS id_registro,
            CONCAT('Registro ID ', CAST(l.ID AS STRING), ' possui valores divergentes entre local e GCP') AS descricao,
            l.json_completo AS dados_local,
            g.json_completo AS dados_gcp,
            CURRENT_DATE() AS data_validacao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM base_local l
        INNER JOIN base_gcp g ON l.id = g.id
        WHERE l.json_completo != g.json_completo
    )
    
    SELECT * FROM ausentes_gcp
    UNION ALL
    SELECT * FROM ausentes_local
    UNION ALL
    SELECT * FROM divergencias;
    
    SELECT 
        tipo_inconsistencia,
        COUNT(*) AS total_inconsistencias
    FROM `elviscsprot-desafio.engenharia_dados.tb_inconsistencias_aplicacoes`
    GROUP BY tipo_inconsistencia
    ORDER BY total_inconsistencias DESC;

END;

