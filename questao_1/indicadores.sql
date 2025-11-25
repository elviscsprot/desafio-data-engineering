CREATE TABLE IF NOT EXISTS indicadores (
    nm_indicador TEXT PRIMARY KEY,
    vlr_indicador REAL
);

DELETE FROM indicadores;

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT 'percentual_ti_menos_40', 
       ROUND(
           CAST(COUNT(CASE WHEN u.idade < 40 THEN 1 END) AS REAL) / 
           COUNT(*) * 100, 
           2
       )
FROM usuarios u
INNER JOIN empresas e ON u.id = e.usuario_id
WHERE LOWER(e.departamento) LIKE '%tech%' 
   OR LOWER(e.departamento) LIKE '%it%'
   OR LOWER(e.departamento) LIKE '%engineering%';

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT 'media_idade', ROUND(AVG(idade), 2)
FROM usuarios;

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT 'total_departamentos_unicos', COUNT(DISTINCT departamento)
FROM empresas;

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT 'percentual_mulheres', 
       ROUND(
           CAST(COUNT(CASE WHEN LOWER(genero) = 'female' THEN 1 END) AS REAL) / 
           COUNT(*) * 100, 
           2
       )
FROM usuarios;

SELECT * FROM indicadores ORDER BY nm_indicador;

