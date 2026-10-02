import duckdb
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def process_lineups(con):
    
    lineups_bronze = BRONZE_DIR / "lineups.parquet"
    calendar_bronze = BRONZE_DIR / "calendario_challenger.parquet"
    output_silver = SILVER_DIR / "lineups_silver.parquet"

    query_lineups = f"""
        COPY (
            SELECT 
                CAST(l.round AS INT) AS round,
                CAST(l.match_id AS BIGINT) AS match_id,
                CAST(l.player_id AS BIGINT) AS player_id,
                CAST(l.player_name AS VARCHAR) AS player_name,
                CAST(l.team_side AS VARCHAR) AS team_side,
                CAST(
                    CASE 
                        WHEN l.team_side = 'home' THEN m.home_team
                        WHEN l.team_side = 'away' THEN m.away_team
                        ELSE 'unknown'
                    END AS VARCHAR
                ) AS team_name,
                CAST(l.position AS VARCHAR) AS position,
                CAST(l.is_starter AS BOOLEAN) AS is_starter,
                CAST(l.minutes_played AS INT) AS minutes_played
            FROM read_parquet('{lineups_bronze}') AS l
            INNER JOIN read_parquet('{calendar_bronze}') AS m
                ON l.match_id = m.match_id
        ) TO '{output_silver}' (FORMAT PARQUET);
    """

    con.execute(query_lineups)

def process_shots(con):
    shots_bronze = BRONZE_DIR / "tiros_raw"
    output_silver = SILVER_DIR / "shots_silver.parquet"

    query_shots = f"""
        COPY (
            SELECT
                CAST(id AS BIGINT) AS shot_id,
                CAST(match_id AS BIGINT) AS match_id,
                CAST(time AS INT) AS minute,
                CAST("player.id" AS BIGINT) AS player_id,
                CAST("player.name" AS VARCHAR) AS player_name,
                CAST("player.position" AS VARCHAR) AS player_position,
                CAST(isHome AS BOOLEAN) AS is_home,

                CAST(shotType AS VARCHAR) AS shot_type,
                CAST(situation AS VARCHAR) AS situation,
                CAST(bodyPart AS VARCHAR) AS body_part,

                -- Normalización métrica UEFA (105x68m)
                ROUND(CAST((100.0 - "playerCoordinates.x") * 1.05 AS DOUBLE), 3) AS x_m,
                ROUND(CAST("playerCoordinates.y" * 0.68 AS DOUBLE), 3) AS y_m,

                CAST(COALESCE(xg, 0.0) AS DOUBLE) AS xg,
                CAST(COALESCE(xgot, 0.0) AS DOUBLE) AS xgot

            FROM read_parquet('{shots_bronze}/*.parquet', union_by_name=True)
            WHERE id IS NOT NULL 
              AND (goalType != 'own' OR goalType IS NULL)
        ) TO '{output_silver}' (FORMAT PARQUET);
    """
    con.execute(query_shots)

def process_events(con):
    events_bronze = BRONZE_DIR / "eventos_raw"
    lineups_silver = SILVER_DIR / "lineups_silver.parquet"
    calendar_bronze = BRONZE_DIR / "calendario_challenger.parquet"
    output_silver = SILVER_DIR / "events_silver.parquet"

    # 1. Matriz de referencia xT (8 filas x 12 columnas)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW v_xt_matrix AS
        SELECT [
            [0.00638, 0.00779, 0.00844, 0.00927, 0.01126, 0.01248, 0.01473, 0.01853, 0.02412, 0.02755, 0.03467, 0.03792],
            [0.00739, 0.00896, 0.00977, 0.01070, 0.01275, 0.01400, 0.01683, 0.02122, 0.02756, 0.03178, 0.05812, 0.04617],
            [0.00846, 0.01022, 0.01115, 0.01214, 0.01423, 0.01549, 0.01852, 0.02412, 0.03046, 0.03608, 0.09139, 0.06388],
            [0.00958, 0.01132, 0.01228, 0.01322, 0.01511, 0.01633, 0.01948, 0.02534, 0.03185, 0.03819, 0.11186, 0.07436],
            [0.00958, 0.01132, 0.01228, 0.01322, 0.01511, 0.01633, 0.01948, 0.02534, 0.03185, 0.03819, 0.11186, 0.07436],
            [0.00846, 0.01022, 0.01115, 0.01214, 0.01423, 0.01549, 0.01852, 0.02412, 0.03046, 0.03608, 0.09139, 0.06388],
            [0.00739, 0.00896, 0.00977, 0.01070, 0.01275, 0.01400, 0.01683, 0.02122, 0.02756, 0.03178, 0.05812, 0.04617],
            [0.00638, 0.00779, 0.00844, 0.00927, 0.01126, 0.01248, 0.01473, 0.01853, 0.02412, 0.02755, 0.03467, 0.03792]
        ] AS mat;
    """)

    # 2. Cruce dimensional con alineaciones y calendario
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW v_raw_enriched AS
        SELECT 
            CAST(c.round AS INT) AS round,
            c.home_team,
            c.away_team,
            l.team_name,
            l.team_side,
            l.player_name,
            l.position,
            e.*
        FROM read_parquet('{events_bronze}/*.parquet', union_by_name=True) e
        LEFT JOIN read_parquet('{lineups_silver}') l
            ON e.match_id = l.match_id AND e.player_id = l.player_id
        LEFT JOIN read_parquet('{calendar_bronze}') c
            ON e.match_id = c.match_id;
    """)

    # 3. Proyección física a metros UEFA (105x68m) y asignación de cuadrícula (12x8)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW v_spatial AS
        SELECT 
            *,
            ROUND(("playerCoordinates.x" / 100.0) * 105.0, 3) AS x_origen_m,
            ROUND(("playerCoordinates.y" / 100.0) * 68.0, 3) AS y_origen_m,
            ROUND(("passEndCoordinates.x" / 100.0) * 105.0, 3) AS x_destino_m,
            ROUND(("passEndCoordinates.y" / 100.0) * 68.0, 3) AS y_destino_m,

            ROUND((("passEndCoordinates.x" / 100.0) * 105.0) - (("playerCoordinates.x" / 100.0) * 105.0), 3) AS progression_x_m,
            ROUND(SQRT(
                POW((("passEndCoordinates.x" / 100.0) * 105.0) - (("playerCoordinates.x" / 100.0) * 105.0), 2) +
                POW((("passEndCoordinates.y" / 100.0) * 68.0) - (("playerCoordinates.y" / 100.0) * 68.0), 2)
            ), 3) AS distance_m,

            CASE 
                WHEN "playerCoordinates.x" IS NOT NULL 
                THEN LEAST(GREATEST(CAST(FLOOR(("playerCoordinates.x" / 100.0) * 12) AS INT), 0), 11)
                ELSE NULL 
            END AS xt_zona_x_origen,
            CASE 
                WHEN "playerCoordinates.y" IS NOT NULL 
                THEN LEAST(GREATEST(CAST(FLOOR(("playerCoordinates.y" / 100.0) * 8) AS INT), 0), 7)
                ELSE NULL 
            END AS xt_zona_y_origen,
            CASE 
                WHEN "passEndCoordinates.x" IS NOT NULL 
                THEN LEAST(GREATEST(CAST(FLOOR(("passEndCoordinates.x" / 100.0) * 12) AS INT), 0), 11)
                ELSE NULL 
            END AS xt_zona_x_destino,
            CASE 
                WHEN "passEndCoordinates.y" IS NOT NULL 
                THEN LEAST(GREATEST(CAST(FLOOR(("passEndCoordinates.y" / 100.0) * 8) AS INT), 0), 7)
                ELSE NULL 
            END AS xt_zona_y_destino
        FROM v_raw_enriched;
    """)

    # 4. Asignación de valores base de amenaza territorial (xT origen y destino)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW v_xt_mapped AS
        SELECT 
            s.*,
            CASE 
                WHEN s.xt_zona_x_origen IS NOT NULL AND s.xt_zona_y_origen IS NOT NULL
                THEN COALESCE(m.mat[s.xt_zona_y_origen + 1][s.xt_zona_x_origen + 1], 0.0)
                ELSE 0.0 
            END AS xt_origen,

            CASE 
                WHEN s.xt_zona_x_destino IS NOT NULL AND s.xt_zona_y_destino IS NOT NULL
                THEN COALESCE(m.mat[s.xt_zona_y_destino + 1][s.xt_zona_x_destino + 1], 0.0)
                ELSE NULL 
            END AS xt_destino
        FROM v_spatial s
        CROSS JOIN v_xt_matrix m;
    """)

    # 5. Cálculo de métricas analíticas (progreso neto y riesgo de pérdida) y persistencia
    con.execute(f"""
        COPY (
            SELECT 
                *,
                ROUND(
                    CASE 
                        WHEN (event_category = 'passes' AND outcome = true AND xt_destino IS NOT NULL)
                          OR (eventActionType = 'ball-carry' AND xt_destino IS NOT NULL)
                        THEN GREATEST(0.0, xt_destino - xt_origen)
                        ELSE 0.0
                    END, 5
                ) AS xt_delta,

                ROUND(
                    CASE 
                        WHEN (event_category = 'passes' AND COALESCE(outcome, false) = false)
                          OR (eventActionType = 'dribble' AND COALESCE(outcome, false) = false)
                        THEN xt_origen
                        ELSE 0.0
                    END, 5
                ) AS xt_turnover_risk
            FROM v_xt_mapped
        ) TO '{output_silver}' (FORMAT PARQUET);
    """)


def main():
    con = duckdb.connect()

    process_lineups(con)
    process_shots(con)
    process_events(con)

    con.close()

if __name__ == "__main__":
    main()