from pathlib import Path
import time
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def build_player_match_gold(con):
    lineups_silver = SILVER_DIR / "lineups_silver.parquet"
    events_silver = SILVER_DIR / "events_silver.parquet"
    shots_silver = SILVER_DIR / "shots_silver.parquet"
    output_path = GOLD_DIR / "player_match_gold.parquet"

    print("[INFO] Building player_match_gold...")
    start_time = time.time()

    query = f"""
    COPY (
        WITH spine AS (
            SELECT 
                round,
                match_id,
                team_side,
                team_name,
                player_id,
                player_name,
                position,
                is_starter,
                minutes_played
            FROM read_parquet('{lineups_silver}')
        ),
        
        eventos_partido AS (
            SELECT 
                match_id,
                player_id,

                -- Progresión territorial y riesgo
                ROUND(SUM(xt_delta), 4) AS xt_total,
                ROUND(SUM(CASE WHEN event_category = 'passes' THEN xt_delta ELSE 0.0 END), 4) AS xt_passes,
                ROUND(SUM(CASE WHEN eventActionType = 'ball-carry' THEN xt_delta ELSE 0.0 END), 4) AS xt_carries,
                ROUND(SUM(xt_turnover_risk), 4) AS xt_turnover_risk,

                -- Distribución
                COUNT(*) FILTER (WHERE event_category = 'passes') AS passes_total,
                COUNT(*) FILTER (WHERE event_category = 'passes' AND outcome = true) AS passes_completed,
                COUNT(*) FILTER (WHERE eventActionType = 'cross') AS crosses_total,
                COUNT(*) FILTER (WHERE eventActionType = 'cross' AND outcome = true) AS crosses_completed,

                -- Creación y verticalidad
                COUNT(*) FILTER (WHERE keypass = true) AS key_passes,
                COUNT(*) FILTER (WHERE isAssist = true) AS assists_total,
                COUNT(*) FILTER (WHERE event_category = 'passes' AND progression_x_m >= 9.14) AS progressive_passes,
                COUNT(*) FILTER (WHERE eventActionType = 'ball-carry' AND progression_x_m >= 9.14) AS progressive_carries,

                -- Desborde individual
                COUNT(*) FILTER (WHERE eventActionType = 'dribble') AS dribbles_total,
                COUNT(*) FILTER (WHERE eventActionType = 'dribble' AND outcome = true) AS dribbles_completed,

                -- Fase defensiva
                COUNT(*) FILTER (WHERE eventActionType = 'ball-recovery') AS recoveries,
                COUNT(*) FILTER (WHERE eventActionType = 'interception') AS interceptions,
                COUNT(*) FILTER (WHERE eventActionType = 'tackle') AS tackles_total,
                COUNT(*) FILTER (WHERE eventActionType = 'tackle' AND outcome = true) AS tackles_won,
                COUNT(*) FILTER (WHERE eventActionType = 'clearance') AS clearances,
                COUNT(*) FILTER (WHERE eventActionType = 'block') AS blocks

            FROM read_parquet('{events_silver}')
            GROUP BY match_id, player_id
        ),
        
        xg_partido AS (
            SELECT 
                match_id,
                player_id,
                COUNT(*) AS shots_total,
                COUNT(*) FILTER (WHERE shot_type = 'goal') AS goals_total,
                ROUND(SUM(xg), 4) AS xg_total,
                ROUND(SUM(xgot), 4) AS xgot_total,
                ROUND(SUM(CASE WHEN shot_type != 'block' THEN (xgot - xg) ELSE 0.0 END), 4) AS sav_total
            FROM read_parquet('{shots_silver}')
            GROUP BY match_id, player_id
        )

        SELECT 
            s.round,
            s.match_id,
            s.team_name,
            s.team_side,
            s.player_id,
            s.player_name,
            s.position,
            s.is_starter,
            s.minutes_played,
            
            -- Progresión territorial
            COALESCE(e.xt_total, 0.0) AS xt_total,
            COALESCE(e.xt_passes, 0.0) AS xt_passes,
            COALESCE(e.xt_carries, 0.0) AS xt_carries,
            COALESCE(e.xt_turnover_risk, 0.0) AS xt_turnover_risk,

            -- Distribución
            COALESCE(e.passes_total, 0) AS passes_total,
            COALESCE(e.passes_completed, 0) AS passes_completed,
            COALESCE(e.crosses_total, 0) AS crosses_total,
            COALESCE(e.crosses_completed, 0) AS crosses_completed,

            -- Creación
            COALESCE(e.key_passes, 0) AS key_passes,
            COALESCE(e.assists_total, 0) AS assists_total,
            COALESCE(e.progressive_passes, 0) AS progressive_passes,
            COALESCE(e.progressive_carries, 0) AS progressive_carries,

            -- Desborde
            COALESCE(e.dribbles_total, 0) AS dribbles_total,
            COALESCE(e.dribbles_completed, 0) AS dribbles_completed,

            -- Fase defensiva
            COALESCE(e.recoveries, 0) AS recoveries,
            COALESCE(e.interceptions, 0) AS interceptions,
            COALESCE(e.tackles_total, 0) AS tackles_total,
            COALESCE(e.tackles_won, 0) AS tackles_won,
            COALESCE(e.clearances, 0) AS clearances,
            COALESCE(e.blocks, 0) AS blocks,

            -- Finalización
            COALESCE(x.shots_total, 0) AS shots_total,
            COALESCE(x.goals_total, 0) AS goals_total,
            COALESCE(x.xg_total, 0.0) AS xg_total,
            COALESCE(x.xgot_total, 0.0) AS xgot_total,
            COALESCE(x.sav_total, 0.0) AS sav_total

        FROM spine s
        LEFT JOIN eventos_partido e 
            ON s.match_id = e.match_id AND s.player_id = e.player_id
        LEFT JOIN xg_partido x 
            ON s.match_id = x.match_id AND s.player_id = x.player_id
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    con.execute(query)
    print(f"[INFO] player_match_gold written in {time.time() - start_time:.2f}s.")


def build_team_match_gold(con):
    player_match_gold = GOLD_DIR / "player_match_gold.parquet"
    output_path = GOLD_DIR / "team_match_gold.parquet"

    print("[INFO] Building team_match_gold...")
    start_time = time.time()

    query = f"""
    COPY (
        WITH team_raw AS (
            SELECT
                round,
                match_id,
                team_name,
                team_side,
                
                -- Marcador y remate
                SUM(goals_total) AS goals_for,
                ROUND(SUM(xg_total), 4) AS xg_for,
                ROUND(SUM(xgot_total), 4) AS xgot_for,
                SUM(shots_total) AS shots_for,
                ROUND(SUM(sav_total), 4) AS sav_total,
                
                -- Progresión territorial y riesgo
                ROUND(SUM(xt_total), 4) AS xt_total,
                ROUND(SUM(xt_passes), 4) AS xt_passes,
                ROUND(SUM(xt_carries), 4) AS xt_carries,
                ROUND(SUM(xt_turnover_risk), 4) AS xt_turnover_risk,
                
                -- Circulación y creación
                SUM(passes_total) AS passes_total,
                SUM(passes_completed) AS passes_completed,
                SUM(progressive_passes) AS prog_passes_total,
                SUM(progressive_carries) AS prog_carries_total,
                SUM(crosses_total) AS crosses_total,
                SUM(crosses_completed) AS crosses_completed,
                SUM(key_passes) AS key_passes_total,
                SUM(assists_total) AS assists_total,
                
                -- Desborde
                SUM(dribbles_total) AS dribbles_total,
                SUM(dribbles_completed) AS dribbles_completed,
                
                -- Fase defensiva colectiva
                SUM(recoveries) AS recoveries_total,
                SUM(interceptions) AS interceptions_total,
                SUM(tackles_total) AS tackles_total,
                SUM(tackles_won) AS tackles_won_total,
                SUM(clearances) AS clearances_total,
                SUM(blocks) AS blocks_total
            FROM read_parquet('{player_match_gold}')
            GROUP BY round, match_id, team_name, team_side
        )

        SELECT 
            t.round,
            t.match_id,
            t.team_name,
            t.team_side,
            opp.team_name AS opponent_name,
            
            -- Resultado
            t.goals_for,
            opp.goals_for AS goals_against,
            CASE 
                WHEN t.goals_for > opp.goals_for THEN 'W'
                WHEN t.goals_for < opp.goals_for THEN 'L'
                ELSE 'D'
            END AS match_result,
            
            -- Métricas esperadas
            t.xg_for,
            opp.xg_for AS xg_against,
            ROUND(t.xg_for - opp.xg_for, 4) AS xg_diff,
            t.xgot_for,
            opp.xgot_for AS xgot_against,
            t.shots_for,
            opp.shots_for AS shots_against,
            t.sav_total,
            
            -- Territorio y contención
            t.xt_total,
            t.xt_passes,
            t.xt_carries,
            t.xt_turnover_risk,
            opp.xt_total AS xt_conceded,
            
            -- Circulación y posesión estimada
            t.passes_total,
            t.passes_completed,
            ROUND((t.passes_completed * 100.0) / NULLIF(t.passes_total, 0), 1) AS pass_accuracy_pct,
            ROUND((t.passes_completed * 100.0) / NULLIF(t.passes_completed + opp.passes_completed, 0), 1) AS possession_proxy_pct,
            
            -- Progresión y creación
            t.prog_passes_total,
            t.prog_carries_total,
            t.crosses_total,
            t.crosses_completed,
            t.key_passes_total,
            t.assists_total,
            t.dribbles_total,
            t.dribbles_completed,
            
            -- Presión colectiva
            t.recoveries_total,
            t.interceptions_total,
            t.tackles_total,
            t.tackles_won_total,
            t.clearances_total,
            t.blocks_total

        FROM team_raw t
        LEFT JOIN team_raw opp 
            ON t.match_id = opp.match_id AND t.team_side != opp.team_side
        ORDER BY t.round, t.match_id, t.team_side DESC
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    con.execute(query)
    print(f"[INFO] team_match_gold written in {time.time() - start_time:.2f}s.")


def build_master_player_gold(con):
    player_match_gold = GOLD_DIR / "player_match_gold.parquet"
    output_path = GOLD_DIR / "master_player_gold.parquet"

    print("[INFO] Building master_player_gold (Position-specific + TOTAL)...")
    start_time = time.time()

    query = f"""
    COPY (
        WITH raw_by_position AS (
            SELECT 
                player_id,
                LAST(player_name) AS player_name,
                ARG_MAX(team_name, round) AS team_name,
                position,
                
                -- Participación
                SUM(minutes_played) AS total_minutes,
                COUNT(DISTINCT match_id) AS matches_played,
                COUNT(*) FILTER (WHERE is_starter = true) AS matches_started,
                COUNT(*) FILTER (WHERE is_starter = false AND minutes_played > 0) AS sub_appearances,
                ROUND(AVG(minutes_played), 1) AS avg_minutes_per_match,
                
                -- Progresión territorial y riesgo
                ROUND(SUM(xt_total), 4) AS xt_total,
                ROUND(SUM(xt_passes), 4) AS xt_passes,
                ROUND(SUM(xt_carries), 4) AS xt_carries,
                ROUND(SUM(xt_turnover_risk), 4) AS xt_turnover_risk_total,
                SUM(progressive_passes) AS prog_passes_total,
                SUM(progressive_carries) AS prog_carries_total,
                
                -- Circulación y creación
                SUM(passes_total) AS passes_total,
                SUM(passes_completed) AS passes_completed,
                SUM(crosses_total) AS crosses_total,
                SUM(crosses_completed) AS crosses_completed,
                SUM(key_passes) AS key_passes_total,
                SUM(assists_total) AS assists_total,
                
                -- Desborde
                SUM(dribbles_total) AS dribbles_total,
                SUM(dribbles_completed) AS dribbles_completed,
                
                -- Fase defensiva
                SUM(recoveries) AS recoveries_total,
                SUM(interceptions) AS interceptions_total,
                SUM(tackles_total) AS tackles_total,
                SUM(tackles_won) AS tackles_won_total,
                SUM(clearances) AS clearances_total,
                SUM(blocks) AS blocks_total,
                
                -- Finalización
                SUM(shots_total) AS shots_total,
                SUM(goals_total) AS goals_total,
                ROUND(SUM(xg_total), 4) AS xg_total,
                ROUND(SUM(xgot_total), 4) AS xgot_total,
                ROUND(SUM(sav_total), 4) AS sav_total
                
            FROM read_parquet('{player_match_gold}')
            GROUP BY player_id, position
        ),

        raw_total AS (
            SELECT 
                player_id,
                LAST(player_name) AS player_name,
                ARG_MAX(team_name, round) AS team_name,
                'TOTAL' AS position,
                
                SUM(minutes_played) AS total_minutes,
                COUNT(DISTINCT match_id) AS matches_played,
                COUNT(*) FILTER (WHERE is_starter = true) AS matches_started,
                COUNT(*) FILTER (WHERE is_starter = false AND minutes_played > 0) AS sub_appearances,
                ROUND(AVG(minutes_played), 1) AS avg_minutes_per_match,
                
                ROUND(SUM(xt_total), 4) AS xt_total,
                ROUND(SUM(xt_passes), 4) AS xt_passes,
                ROUND(SUM(xt_carries), 4) AS xt_carries,
                ROUND(SUM(xt_turnover_risk), 4) AS xt_turnover_risk_total,
                SUM(progressive_passes) AS prog_passes_total,
                SUM(progressive_carries) AS prog_carries_total,
                
                SUM(passes_total) AS passes_total,
                SUM(passes_completed) AS passes_completed,
                SUM(crosses_total) AS crosses_total,
                SUM(crosses_completed) AS crosses_completed,
                SUM(key_passes) AS key_passes_total,
                SUM(assists_total) AS assists_total,
                
                SUM(dribbles_total) AS dribbles_total,
                SUM(dribbles_completed) AS dribbles_completed,
                
                SUM(recoveries) AS recoveries_total,
                SUM(interceptions) AS interceptions_total,
                SUM(tackles_total) AS tackles_total,
                SUM(tackles_won) AS tackles_won_total,
                SUM(clearances) AS clearances_total,
                SUM(blocks) AS blocks_total,
                
                SUM(shots_total) AS shots_total,
                SUM(goals_total) AS goals_total,
                ROUND(SUM(xg_total), 4) AS xg_total,
                ROUND(SUM(xgot_total), 4) AS xgot_total,
                ROUND(SUM(sav_total), 4) AS sav_total
                
            FROM read_parquet('{player_match_gold}')
            GROUP BY player_id
        ),

        unified_raw AS (
            SELECT * FROM raw_by_position
            UNION ALL
            SELECT * FROM raw_total
        )

        SELECT 
            *,
            -- Eficacia y toma de decisiones
            ROUND((passes_completed * 100.0) / NULLIF(passes_total, 0), 1) AS pass_accuracy_pct,
            ROUND((crosses_completed * 100.0) / NULLIF(crosses_total, 0), 1) AS cross_accuracy_pct,
            ROUND((dribbles_completed * 100.0) / NULLIF(dribbles_total, 0), 1) AS dribble_success_pct,
            ROUND((tackles_won_total * 100.0) / NULLIF(tackles_total, 0), 1) AS tackle_success_pct,
            ROUND(sav_total / NULLIF(shots_total, 0), 4) AS sav_per_shot,
            ROUND(goals_total - xg_total, 2) AS finishing_skill_total,
            ROUND(xt_total / NULLIF(xt_turnover_risk_total, 0), 2) AS risk_reward_ratio,
            
            -- Normalizaciones P90 protegidas
            ROUND((xt_total / NULLIF(total_minutes, 0)) * 90.0, 4) AS xt_p90,
            ROUND((xt_passes / NULLIF(total_minutes, 0)) * 90.0, 4) AS xt_passes_p90,
            ROUND((xt_carries / NULLIF(total_minutes, 0)) * 90.0, 4) AS xt_carries_p90,
            ROUND((xt_turnover_risk_total / NULLIF(total_minutes, 0)) * 90.0, 4) AS xt_turnover_risk_p90,
            ROUND((prog_passes_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS prog_passes_p90,
            ROUND((prog_carries_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS prog_carries_p90,
            ROUND((passes_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS passes_p90,
            ROUND((key_passes_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS key_passes_p90,
            ROUND((assists_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS assists_p90,
            ROUND((dribbles_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS dribbles_p90,
            ROUND((recoveries_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS recoveries_p90,
            ROUND((interceptions_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS interceptions_p90,
            ROUND((tackles_won_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS tackles_won_p90,
            ROUND((clearances_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS clearances_p90,
            ROUND((shots_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS shots_p90,
            ROUND((xg_total / NULLIF(total_minutes, 0)) * 90.0, 4) AS xg_p90,
            ROUND((goals_total * 1.0 / NULLIF(total_minutes, 0)) * 90.0, 2) AS goals_p90

        FROM unified_raw
        ORDER BY player_id, position
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    con.execute(query)
    print(f"[INFO] master_player_gold written in {time.time() - start_time:.2f}s.")


def build_scouting_percentiles_gold(con):
    master_player_gold = GOLD_DIR / "master_player_gold.parquet"
    output_path = GOLD_DIR / "scouting_percentiles_gold.parquet"

    print("[INFO] Building scouting_percentiles_gold...")
    start_time = time.time()

    query = f"""
    COPY (
        WITH filtrados AS (
            SELECT *
            FROM read_parquet('{master_player_gold}')
            WHERE total_minutes >= 180 
              AND position NOT IN ('G', 'TOTAL')
        ),
        
        percentiles_generales AS (
            SELECT 
                *,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY xt_p90) * 100.0, 1) AS pct_xt,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY prog_passes_p90) * 100.0, 1) AS pct_prog_passes,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY passes_p90) * 100.0, 1) AS pct_passes_vol,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY pass_accuracy_pct) * 100.0, 1) AS pct_pass_acc,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY key_passes_p90) * 100.0, 1) AS pct_key_passes,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY dribbles_p90) * 100.0, 1) AS pct_dribbles,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY recoveries_p90) * 100.0, 1) AS pct_recoveries,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY interceptions_p90) * 100.0, 1) AS pct_interceptions,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY tackles_won_p90) * 100.0, 1) AS pct_tackles_won,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY clearances_p90) * 100.0, 1) AS pct_clearances,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY shots_p90) * 100.0, 1) AS pct_shots,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY xg_p90) * 100.0, 1) AS pct_xg,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY COALESCE(risk_reward_ratio, 0.0)) * 100.0, 1) AS pct_decision_making
            FROM filtrados
        ),
        
        percentiles_tiro AS (
            SELECT 
                player_id,
                position,
                ROUND(PERCENT_RANK() OVER (PARTITION BY position ORDER BY sav_per_shot) * 100.0, 1) AS pct_sav_quality
            FROM filtrados
            WHERE shots_total > 0
        )

        SELECT 
            g.*,
            t.pct_sav_quality
        FROM percentiles_generales g
        LEFT JOIN percentiles_tiro t 
            ON g.player_id = t.player_id AND g.position = t.position
        ORDER BY g.position, g.pct_xt DESC
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    con.execute(query)
    print(f"[INFO] scouting_percentiles_gold written in {time.time() - start_time:.2f}s.")


def main():
    con = duckdb.connect()
    build_player_match_gold(con)
    build_team_match_gold(con)
    build_master_player_gold(con)
    build_scouting_percentiles_gold(con)
    con.close()
    print("[INFO] Gold pipeline completed successfully.")


if __name__ == "__main__":
    main()