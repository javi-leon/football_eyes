import time
import random
import pandas as pd
from curl_cffi import requests
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def extract_calendar():
    league_id = 9
    season_id = 96912
    calendar_path = BRONZE_DIR / "calendario_challenger.parquet"

    existing_df = (
        pd.read_parquet(calendar_path) if calendar_path.exists() else pd.DataFrame()
    )

    if not existing_df.empty:
        pending_matches = existing_df[existing_df["status"] != "Ended"]
        if pending_matches.empty:
            print("[INFO] Calendar already up to date. All matches marked as 'Ended'.")
            return
        start_round = int(pending_matches["round"].min())
        print(f"[INFO] Resuming calendar extraction from round {start_round}...")
    else:
        start_round = 1
        print("[INFO] Starting full calendar extraction from round 1...")

    updated_records = []
    current_round = start_round
    now_ts = time.time()

    while True:
        url = f"https://www.sofascore.com/api/v1/unique-tournament/{league_id}/season/{season_id}/events/round/{current_round}"

        try:
            response = requests.get(url, impersonate="chrome")

            if response.status_code != 200:
                print(
                    f"[INFO] Round {current_round} returned status {response.status_code}. Stopping calendar scan."
                )
                break

            data = response.json()
            events = data.get("events", [])

            if not events:
                print(
                    f"[INFO] No events found in round {current_round}. Stopping calendar scan."
                )
                break

            print(
                f"[INFO] Round {current_round}: {len(events)} matches retrieved."
            )

            for event in events:
                updated_records.append(
                    {
                        "match_id": event.get("id"),
                        "home_team": event.get("homeTeam", {}).get("name"),
                        "away_team": event.get("awayTeam", {}).get("name"),
                        "round": current_round,
                        "status": event.get("status", {}).get("description"),
                        "start_timestamp": event.get("startTimestamp"),
                    }
                )

            all_future_unstarted = all(
                ev.get("startTimestamp", float("inf")) > now_ts
                and ev.get("status", {}).get("description") == "Not started"
                for ev in events
            )
            if all_future_unstarted and current_round > start_round:
                print(
                    f"[INFO] Round {current_round} contains only future fixtures. Terminating scan."
                )
                break

            current_round += 1
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as exc:
            print(
                f"[ERROR] Failed fetching calendar for round {current_round}: {exc}"
            )
            break

    if updated_records:
        new_df = pd.DataFrame(updated_records)

        if not existing_df.empty:
            updated_ids = set(new_df["match_id"])
            retained_df = existing_df[~existing_df["match_id"].isin(updated_ids)]
            final_df = pd.concat([retained_df, new_df], ignore_index=True)
        else:
            final_df = new_df

        final_df = final_df.sort_values(by=["round", "match_id"]).reset_index(
            drop=True
        )
        final_df.to_parquet(calendar_path, index=False)
        print(f"[INFO] Calendar persisted. Total matches: {len(final_df)}.")

def extract_lineups():
    ruta_lineups = BRONZE_DIR / "lineups.parquet"
    lineups_existentes = pd.read_parquet(ruta_lineups) if ruta_lineups.exists() else pd.DataFrame()
    downloaded_matches = set(lineups_existentes['match_id'].unique()) if not lineups_existentes.empty else set()

    calendario = pd.read_parquet(BRONZE_DIR / "calendario_challenger.parquet")
    
    # Todos los partidos finalizados que aún no hemos descargado
    pendientes = calendario[
        (calendario['status'] == 'Ended') & 
        (~calendario['match_id'].isin(downloaded_matches))
    ]

    print(f"Partidos pendientes de alineaciones: {len(pendientes)}")
    lineups_nuevos = []

    for _, row in pendientes.iterrows():
        match_id = row['match_id']
        jornada = row['round']
        url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"

        respuesta = requests.get(url, impersonate="chrome")
        if respuesta.status_code == 200:
            datos_lineups = respuesta.json()
            for equipo in ['home', 'away']:
                lista_jugadores = datos_lineups.get(equipo, {}).get('players', [])
                for j in lista_jugadores:
                    lineups_nuevos.append({
                        "round": jornada,
                        "match_id": match_id,
                        "team_side": equipo,
                        "player_id": j['player']['id'],
                        "player_name": j['player']['name'],
                        "position": j.get('position', 'N/A'),
                        "is_starter": not j.get('substitute', False),
                        "minutes_played": (j.get('statistics') or {}).get('minutesPlayed', 0)
                    })
            print(f"✅ Alineaciones extraídas para el partido {match_id}")
        else:
            print(f"❌ Error al obtener lineups del partido {match_id}. Estado: {respuesta.status_code}")

        time.sleep(random.uniform(1.0, 2.5))

    if lineups_nuevos:
        df_nuevos = pd.DataFrame(lineups_nuevos)
        df_final = pd.concat([lineups_existentes, df_nuevos], ignore_index=True) if not lineups_existentes.empty else df_nuevos
        df_final.to_parquet(ruta_lineups, index=False)

def extract_events():
    carpeta_eventos = BRONZE_DIR / "eventos_raw"
    carpeta_eventos.mkdir(parents=True, exist_ok=True)

    # 1. Recuperamos tanto los .parquet procesados como las marcas .empty
    procesados = set()
    for f in os.listdir(carpeta_eventos):
        if f.startswith("match_") and (f.endswith(".parquet") or f.endswith(".empty")):
            partes = f.split("_")
            if len(partes) >= 4:
                try:
                    match_id = int(partes[1])
                    player_id = int(partes[3].split(".")[0])
                    procesados.add((match_id, player_id))
                except ValueError:
                    continue

    lineups = pd.read_parquet(BRONZE_DIR / "lineups.parquet")
    lineups_activos = lineups[lineups['minutes_played'] > 0]

    # 2. Filtrado vectorial instantáneo (O(1)) de los que ya están en disco
    lineups_activos = lineups_activos[
        ~lineups_activos.apply(lambda r: (r['match_id'], r['player_id']) in procesados, axis=1)
    ]

    print(f"Total de jugadores a extraer eventos: {len(lineups_activos)}")

    for _, row in lineups_activos.iterrows():
        match_id = row['match_id']
        player_id = row['player_id']

        ruta_archivo = carpeta_eventos / f"match_{match_id}_player_{player_id}.parquet"
        ruta_skip = carpeta_eventos / f"match_{match_id}_player_{player_id}.empty"

        if ruta_archivo.exists() or ruta_skip.exists():
            continue

        url = f"https://www.sofascore.com/api/v1/event/{match_id}/player/{player_id}/rating-breakdown"

        try:
            respuesta = requests.get(url, impersonate="chrome")

            if respuesta.status_code == 200:
                datos = respuesta.json()
                dataframes_temporales = []

                for tipo_evento, contenido in datos.items():
                    if isinstance(contenido, list) and len(contenido) > 0:
                        df_temp = pd.json_normalize(contenido)
                        df_temp['event_category'] = tipo_evento
                        dataframes_temporales.append(df_temp)

                if dataframes_temporales:
                    df_eventos_jugador = pd.concat(dataframes_temporales, ignore_index=True)
                    df_eventos_jugador['match_id'] = match_id
                    df_eventos_jugador['player_id'] = player_id
                    df_eventos_jugador.to_parquet(ruta_archivo, index=False)
                    print(f"✅ Partido {match_id} | Jugador {player_id}: {len(df_eventos_jugador)} eventos guardados.")
                else:
                    # Jugador con minutos pero sin acciones registradas
                    ruta_skip.touch()
                    print(f"⚠️ Partido {match_id} | Jugador {player_id}: Sin eventos registrados.")

            elif respuesta.status_code == 404:
                ruta_skip.touch()
                print(f"⚠️ Partido {match_id} | Jugador {player_id}: Desglose no disponible (404).")
            else:
                print(f"❌ Error {respuesta.status_code} en Partido {match_id} | Jugador {player_id}")

        except Exception as e:
            print(f"❌ Fallo de conexión en Partido {match_id} | Jugador {player_id}: {e}")

        time.sleep(random.uniform(1.0, 2.0))

    print("\nExtracción de eventos completada.")

def extract_shots():
    

    carpeta_tiros = BRONZE_DIR / "tiros_raw"
    os.makedirs(carpeta_tiros, exist_ok=True)

    procesados = set()
    for f in os.listdir(carpeta_tiros):
        if f.startswith("match_"):
            # match_123_shots.parquet -> 123 | match_123.empty -> 123
            try:
                m_id = int(f.split("_")[1].split(".")[0])
                procesados.add(m_id)
            except ValueError:
                continue

    lineups = pd.read_parquet(BRONZE_DIR / "lineups.parquet")
    #Solo mantener los partidos que no tienen un archivo de tiros ya extraído
    match_ids = [m for m in lineups['match_id'].unique() if m not in procesados]

    print(f"Total de partidos a extraer: {len(match_ids)}")

    for match_id in match_ids:
        
        ruta_archivo = carpeta_tiros/f"match_{match_id}_shots.parquet"
        ruta_skip = carpeta_tiros / f"match_{match_id}.empty"
        
        if ruta_archivo.exists() or ruta_skip.exists():
            continue
            
        url = f"https://www.sofascore.com/api/v1/event/{match_id}/shotmap"
        
        try:
            respuesta = requests.get(url, impersonate="chrome")
            
            if respuesta.status_code == 200:
                datos = respuesta.json()
                shots = datos.get('shotmap', [])
                
                if len(shots) > 0:
                    df_temp = pd.json_normalize(shots)
                    cols_basura = [c for c in df_temp.columns if 'fieldTranslations' in c or 'slug' in c or 'userCount' in c]
                    df_temp = df_temp.drop(columns=cols_basura)
                    df_temp['match_id'] = match_id
                    df_temp.to_parquet(ruta_archivo, index=False)
                    print(f"✅ Partido {match_id}: {len(df_temp)} disparos guardados.")
                else:
                    ruta_skip.touch()
                    # Si el partido terminó 0-0 sin tiros o sin tracking
                    
            elif respuesta.status_code == 404:
                ruta_skip.touch()
                # Partido sin cobertura de shotmap
            else:
                print(f"❌ Error {respuesta.status_code} en Partido {match_id}")
                
        except Exception as e:
            print(f"❌ Fallo de conexión en Partido {match_id}: {e}")
            
        time.sleep(random.uniform(1.2, 2.0))

    print("\nExtracción completada.")

def main():
    extract_calendar()
    extract_lineups()
    extract_events()
    extract_shots()

if __name__ == "__main__":
    main()