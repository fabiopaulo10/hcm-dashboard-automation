"""
GitHub Actions — HCM Dashboard incremental update
Roda na nuvem, sem notebook. Seg-Sex conforme cron do workflow.
"""
import json, re, os, sys, datetime, base64, requests, subprocess, shutil, tempfile

# Tokens — o workflow passa GRID_API_TOKEN; GRID_TOKEN é fallback local
GRID_TOKEN = os.environ.get("GRID_TOKEN") or os.environ.get("GRID_API_TOKEN", "")
DOC_ID     = "01KSRJ5STC80GGR6CKPB24MH9W"

NUMERIC = [
    "quantidade_hc_solicitado_workable","quantidade_hc_solicitado_total",
    "quantidade_hc_rosterizado_workable","quantidade_hc_rosterizado_total",
    "reps_presentes_con_turno_con_provider","reps_presentes_con_turno_sin_provider",
    "reps_presentes_sin_turno",
]

# ── 1. Histórico — lê data.json do repo local (checkout já fez clone) ─────────
def load_hist():
    # Procura data_{d}.json do mais recente para o mais antigo
    today = datetime.date.today()
    for i in range(1, 8):
        d = (today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        path = f"data_{d}.json"
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            rows = data.get("rows", data) if isinstance(data, dict) else data
            if rows:
                de = data.get("data_end") if isinstance(data, dict) else None
                de = de or max(r["fecha"] for r in rows)
                print(f"[hist] {path} — {len(rows)} linhas, data_end={de}")
                return rows, de
    # fallback: data.json
    if os.path.exists("data.json"):
        with open("data.json", encoding="utf-8") as f:
            data = json.load(f)
        rows = data.get("rows", data) if isinstance(data, dict) else data
        if isinstance(rows, list) and rows:
            de = data.get("data_end") if isinstance(data, dict) else None
            de = de or max(r["fecha"] for r in rows)
            print(f"[hist] data.json — {len(rows)} linhas, data_end={de}")
            return rows, de
    return [], "2026-01-01"

# ── 2. BigQuery incremental ───────────────────────────────────────────────────
def run_bq(dt_start, dt_end):
    from google.cloud import bigquery
    SQL = f"""
WITH
base_start AS (SELECT DATE '{dt_start}' AS dt_start, DATE '{dt_end}' AS dt_end),
solicitudes_aprobadas AS (
  SELECT req.FACILITY, detalles.ID AS detail_id, detalles.RELATIONSHIP_TYPE,
    detalles.SCHEDULE_ID, detalles.REQUEST_QUANTITY, fecha_solicitud_ajustada
  FROM `meli-bi-data.WHOWNER.BT_HCM_STAFF_REQUIREMENT` AS req,
    UNNEST(req.STAFF_REQUIREMENT_DETAILS) AS detalles,
    UNNEST(GENERATE_DATE_ARRAY(DATE(detalles.FROM_DATE), DATE(detalles.TO_DATE))) AS fecha_solicitud_ajustada
  WHERE detalles.STATUS IN ('FINALIZED', 'APPROVED', 'CONFIRMED')
    AND fecha_solicitud_ajustada >= (SELECT dt_start FROM base_start)
    AND fecha_solicitud_ajustada <= (SELECT dt_end FROM base_start)
    AND req.FACILITY IN ('SSP9','SSP29','SSP36')
),
solicitudes_diarias AS (
  SELECT s.FACILITY AS facility_id, s.fecha_solicitud_ajustada AS fecha,
    s.RELATIONSHIP_TYPE AS tipo_contratacion, SUM(s.REQUEST_QUANTITY) AS hc_solicitado
  FROM solicitudes_aprobadas AS s
  INNER JOIN `meli-bi-data.WHOWNER.LK_SHP_MT_TYA_SCHEDULE` AS turnos ON s.SCHEDULE_ID = turnos.ID
  INNER JOIN UNNEST(turnos.SCHEDULE.SCHEDULE_SHIFTS) AS detalle_turno
    ON detalle_turno.CYCLE_DAY = MOD(DATE_DIFF(s.fecha_solicitud_ajustada, turnos.START_DATE, DAY), turnos.SCHEDULE.CYCLE_DAYS) + 1
  WHERE COALESCE(turnos.IS_DELETED, FALSE) IS FALSE AND detalle_turno.WORKABLE IS TRUE
  GROUP BY 1,2,3
),
solicitudes_diarias_total AS (
  SELECT s.FACILITY AS facility_id, s.fecha_solicitud_ajustada AS fecha,
    s.RELATIONSHIP_TYPE AS tipo_contratacion, SUM(s.REQUEST_QUANTITY) AS hc_solicitado_total
  FROM solicitudes_aprobadas AS s GROUP BY 1,2,3
),
roster_diario AS (
  SELECT s.FACILITY AS facility_id, dias_roster AS fecha,
    s.RELATIONSHIP_TYPE AS tipo_contratacion, COUNT(DISTINCT roster.PERSON_ID) AS hc_rosterizado
  FROM `meli-bi-data.WHOWNER.BT_HCM_ROSTER` AS roster,
    UNNEST(GENERATE_DATE_ARRAY(DATE(roster.FROM_DATE), DATE(roster.TO_DATE))) AS dias_roster
  INNER JOIN solicitudes_aprobadas AS s
    ON CAST(roster.STAFF_REQUIREMENT_DETAIL_ID AS STRING) = CAST(s.detail_id AS STRING)
    AND dias_roster = s.fecha_solicitud_ajustada
  INNER JOIN `meli-bi-data.WHOWNER.LK_SHP_MT_TYA_SCHEDULE` AS turnos ON s.SCHEDULE_ID = turnos.ID
  INNER JOIN UNNEST(turnos.SCHEDULE.SCHEDULE_SHIFTS) AS detalle_turno
    ON detalle_turno.CYCLE_DAY = MOD(DATE_DIFF(dias_roster, turnos.START_DATE, DAY), turnos.SCHEDULE.CYCLE_DAYS) + 1
  WHERE (roster.DELETED_AT > CURRENT_DATETIME() OR roster.DELETED_AT IS NULL)
    AND COALESCE(turnos.IS_DELETED, FALSE) IS FALSE AND detalle_turno.WORKABLE IS TRUE
    AND dias_roster >= (SELECT dt_start FROM base_start)
    AND dias_roster <= (SELECT dt_end FROM base_start)
  GROUP BY 1,2,3
),
roster_diario_total AS (
  SELECT s.FACILITY AS facility_id, dias_roster AS fecha,
    s.RELATIONSHIP_TYPE AS tipo_contratacion, COUNT(DISTINCT roster.PERSON_ID) AS hc_rosterizado_total
  FROM `meli-bi-data.WHOWNER.BT_HCM_ROSTER` AS roster,
    UNNEST(GENERATE_DATE_ARRAY(DATE(roster.FROM_DATE), DATE(roster.TO_DATE))) AS dias_roster
  INNER JOIN solicitudes_aprobadas AS s
    ON CAST(roster.STAFF_REQUIREMENT_DETAIL_ID AS STRING) = CAST(s.detail_id AS STRING)
    AND dias_roster = s.fecha_solicitud_ajustada
  WHERE (roster.DELETED_AT > CURRENT_DATETIME() OR roster.DELETED_AT IS NULL)
    AND dias_roster >= (SELECT dt_start FROM base_start)
    AND dias_roster <= (SELECT dt_end FROM base_start)
  GROUP BY 1,2,3
),
timecards_con_facility AS (
  SELECT EMPLOYEE_ID, APPLIED_FOR, ABSENCE.ID AS absence_id, IS_DELETED,
    ASSIGNMENT.ID AS assignment_id,
    COALESCE(EXPECTED_WORK_DAY.FACILITY_ID,
      (SELECT p.FACILITY_ID FROM UNNEST(PUNCHES) AS p LIMIT 1)) AS facility_id
  FROM `meli-bi-data.WHOWNER.BT_SHP_TYA_EMPLOYEE_TIMECARD`
  WHERE DATE(APPLIED_FOR) >= (SELECT dt_start FROM base_start)
    AND DATE(APPLIED_FOR) <= (SELECT dt_end FROM base_start)
),
presentismo AS (
  SELECT tc.facility_id, DATE(tc.APPLIED_FOR) AS fecha,
    asignaciones.TYPE AS tipo_contratacion,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NOT NULL AND asignaciones.PROVIDER_ID IS NOT NULL THEN tc.EMPLOYEE_ID END) AS reps_presentes_con_turno_con_provider,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NOT NULL AND asignaciones.PROVIDER_ID IS NULL THEN tc.EMPLOYEE_ID END) AS reps_presentes_con_turno_sin_provider,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NULL THEN tc.EMPLOYEE_ID END) AS reps_presentes_sin_turno
  FROM timecards_con_facility AS tc
  LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_MT_TYA_EMPLOYEE_SCHEDULE` AS asignaciones
    ON tc.assignment_id = asignaciones.ID
  WHERE tc.absence_id IS NULL AND COALESCE(tc.IS_DELETED, FALSE) IS FALSE
    AND (asignaciones.ID IS NULL OR (
      COALESCE(asignaciones.IS_DELETED, FALSE) IS FALSE
      AND (asignaciones.DELETED_AT > CURRENT_DATETIME() OR asignaciones.DELETED_AT IS NULL)
    ))
    AND tc.facility_id IN ('SSP9','SSP29','SSP36')
  GROUP BY 1,2,3
),
dimensiones AS (
  SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM solicitudes_diarias
  UNION DISTINCT SELECT facility_id, fecha, tipo_contratacion FROM solicitudes_diarias_total
  UNION DISTINCT SELECT facility_id, fecha, tipo_contratacion FROM roster_diario
  UNION DISTINCT SELECT facility_id, fecha, tipo_contratacion FROM roster_diario_total
  UNION DISTINCT SELECT facility_id, fecha, tipo_contratacion FROM presentismo
)
SELECT d.fecha, d.facility_id, d.tipo_contratacion,
  COALESCE(s.hc_solicitado,        0) AS quantidade_hc_solicitado_workable,
  COALESCE(st.hc_solicitado_total, 0) AS quantidade_hc_solicitado_total,
  COALESCE(r.hc_rosterizado,       0) AS quantidade_hc_rosterizado_workable,
  COALESCE(rt.hc_rosterizado_total,0) AS quantidade_hc_rosterizado_total,
  COALESCE(p.reps_presentes_con_turno_con_provider,  0) AS reps_presentes_con_turno_con_provider,
  COALESCE(p.reps_presentes_con_turno_sin_provider,  0) AS reps_presentes_con_turno_sin_provider,
  COALESCE(p.reps_presentes_sin_turno,               0) AS reps_presentes_sin_turno
FROM dimensiones AS d
LEFT JOIN solicitudes_diarias       AS s  ON d.facility_id=s.facility_id  AND d.fecha=s.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(s.tipo_contratacion,'N/A')
LEFT JOIN solicitudes_diarias_total AS st ON d.facility_id=st.facility_id AND d.fecha=st.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(st.tipo_contratacion,'N/A')
LEFT JOIN roster_diario             AS r  ON d.facility_id=r.facility_id  AND d.fecha=r.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(r.tipo_contratacion,'N/A')
LEFT JOIN roster_diario_total       AS rt ON d.facility_id=rt.facility_id AND d.fecha=rt.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(rt.tipo_contratacion,'N/A')
LEFT JOIN presentismo               AS p  ON d.facility_id=p.facility_id  AND d.fecha=p.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(p.tipo_contratacion,'N/A')
WHERE d.facility_id IN ('SSP9','SSP29','SSP36')
ORDER BY d.fecha, d.facility_id, d.tipo_contratacion
"""
    client = bigquery.Client(project="meli-bi-data")
    rows = list(client.query(SQL).result())
    result = []
    for row in rows:
        obj = dict(row)
        for f in NUMERIC:
            obj[f] = float(obj.get(f) or 0)
        if obj.get("fecha"):
            obj["fecha"] = str(obj["fecha"])[:10]
        result.append(obj)
    print(f"[BQ] {len(result)} linhas | datas: {sorted({r['fecha'] for r in result})}")
    return result

# ── 3. Salvar arquivos locais e commitar via git ───────────────────────────────
def git_push(merged, data_end, today_str):
    payload = json.dumps({"rows": merged, "data_end": data_end}, ensure_ascii=False, separators=(",",":"))
    with open(f"data_{today_str}.json", "w", encoding="utf-8") as f: f.write(payload)
    with open("data.json", "w", encoding="utf-8") as f: f.write(payload)

    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
    subprocess.run(["git", "add", f"data_{today_str}.json", "data.json", "artifact_hcm.html"], check=True)
    result = subprocess.run(["git", "commit", "-m", f"Auto HCM {today_str} — data_end={data_end}"],
                            capture_output=True, text=True)
    if result.returncode == 0:
        subprocess.run(["git", "push"], check=True)
        print(f"[git] commit+push ok")
    else:
        print(f"[git] nada a commitar: {result.stdout.strip()}")

# ── 4. Atualizar artifact_hcm.html ────────────────────────────────────────────
def update_html_local(merged, data_end):
    with open("artifact_hcm.html", "r", encoding="utf-8") as f:
        html = f.read()
    json_str = json.dumps(merged, ensure_ascii=False, separators=(",",":"))
    html = re.sub(r"let EMBEDDED_DATA = \[.*?\];", f"let EMBEDDED_DATA = {json_str};", html, flags=re.DOTALL)
    html = re.sub(r"let DATA_END = '[^']*';", f"let DATA_END = '{data_end}';", html)
    html = html.replace(
        "function sum(arr, f) { return arr.reduce((a,r)=>a+(r[f]||0),0); }",
        "function sum(arr, f) { return arr.reduce((a,r)=>a+parseFloat(r[f]||0),0); }"
    )
    with open("artifact_hcm.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[HTML] DATA_END={data_end} | {len(html)} chars")
    return html

# ── 5. Upload Grid ─────────────────────────────────────────────────────────────
def upload_grid(data_end):
    with open("artifact_hcm.html", "rb") as fh:
        r = requests.post(
            "https://grid.melioffice.com/api/v1/engine/run",
            headers={"Authorization": f"Bearer {GRID_TOKEN}"},
            files={
                "config": (None, json.dumps({
                    "skill_version": "3.6.5",
                    "doc_id": DOC_ID,
                    "file_new_version": True,
                    "title": f"Tableau Gerencial HCM — até {data_end}"
                })),
                "file": ("artifact_hcm.html", fh, "text/html")
            },
            timeout=120
        )
    result = r.json()
    print(f"[Grid] ok={result.get('ok')} version={result.get('version')} url={result.get('view_url','')}")
    return result.get("ok", False)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    today   = datetime.date.today()
    dt_end  = today - datetime.timedelta(days=1)
    print(f"=== HCM Update {today} | target D-1={dt_end} ===")

    hist_rows, hist_data_end = load_hist()
    dt_hist  = datetime.date.fromisoformat(hist_data_end)
    dt_start = dt_hist + datetime.timedelta(days=1)

    if dt_start > dt_end:
        print(f"[skip-BQ] Já atualizado até {hist_data_end}")
        new_rows = []
        merged   = hist_rows
        data_end = hist_data_end
    else:
        print(f"[BQ] Consultando {dt_start} → {dt_end}")
        new_rows = run_bq(str(dt_start), str(dt_end))
        new_fechas = {r["fecha"] for r in new_rows}
        merged = [r for r in hist_rows if r.get("fecha") not in new_fechas] + new_rows
        merged.sort(key=lambda x: (x.get("fecha",""), x.get("facility_id",""), str(x.get("tipo_contratacion",""))))
        data_end = max(r["fecha"] for r in merged) if merged else str(dt_end)
        print(f"[merge] {len(merged)} linhas | data_end={data_end}")

    update_html_local(merged, data_end)

    today_str = today.strftime("%Y-%m-%d")
    git_push(merged, data_end, today_str)

    if not upload_grid(data_end):
        print("[ERRO] Grid upload falhou", file=sys.stderr)
        sys.exit(1)

    print(f"=== Concluído — {data_end} ===")

if __name__ == "__main__":
    main()
