#!/usr/bin/env python3
"""
HCM Dashboard Incremental Update — GitHub Actions
Roda D-1 todo dia às 09:00 BRT (12:00 UTC) no servidor do GitHub.
"""
import json, base64, requests, datetime, os, sys, re

GITHUB_TOKEN = os.environ.get("GH_TOKEN", "")
GITHUB_OWNER = "fabiopaulo10"
GITHUB_REPO  = "hcm-dashboard-automation"
GRID_TOKEN   = os.environ.get("GRID_TOKEN", "grid_sk_01KSZV8M0KB8M8MWDR3FBZ4VWW")
GRID_DOC_ID  = "01KSRJ5STC80GGR6CKPB24MH9W"
GRID_HOST    = "https://grid.melioffice.com"
BQ_PROJECT   = "meli-bi-data"

gh_headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ── 1. Descobre datas faltantes ─────────────────────────────────────────────
def get_historical():
    today = datetime.date.today()
    for i in range(1, 8):
        d = today - datetime.timedelta(days=i)
        fname = f"data_{d}.json"
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{fname}",
            headers=gh_headers, timeout=30
        )
        if r.status_code == 200:
            content = base64.b64decode(r.json()["content"])
            data = json.loads(content)
            print(f"Histórico: {fname} | {len(data['rows'])} rows | data_end={data['data_end']}")
            return data["rows"], data["data_end"]
    # Fallback: começa do zero
    dt = (today - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"Nenhum arquivo histórico encontrado — começa de {dt}")
    return [], dt

# ── 2. Query BigQuery incremental ───────────────────────────────────────────
def run_bq_query(dt_start: str, dt_end: str):
    from google.cloud import bigquery
    client = bigquery.Client(project=BQ_PROJECT)
    print(f"BigQuery: {dt_start} → {dt_end}")
    sql = f"""
WITH
base_start AS (SELECT DATE '{dt_start}' AS dt_start, DATE '{dt_end}' AS dt_end),
facilities_activos AS (
  SELECT DISTINCT req.FACILITY
  FROM `meli-bi-data.WHOWNER.BT_HCM_STAFF_REQUIREMENT` AS req
),
solicitudes_aprobadas AS (
  SELECT req.FACILITY, detalles.ID AS detail_id, detalles.RELATIONSHIP_TYPE,
    detalles.SCHEDULE_ID, detalles.REQUEST_QUANTITY, fecha_solicitud_ajustada
  FROM `meli-bi-data.WHOWNER.BT_HCM_STAFF_REQUIREMENT` AS req,
    UNNEST(req.STAFF_REQUIREMENT_DETAILS) AS detalles,
    UNNEST(GENERATE_DATE_ARRAY(DATE(detalles.FROM_DATE), DATE(detalles.TO_DATE))) AS fecha_solicitud_ajustada
  WHERE detalles.STATUS IN ('FINALIZED', 'APPROVED', 'CONFIRMED')
    AND fecha_solicitud_ajustada >= (SELECT dt_start FROM base_start)
    AND fecha_solicitud_ajustada <= (SELECT dt_end FROM base_start)
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
    AND tc.facility_id IN (SELECT FACILITY FROM facilities_activos)
  GROUP BY 1,2,3
),
dimensiones AS (
  SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM solicitudes_diarias
  UNION DISTINCT SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM solicitudes_diarias_total
  UNION DISTINCT SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM roster_diario
  UNION DISTINCT SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM roster_diario_total
  UNION DISTINCT SELECT DISTINCT facility_id, fecha, tipo_contratacion FROM presentismo
)
SELECT
  d.fecha,
  CASE WHEN d.facility_id='BRXSP8' THEN 'SSP26'
       WHEN d.facility_id='XPR1'   THEN 'SPR8'
       WHEN d.facility_id='XRJ1'   THEN 'SRJ1'
       ELSE d.facility_id END AS facility_id,
  d.tipo_contratacion,
  COALESCE(s.hc_solicitado,  0) AS quantidade_hc_solicitado_workable,
  COALESCE(st.hc_solicitado_total, 0) AS quantidade_hc_solicitado_total,
  COALESCE(r.hc_rosterizado, 0) AS quantidade_hc_rosterizado_workable,
  COALESCE(rt.hc_rosterizado_total, 0) AS quantidade_hc_rosterizado_total,
  COALESCE(p.reps_presentes_con_turno_con_provider, 0) AS reps_presentes_con_turno_con_provider,
  COALESCE(p.reps_presentes_con_turno_sin_provider, 0) AS reps_presentes_con_turno_sin_provider,
  COALESCE(p.reps_presentes_sin_turno, 0) AS reps_presentes_sin_turno
FROM dimensiones AS d
LEFT JOIN solicitudes_diarias        AS s  ON d.facility_id=s.facility_id  AND d.fecha=s.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(s.tipo_contratacion,'N/A')
LEFT JOIN solicitudes_diarias_total  AS st ON d.facility_id=st.facility_id AND d.fecha=st.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(st.tipo_contratacion,'N/A')
LEFT JOIN roster_diario              AS r  ON d.facility_id=r.facility_id  AND d.fecha=r.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(r.tipo_contratacion,'N/A')
LEFT JOIN roster_diario_total        AS rt ON d.facility_id=rt.facility_id AND d.fecha=rt.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(rt.tipo_contratacion,'N/A')
LEFT JOIN presentismo                AS p  ON d.facility_id=p.facility_id  AND d.fecha=p.fecha  AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(p.tipo_contratacion,'N/A')
WHERE CASE WHEN d.facility_id='BRXSP8' THEN 'SSP26'
           WHEN d.facility_id='XPR1'   THEN 'SPR8'
           WHEN d.facility_id='XRJ1'   THEN 'SRJ1'
           ELSE d.facility_id END IN ('SSP9','SSP29','SSP36')
ORDER BY d.fecha, d.facility_id, d.tipo_contratacion
"""
    numeric_fields = [
        "quantidade_hc_solicitado_workable", "quantidade_hc_solicitado_total",
        "quantidade_hc_rosterizado_workable", "quantidade_hc_rosterizado_total",
        "reps_presentes_con_turno_con_provider", "reps_presentes_con_turno_sin_provider",
        "reps_presentes_sin_turno",
    ]
    rows = []
    for row in client.query(sql).result():
        obj = dict(row)
        for f in numeric_fields:
            obj[f] = float(obj.get(f) or 0)
        if obj.get("fecha"):
            obj["fecha"] = str(obj["fecha"])[:10]
        rows.append(obj)
    print(f"BigQuery: {len(rows)} linhas novas")
    return rows

# ── 3. Push para GitHub ──────────────────────────────────────────────────────
def github_put(filename, content_bytes, msg):
    api_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{filename}"
    r = requests.get(api_url, headers=gh_headers, timeout=30)
    sha = r.json().get("sha") if r.status_code == 200 else None
    payload = {"message": msg, "content": base64.b64encode(content_bytes).decode("ascii"), "branch": "main"}
    if sha:
        payload["sha"] = sha
    resp = requests.put(api_url, headers=gh_headers, json=payload, timeout=120)
    return resp.status_code

# ── 4. Upload para Grid ──────────────────────────────────────────────────────
def upload_to_grid(html_content: str, data_end: str):
    payload = json.dumps({
        "skill_version": "3.6.5",
        "doc_id": GRID_DOC_ID,
        "file_new_version": True
    })
    files = [
        ("config", (None, payload, "application/json")),
        ("file", ("artifact_hcm.html", html_content.encode("utf-8"), "text/html")),
    ]
    headers = {"Authorization": f"Bearer {GRID_TOKEN}"}
    resp = requests.post(f"{GRID_HOST}/api/v1/engine/run", headers=headers, files=files, timeout=120)
    try:
        r = resp.json()
        if r.get("ok"):
            print(f"Grid: versão {r.get('version')} | {r.get('view_url')}")
        else:
            print(f"Grid erro: {r}")
    except Exception as e:
        print(f"Grid HTTP {resp.status_code}: {resp.text[:200]}")

# ── 5. Atualiza HTML embutido ────────────────────────────────────────────────
def update_html_embedded(rows, data_end):
    html_path = os.path.join(os.path.dirname(__file__), "artifact_hcm.html")
    if not os.path.exists(html_path):
        print("artifact_hcm.html não encontrado — pulando upload Grid")
        return None
    with open(html_path, encoding="utf-8") as f:
        html = f.read()
    json_str = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(r"let EMBEDDED_DATA = \[.*?\];", f"let EMBEDDED_DATA = {json_str};", html, flags=re.DOTALL)
    html = re.sub(r"let DATA_END = '.*?';", f"let DATA_END = '{data_end}';", html)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    return html

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    today     = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    dt_end    = yesterday.strftime("%Y-%m-%d")

    # 1. Histórico
    hist_rows, hist_data_end = get_historical()

    # Data de início da query = dia seguinte ao data_end histórico
    dt_start_date = datetime.date.fromisoformat(hist_data_end) + datetime.timedelta(days=1)
    dt_start      = dt_start_date.strftime("%Y-%m-%d")

    if dt_start > dt_end:
        print(f"Já atualizado até {hist_data_end} — nada a fazer.")
        return

    # 2. Query incremental
    new_rows = run_bq_query(dt_start, dt_end)
    if not new_rows:
        print("Nenhum dado novo retornado pelo BigQuery.")
        return

    # 3. Merge
    hist_rows_filtered = [r for r in hist_rows if r["fecha"] < dt_start]
    all_rows  = hist_rows_filtered + new_rows
    data_end  = max(r["fecha"] for r in all_rows)
    data_json = json.dumps({"rows": all_rows, "data_end": data_end}, ensure_ascii=False, separators=(",", ":"))
    print(f"Merge: {len(all_rows)} linhas | data_end={data_end}")

    # 4. GitHub
    today_str = today.strftime("%Y-%m-%d")
    s1 = github_put(f"data_{today_str}.json", data_json.encode("utf-8"), f"Auto HCM {today_str} ({len(all_rows)} rows)")
    s2 = github_put("data.json", data_json.encode("utf-8"), f"data.json — {data_end}")
    print(f"GitHub data_{today_str}.json: HTTP {s1} | data.json: HTTP {s2}")

    # 5. Grid
    html = update_html_embedded(all_rows, data_end)
    if html:
        upload_to_grid(html, data_end)

    print("=== CONCLUÍDO ===")

if __name__ == "__main__":
    main()
