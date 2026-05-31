"""
GitHub Actions Job — Dashboard HCM WH Operations
Atualiza dados do BigQuery e salva o HTML localmente (para GitHub Pages).
"""

import json
import re
import os
from google.cloud import bigquery

BQ_PROJECT   = "meli-bi-data"
HTML_TEMPLATE = "docs/index.html"

SQL = """
WITH
base_date AS (SELECT DATE '2026-01-01' AS dt),
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
    AND fecha_solicitud_ajustada >= (SELECT dt FROM base_date)
    AND fecha_solicitud_ajustada <= CURRENT_DATE('America/Sao_Paulo')
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
    AND dias_roster >= (SELECT dt FROM base_date)
    AND dias_roster <= CURRENT_DATE('America/Sao_Paulo')
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
    AND dias_roster >= (SELECT dt FROM base_date)
    AND dias_roster <= CURRENT_DATE('America/Sao_Paulo')
  GROUP BY 1,2,3
),
timecards_con_facility AS (
  SELECT EMPLOYEE_ID, APPLIED_FOR, ABSENCE.ID AS absence_id, IS_DELETED,
    ASSIGNMENT.ID AS assignment_id,
    COALESCE(EXPECTED_WORK_DAY.FACILITY_ID,
      (SELECT p.FACILITY_ID FROM UNNEST(PUNCHES) AS p LIMIT 1)) AS facility_id
  FROM `meli-bi-data.WHOWNER.BT_SHP_TYA_EMPLOYEE_TIMECARD`
  WHERE DATE(APPLIED_FOR) >= DATE '2026-01-01'
),
presentismo AS (
  SELECT tc.facility_id, DATE(tc.APPLIED_FOR) AS fecha,
    asignaciones.TYPE AS tipo_contratacion,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NOT NULL AND asignaciones.PROVIDER_ID IS NOT NULL THEN tc.EMPLOYEE_ID END) AS presentes_con_turno_con_provider,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NOT NULL AND asignaciones.PROVIDER_ID IS NULL THEN tc.EMPLOYEE_ID END) AS presentes_con_turno_sin_provider,
    COUNT(DISTINCT CASE WHEN tc.assignment_id IS NULL THEN tc.EMPLOYEE_ID END) AS presentes_sin_turno
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
SELECT d.fecha,
  CASE WHEN d.facility_id='BRXSP8' THEN 'SSP26' WHEN d.facility_id='XPR1' THEN 'SPR8' WHEN d.facility_id='XRJ1' THEN 'SRJ1' ELSE d.facility_id END AS facility_id,
  d.tipo_contratacion,
  COALESCE(s.hc_solicitado,0) AS quantidade_hc_solicitado_workable,
  COALESCE(st.hc_solicitado_total,0) AS quantidade_hc_solicitado_total,
  COALESCE(r.hc_rosterizado,0) AS quantidade_hc_rosterizado_workable,
  COALESCE(rt.hc_rosterizado_total,0) AS quantidade_hc_rosterizado_total,
  COALESCE(p.presentes_con_turno_con_provider,0) AS reps_presentes_con_turno_con_provider,
  COALESCE(p.presentes_con_turno_sin_provider,0) AS reps_presentes_con_turno_sin_provider,
  COALESCE(p.presentes_sin_turno,0) AS reps_presentes_sin_turno
FROM dimensiones AS d
LEFT JOIN solicitudes_diarias AS s ON d.facility_id=s.facility_id AND d.fecha=s.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(s.tipo_contratacion,'N/A')
LEFT JOIN solicitudes_diarias_total AS st ON d.facility_id=st.facility_id AND d.fecha=st.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(st.tipo_contratacion,'N/A')
LEFT JOIN roster_diario AS r ON d.facility_id=r.facility_id AND d.fecha=r.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(r.tipo_contratacion,'N/A')
LEFT JOIN roster_diario_total AS rt ON d.facility_id=rt.facility_id AND d.fecha=rt.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(rt.tipo_contratacion,'N/A')
LEFT JOIN presentismo AS p ON d.facility_id=p.facility_id AND d.fecha=p.fecha AND IFNULL(d.tipo_contratacion,'N/A')=IFNULL(p.tipo_contratacion,'N/A')
WHERE CASE WHEN d.facility_id='BRXSP8' THEN 'SSP26' WHEN d.facility_id='XPR1' THEN 'SPR8' WHEN d.facility_id='XRJ1' THEN 'SRJ1' ELSE d.facility_id END IN ('SSP9','SSP18','SSP29','SSP36')
ORDER BY d.fecha, d.facility_id, d.tipo_contratacion
"""

NUMERIC_FIELDS = [
    "quantidade_hc_solicitado_workable","quantidade_hc_solicitado_total",
    "quantidade_hc_rosterizado_workable","quantidade_hc_rosterizado_total",
    "reps_presentes_con_turno_con_provider","reps_presentes_con_turno_sin_provider",
    "reps_presentes_sin_turno",
]

def run_query():
    client = bigquery.Client(project=BQ_PROJECT)
    rows = list(client.query(SQL).result())
    result = []
    for row in rows:
        obj = dict(row)
        for f in NUMERIC_FIELDS:
            obj[f] = float(obj.get(f) or 0)
        if obj.get("fecha"):
            obj["fecha"] = str(obj["fecha"])[:10]
        result.append(obj)
    print(f"[BQ] {len(result)} linhas | última fecha: {max(r['fecha'] for r in result)}")
    return result

def update_html(rows):
    with open(HTML_TEMPLATE, 'r', encoding='utf-8') as f:
        html = f.read()
    data_end = max(r["fecha"] for r in rows)
    json_str = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(r"const EMBEDDED_DATA = \[.*?\];", f"const EMBEDDED_DATA = {json_str};", html, flags=re.DOTALL)
    html = re.sub(r"const DATA_END = '.*?';", f"const DATA_END = '{data_end}';", html)
    with open(HTML_TEMPLATE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"[HTML] Atualizado — DATA_END: {data_end}")
    return data_end

def main():
    print("=== HCM Dashboard Update ===")
    os.makedirs("docs", exist_ok=True)
    if not os.path.exists(HTML_TEMPLATE):
        import shutil
        if os.path.exists("artifact_hcm.html"):
            shutil.copy("artifact_hcm.html", HTML_TEMPLATE)
        else:
            print("ERRO: arquivo HTML base não encontrado")
            return
    print("1. Executando query BigQuery...")
    rows = run_query()
    print("2. Atualizando HTML...")
    data_end = update_html(rows)
    print(f"=== Concluído — {len(rows)} linhas até {data_end} ===")

if __name__ == "__main__":
    main()
