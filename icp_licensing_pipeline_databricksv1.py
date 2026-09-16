# ==========================================================================
# Databricks — ICP / MarketplaceSimpleAPI
# Pipeline de LICENCIAMIENTO — carga INCREMENTAL (MERGE / upsert)
#   subscriptions : upsert por AccountId + baja lógica (activo/first_seen/last_seen)
#   audit_log     : insert-only por Id (acumula historia)
#   preview_invoices: overwrite (foto del ciclo en curso)
#   silver        : proyección derivada del bronze (conserva historia)
#
# Auth: GetSessionToken (user/pass) -> "Authenticate: CCPSessionId <token>"
# Red : egress por la IP pública en el allowlist de Intcomex.
# ==========================================================================

import requests, json, datetime as dt
import pandas as pd
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# --------------------------------------------------------------------------
# Parámetros (widgets) — el Job los puede sobreescribir sin editar código
# --------------------------------------------------------------------------
dbutils.widgets.text("catalog", "dbx_icp_vnet")
dbutils.widgets.text("reseller_account_id", "281148")   # AccountId del API (NO 736088)
dbutils.widgets.text("audit_start_date", "2026-01-01T00:00:00")
dbutils.widgets.dropdown("pull_audit", "true", ["true", "false"])

CATALOG           = dbutils.widgets.get("catalog")
CALLER_ACCOUNT_ID = int(dbutils.widgets.get("reseller_account_id"))
AUDIT_START       = dbutils.widgets.get("audit_start_date")
PULL_AUDIT        = dbutils.widgets.get("pull_audit") == "true"
BRONZE, SILVER    = "icp_bronze", "icp_silver"
BASE_URL = "https://marketplacexpe.intcomexcloud.com/SimpleAPI/SimpleAPIService.svc/rest"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")

# --------------------------------------------------------------------------
# Autenticación con renovación automática de token
# --------------------------------------------------------------------------
_session = {"token": None}

def _login():
    u = dbutils.secrets.get("intcomex", "username").strip()
    p = dbutils.secrets.get("intcomex", "password").strip()
    r = requests.post(f"{BASE_URL}/GetSessionToken",
        headers={"Content-Type": "application/json;charset=UTF8", "Accept": "application/json"},
        json={"username": u, "password": p}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Login ICP falló ({r.status_code}): {r.text[:300]}")
    _session["token"] = r.json()

def _headers():
    if not _session["token"]:
        _login()
    return {"Authenticate": f"CCPSessionId {_session['token']}",
            "Content-Type": "application/json;charset=UTF8", "Accept": "application/json"}

def icp_post(endpoint, payload, _retry=True):
    r = requests.post(f"{BASE_URL}/{endpoint}", headers=_headers(), json=payload, timeout=180)
    if r.status_code == 200:
        return r.json()
    if _retry and ("IsSessionExpired>true" in r.text or r.status_code == 401):
        _login(); return icp_post(endpoint, payload, _retry=False)
    raise RuntimeError(f"{endpoint} falló ({r.status_code}): {r.text[:300]}")

# --------------------------------------------------------------------------
# Helpers de carga
# --------------------------------------------------------------------------
def _to_spark(pdf):
    if pdf is None or len(pdf) == 0:
        return None
    return spark.createDataFrame(pdf.astype(str).where(pd.notnull(pdf), None))

def save_overwrite(pdf, table):
    df = _to_spark(pdf)
    if df is None:
        print(f"  (sin datos) {table}"); return None
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"{CATALOG}.{BRONZE}.{table}")
    print(f"  OK {table}: {df.count()} filas (overwrite)"); return df

def merge_upsert(pdf, table, key_cols, mode="scd", bysource_condition=None):
    """mode='scd': upsert + baja lógica (activo). mode='append_only': solo insertar nuevos por key.
       bysource_condition: limita la baja lógica a las filas que cumplen la condición (p.ej. solo
       las empresas consultadas OK esta corrida), para no dar de baja por un error parcial."""
    df = _to_spark(pdf)
    if df is None:
        print(f"  (sin datos) {table}"); return
    df = df.dropDuplicates(key_cols).withColumn("last_seen", F.current_timestamp())
    fqn = f"{CATALOG}.{BRONZE}.{table}"
    if not spark.catalog.tableExists(fqn):
        init = df.withColumn("first_seen", F.col("last_seen"))
        if mode == "scd":
            init = init.withColumn("activo", F.lit(True))
        init.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
        print(f"  creada {table}: {init.count()} filas"); return

    tgt  = DeltaTable.forName(spark, fqn)
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in key_cols])
    ins  = {c: f"s.`{c}`" for c in df.columns}; ins["first_seen"] = "s.`last_seen`"
    upd  = {c: f"s.`{c}`" for c in df.columns}
    m = tgt.alias("t").merge(df.alias("s"), cond)
    if mode == "scd":
        ins["activo"] = "true"; upd["activo"] = "true"
        m = m.whenMatchedUpdate(set=upd).whenNotMatchedInsert(values=ins)
        if bysource_condition:
            m = m.whenNotMatchedBySourceUpdate(condition=bysource_condition, set={"activo": "false"})
        else:
            m = m.whenNotMatchedBySourceUpdate(set={"activo": "false"})
        m.execute()
    else:  # append_only
        m.whenNotMatchedInsert(values=ins).execute()
    print(f"  merge {table}: {df.count()} filas procesadas ({mode})")

# --------------------------------------------------------------------------
# 1) PreviewInvoices (todas las empresas + costo del ciclo)  -> overwrite
# --------------------------------------------------------------------------
print("PreviewInvoices...")
preview = icp_post("GetPreviewInvoices", {"resellerContext": CALLER_ACCOUNT_ID, "groupByDepartments": True})
inv_rows = []
for comp in (preview if isinstance(preview, list) else []):
    for ch in comp.get("Charges", []):
        inv_rows.append({
            "CompanyName": comp.get("CompanyName"), "CompanyAccountId": comp.get("CompanyAccountId"),
            "CompanyVatId": comp.get("CompanyVatId"), "BillingInterval": comp.get("BillingInterval"),
            **{k: ch.get(k) for k in ("ServiceName","ServiceId","AccountId","Costs","CostsOfUnit",
                "SalesPrice","Currency","UDRCValue","BillableParameter","ActualChargeInterval",
                "VendorName","ProductNumber")}})
save_overwrite(pd.DataFrame(inv_rows), "preview_invoices")

company_ids = list({CALLER_ACCOUNT_ID, *{c.get("CompanyAccountId") for c in
                    (preview if isinstance(preview, list) else []) if c.get("CompanyAccountId")}})
print(f"Empresas a recorrer: {len(company_ids)}")

# --------------------------------------------------------------------------
# 2) GetSubscriptions por empresa -> aplanar -> upsert incremental
# --------------------------------------------------------------------------
FIELD_PICK = {"Quantity": "quantity", "MicrosoftTenantId": "tenant_id", "TenantID": "tenant_id",
              "Subscriptionstatus": "subscription_status", "OfferId": "offer_id",
              "BillingType": "billing_type", "Segment": "segment", "SubscriptionName": "subscription_name"}

def flatten_subscription(sub, company_id):
    fields = {f["Name"]: f.get("Value") for f in sub.get("Fields", [])}
    row = {
        "queried_company_id": company_id, "CompanyAccountId": sub.get("CompanyAccountId"),
        "ParentAccountId": sub.get("ParentAccountId"), "ParentType": sub.get("ParentType"),
        "AccountId": sub.get("AccountId"), "AccountState": sub.get("AccountState"),
        "ServiceName": sub.get("ServiceName"), "ServiceDisplayName": sub.get("ServiceDisplayName"),
        "VendorDisplayName": sub.get("VendorDisplayName"), "BillingStartDate": sub.get("BillingStartDate"),
        "ContractEndDate": sub.get("ContractEndDate"), "ProvisioningStatus": sub.get("ProvisioningStatus"),
        "DependencyAccountId": sub.get("DependencyAccountId"),
        "AdvancePeriodEndAction": sub.get("AdvancePeriodEndAction"),
        "HasRenewActionValuesConfigured": sub.get("HasRenewActionValuesConfigured"),
        "ContractId": sub.get("ContractId"), "PriceProtectionEndDate": sub.get("PriceProtectionEndDate"),
    }
    for src, dst in FIELD_PICK.items():
        if src in fields and (dst not in row or row.get(dst) in (None, "")):
            row[dst] = fields[src]
    monthly = [p for p in sub.get("PriceableItems", []) if p.get("PriceableItemType") == "Monthly"]
    pit = monthly[0] if monthly else (sub.get("PriceableItems") or [{}])[0]
    row["purchase_price"] = pit.get("PurchasePrice"); row["sales_price"] = pit.get("SalesPrice")
    row["price_currency"] = pit.get("Currency"); row["commitment_months"] = pit.get("CommitementPeriodInMonths")
    row["_fields_json"] = json.dumps(fields, ensure_ascii=False, default=str)
    return row

print("Subscriptions...")
sub_rows, ok_ids = [], []
for cid in company_ids:
    try:
        res = icp_post("GetSubscriptions", {"parentAccountId": cid, "excludeUserLevel": False})
        for sub in (res if isinstance(res, list) else []):
            sub_rows.append(flatten_subscription(sub, cid))
        ok_ids.append(str(cid))                      # empresa consultada OK
    except Exception as e:
        print(f"  aviso: empresa {cid} -> {str(e)[:120]}")

# baja lógica SOLO dentro de las empresas consultadas OK (evita bajas falsas por error parcial)
bysrc = None
if ok_ids:
    inlist = ",".join(f"'{i}'" for i in ok_ids)
    bysrc = f"t.`queried_company_id` IN ({inlist})"
merge_upsert(pd.DataFrame(sub_rows), "subscriptions", ["AccountId"], mode="scd", bysource_condition=bysrc)

# --------------------------------------------------------------------------
# 3) Audit Log por empresa -> merge insert-only por Id (acumula)
# --------------------------------------------------------------------------
if PULL_AUDIT:
    print("Audit Log...")
    audit_rows = []
    for cid in company_ids:
        try:
            res = icp_post("ExecuteReport", {"reportName": "Audit Log Object Company",
                           "parameters": {"targetCompany": cid, "startDate": AUDIT_START}})
            cols = sorted(res.get("Columns", []), key=lambda c: c["CellIndex"])
            names = [c["Name"] for c in cols]
            for r in res.get("Rows", []):
                d = dict(zip(names, r["Cells"])); d["queried_company_id"] = cid
                audit_rows.append(d)
        except Exception as e:
            print(f"  aviso: audit {cid} -> {str(e)[:120]}")
    merge_upsert(pd.DataFrame(audit_rows), "audit_log", ["Id"], mode="append_only")

# --------------------------------------------------------------------------
# 4) Silver — proyección derivada del bronze (conserva bajas via 'activo')
# --------------------------------------------------------------------------
subs  = spark.table(f"{CATALOG}.{BRONZE}.subscriptions")
audit = spark.table(f"{CATALOG}.{BRONZE}.audit_log")

w = Window.partitionBy("TargetAccountId").orderBy(F.col("_evt_date").desc())
audit_last = (audit.withColumn("_evt_date", F.to_timestamp("Date"))
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    .select(F.col("TargetAccountId").cast("string").alias("_acct"),
            F.col("UserUsername").alias("activado_por"),
            F.col("EventType").alias("ultimo_evento"),
            F.col("_evt_date").alias("fecha_ultimo_evento")))

# dimensión de nombres de empresa: preview_invoices (prioridad 1) + audit empresa (prioridad 2)
prev = spark.table(f"{CATALOG}.{BRONZE}.preview_invoices")
dim_prev = (prev.select(F.col("CompanyAccountId").cast("string").alias("cid"),
                        F.col("CompanyName").alias("nombre"))
                .where("nombre is not null").withColumn("_pri", F.lit(1)))
dim_aud = (audit.where("TargetAccountType = 'Company'")
                .select(F.col("TargetAccountId").cast("string").alias("cid"),
                        F.col("TargetDisplayName").alias("nombre"))
                .where("nombre is not null").withColumn("_pri", F.lit(2)))
_wc = Window.partitionBy("cid").orderBy("_pri")
dim_company = (dim_prev.unionByName(dim_aud)
               .withColumn("_rn", F.row_number().over(_wc)).filter("_rn = 1")
               .select("cid", "nombre"))

base = subs.select(
    F.col("CompanyAccountId").alias("empresa_account_id"),
    F.col("tenant_id"),
    F.col("ServiceName").alias("sku_id"),
    F.col("ServiceDisplayName").alias("sku_nombre"),
    F.col("VendorDisplayName").alias("vendor"),
    F.expr("cast(try_cast(quantity as double) as int)").alias("cantidad_contratada"),
    F.coalesce(F.col("subscription_status"), F.col("AccountState")).alias("estado"),
    F.to_timestamp("BillingStartDate").alias("fecha_alta"),
    F.to_timestamp("ContractEndDate").alias("fecha_fin_contrato"),
    F.col("AdvancePeriodEndAction").alias("accion_fin_periodo"),
    F.when(F.col("AdvancePeriodEndAction").isNull(), None)
     .otherwise(F.col("AdvancePeriodEndAction") != F.lit("Terminate")).alias("es_renovable"),
    (F.lower(F.coalesce(F.col("billing_type"), F.lit(""))).contains("commitment")
     | (F.expr("try_cast(commitment_months as int)") > 0)).alias("tiene_compromiso"),
    F.expr("try_cast(commitment_months as int)").alias("meses_compromiso"),
    F.col("billing_type").alias("tipo_facturacion"),
    F.to_timestamp("PriceProtectionEndDate").alias("fin_proteccion_precio"),
    F.col("ContractId").alias("contrato_id"),
    F.expr("try_cast(purchase_price as double)").alias("costo_unit_compra"),
    F.expr("try_cast(sales_price as double)").alias("precio_unit_venta"),
    F.col("price_currency").alias("moneda"),
    F.col("AccountId").cast("string").alias("subscription_id"),
    F.col("activo"), F.col("first_seen"), F.col("last_seen"),
)

silver = (base
    .join(audit_last, base.subscription_id == audit_last._acct, "left").drop("_acct")
    .join(dim_company, base.empresa_account_id == dim_company.cid, "left").drop("cid")
    .withColumn("empresa_nombre", F.coalesce(F.col("nombre"), F.col("empresa_account_id"))).drop("nombre"))

# ordena columnas: nombre de empresa junto al id
_first = ["empresa_account_id", "empresa_nombre", "tenant_id"]
silver = silver.select(*_first, *[c for c in silver.columns if c not in _first])

(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SILVER}.licenciamiento"))
print(f"\nSilver lista: {CATALOG}.{SILVER}.licenciamiento  ({silver.count()} filas, "
      f"{silver.filter('activo = true').count()} activas)")
display(silver.limit(30))