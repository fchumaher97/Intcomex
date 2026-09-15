# ==========================================================================
# Databricks — ICP / MarketplaceSimpleAPI
# Pipeline de LICENCIAMIENTO: PreviewInvoices + Subscriptions (aplanado) + Audit
# -> Bronze (crudo) + Silver (inventario alineado al requerimiento)
#
# Auth: GetSessionToken (user/pass) -> header "Authenticate: CCPSessionId <token>"
#       El token se renueva solo si la sesión expira durante la corrida.
# IMPORTANTE: el API usa el AccountId tipo 281148 (NO el Numeric ID 736088).
# Requisito de red: egress por la IP pública en el allowlist de Intcomex.
# ==========================================================================

import requests, json, datetime as dt
import pandas as pd
from pyspark.sql import functions as F

BASE_URL = "https://marketplacexpe.intcomexcloud.com/SimpleAPI/SimpleAPIService.svc/rest"
CALLER_ACCOUNT_ID = 281148          # DAILY TECHNOLOGY SAC (AccountId del API)
CATALOG, BRONZE, SILVER = "dbx_icp_vnet", "icp_bronze", "icp_silver"
PULL_AUDIT = True                   # bitácora de cambios por empresa (puede ser lento)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")

# --------------------------------------------------------------------------
# 0) Autenticación con renovación automática de token de sesión
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
    _session["token"] = r.json()          # el token viene como string
    return _session["token"]

def _headers():
    if not _session["token"]:
        _login()
    return {"Authenticate": f"CCPSessionId {_session['token']}",
            "Content-Type": "application/json;charset=UTF8",
            "Accept": "application/json"}

def icp_post(endpoint, payload, _retry=True):
    r = requests.post(f"{BASE_URL}/{endpoint}", headers=_headers(), json=payload, timeout=180)
    if r.status_code == 200:
        return r.json()
    # sesión expirada -> renovar token y reintentar una vez
    if _retry and ("IsSessionExpired>true" in r.text or r.status_code == 401):
        _login()
        return icp_post(endpoint, payload, _retry=False)
    raise RuntimeError(f"{endpoint} falló ({r.status_code}): {r.text[:300]}")

def save_bronze(pdf, table):
    if pdf is None or len(pdf) == 0:
        print(f"  (sin datos) {table}"); return None
    df = spark.createDataFrame(pdf.astype(str).where(pd.notnull(pdf), None)) \
             .withColumn("_ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"{CATALOG}.{BRONZE}.{table}")
    print(f"  OK {table}: {df.count()} filas"); return df

# --------------------------------------------------------------------------
# 1) GetPreviewInvoices  (todas las empresas + costo del ciclo en curso)
# --------------------------------------------------------------------------
print("PreviewInvoices...")
preview = icp_post("GetPreviewInvoices",
                   {"resellerContext": CALLER_ACCOUNT_ID, "groupByDepartments": True})
inv_rows = []
for comp in (preview if isinstance(preview, list) else []):
    for ch in comp.get("Charges", []):
        inv_rows.append({
            "CompanyName":      comp.get("CompanyName"),
            "CompanyAccountId": comp.get("CompanyAccountId"),
            "CompanyVatId":     comp.get("CompanyVatId"),
            "BillingInterval":  comp.get("BillingInterval"),
            **{k: ch.get(k) for k in
               ("ServiceName","ServiceId","AccountId","Costs","CostsOfUnit",
                "SalesPrice","Currency","UDRCValue","BillableParameter",
                "ActualChargeInterval","VendorName","ProductNumber")}
        })
save_bronze(pd.DataFrame(inv_rows), "preview_invoices")

company_ids = sorted({c.get("CompanyAccountId") for c in
                      (preview if isinstance(preview, list) else [])
                      if c.get("CompanyAccountId")})
company_ids = list({CALLER_ACCOUNT_ID, *company_ids})
print(f"Empresas a recorrer: {len(company_ids)}")

# --------------------------------------------------------------------------
# 2) GetSubscriptions por empresa  ->  aplanar Fields[] y PriceableItems[]
# --------------------------------------------------------------------------
FIELD_PICK = {
    "Quantity": "quantity",
    "MicrosoftTenantId": "tenant_id",
    "TenantID": "tenant_id",
    "Subscriptionstatus": "subscription_status",
    "OfferId": "offer_id",
    "BillingType": "billing_type",
    "Segment": "segment",
    "SubscriptionName": "subscription_name",
}

def flatten_subscription(sub, company_id):
    fields = {f["Name"]: f.get("Value") for f in sub.get("Fields", [])}
    row = {
        "queried_company_id": company_id,
        "CompanyAccountId":   sub.get("CompanyAccountId"),
        "ParentAccountId":    sub.get("ParentAccountId"),
        "ParentType":         sub.get("ParentType"),
        "AccountId":          sub.get("AccountId"),
        "AccountState":       sub.get("AccountState"),
        "ServiceName":        sub.get("ServiceName"),
        "ServiceDisplayName": sub.get("ServiceDisplayName"),
        "VendorDisplayName":  sub.get("VendorDisplayName"),
        "BillingStartDate":   sub.get("BillingStartDate"),
        "ContractEndDate":    sub.get("ContractEndDate"),
        "ProvisioningStatus": sub.get("ProvisioningStatus"),
        "DependencyAccountId":sub.get("DependencyAccountId"),
    }
    for src, dst in FIELD_PICK.items():
        if src in fields and (dst not in row or row.get(dst) in (None, "")):
            row[dst] = fields[src]
    monthly = [p for p in sub.get("PriceableItems", []) if p.get("PriceableItemType") == "Monthly"]
    pit = monthly[0] if monthly else (sub.get("PriceableItems") or [{}])[0]
    row["purchase_price"] = pit.get("PurchasePrice")
    row["sales_price"]    = pit.get("SalesPrice")
    row["price_currency"] = pit.get("Currency")
    row["_fields_json"]   = json.dumps(fields, ensure_ascii=False, default=str)
    return row

print("Subscriptions...")
sub_rows = []
for cid in company_ids:
    try:
        res = icp_post("GetSubscriptions", {"parentAccountId": cid, "excludeUserLevel": False})
        for sub in (res if isinstance(res, list) else []):
            sub_rows.append(flatten_subscription(sub, cid))
    except Exception as e:
        print(f"  aviso: empresa {cid} -> {str(e)[:120]}")
save_bronze(pd.DataFrame(sub_rows), "subscriptions")

# --------------------------------------------------------------------------
# 3) Audit Log Object Company por empresa  (trazabilidad: quién/cuándo)
# --------------------------------------------------------------------------
if PULL_AUDIT:
    print("Audit Log...")
    audit_rows = []
    for cid in company_ids:
        try:
            res = icp_post("ExecuteReport", {"reportName": "Audit Log Object Company",
                           "parameters": {"targetCompany": cid, "startDate": "2026-01-01T00:00:00"}})
            cols = sorted(res.get("Columns", []), key=lambda c: c["CellIndex"])
            names = [c["Name"] for c in cols]
            for r in res.get("Rows", []):
                d = dict(zip(names, r["Cells"])); d["queried_company_id"] = cid
                audit_rows.append(d)
        except Exception as e:
            print(f"  aviso: audit {cid} -> {str(e)[:120]}")
    save_bronze(pd.DataFrame(audit_rows), "audit_log")

# --------------------------------------------------------------------------
# 4) Silver — inventario de licenciamiento alineado al requerimiento
# --------------------------------------------------------------------------
subs = spark.table(f"{CATALOG}.{BRONZE}.subscriptions")
silver = subs.select(
    F.col("CompanyAccountId").alias("empresa_account_id"),
    F.col("tenant_id"),
    F.col("ServiceName").alias("sku_id"),
    F.col("ServiceDisplayName").alias("sku_nombre"),
    F.col("VendorDisplayName").alias("vendor"),
    F.col("quantity").cast("int").alias("cantidad_contratada"),
    F.coalesce(F.col("subscription_status"), F.col("AccountState")).alias("estado"),
    F.to_timestamp("BillingStartDate").alias("fecha_alta"),
    F.to_timestamp("ContractEndDate").alias("fecha_fin_contrato"),
    F.col("purchase_price").cast("double").alias("costo_unit_compra"),
    F.col("sales_price").cast("double").alias("precio_unit_venta"),
    F.col("price_currency").alias("moneda"),
    F.col("AccountId").alias("subscription_id"),
)
(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SILVER}.licenciamiento"))
print(f"\nSilver lista: {CATALOG}.{SILVER}.licenciamiento")
display(silver.limit(30))
