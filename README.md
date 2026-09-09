# Azure Databricks con VNet Injection y NAT Gateway para ICP

## 1. Objetivo

Implementar un workspace de Azure Databricks con una dirección IP pública estática para consumir la API REST de Intcomex Cloud Platform (ICP), cuya seguridad requiere registrar la IP de origen en un allowlist.

La solución utiliza:

- Azure Databricks con VNet Injection.
- Secure Cluster Connectivity.
- Azure Virtual Network.
- Dos subredes delegadas a Azure Databricks.
- Network Security Group administrado por Databricks.
- NAT Gateway.
- Dirección IP pública estática.
- Cluster clásico de propósito general en modo Single Node.
- Git folder para versionar notebooks y código.
- Databricks Secret Scope para proteger credenciales de ICP.

> **IP pública estática validada:** `20.124.193.245`

---

## 2. Arquitectura final

```text
┌─────────────────────────────────────────────┐
│ Intcomex Cloud Platform                     │
│ MarketplaceSimpleAPI                       │
│ Allowlist: 20.124.193.245                   │
└──────────────────────▲──────────────────────┘
                       │ HTTPS 443
                       │
┌──────────────────────┴──────────────────────┐
│ Public IP estática                          │
│ pip-icp-nat                                 │
│ 20.124.193.245                              │
└──────────────────────▲──────────────────────┘
                       │
┌──────────────────────┴──────────────────────┐
│ NAT Gateway                                 │
│ nat-icp                                     │
└──────────────────────▲──────────────────────┘
                       │ Asociado a ambas subredes
┌──────────────────────┴──────────────────────┐
│ VNet: vnet-databricks-icp                   │
│ CIDR: 10.100.0.0/16                         │
│                                             │
│ ┌─────────────────────────────────────────┐ │
│ │ snet-host                               │ │
│ │ 10.100.1.0/24                           │ │
│ │ Microsoft.Databricks/workspaces         │ │
│ └─────────────────────────────────────────┘ │
│                                             │
│ ┌─────────────────────────────────────────┐ │
│ │ snet-container                          │ │
│ │ 10.100.2.0/24                           │ │
│ │ Microsoft.Databricks/workspaces         │ │
│ └─────────────────────────────────────────┘ │
└──────────────────────▲──────────────────────┘
                       │ VNet Injection
┌──────────────────────┴──────────────────────┐
│ Azure Databricks                            │
│ Workspace: dbx-icp-vnet                     │
│ Compute: cluster-icp                        │
│ All-purpose compute, Single Node            │
└─────────────────────────────────────────────┘
```

---

## 3. Recursos utilizados

| Recurso | Nombre / valor |
|---|---|
| Suscripción | Patrocinio de Microsoft Azure 10K |
| Subscription ID | `36a971a6-a8e0-4ff8-9ff5-ef9daa8e0fdb` |
| Región | `eastus` |
| Resource Group | `RG_TestFrancok` |
| VNet | `vnet-databricks-icp` |
| VNet CIDR | `10.100.0.0/16` |
| Subnet host | `snet-host` (`10.100.1.0/24`) |
| Subnet container | `snet-container` (`10.100.2.0/24`) |
| NAT Gateway | `nat-icp` |
| Public IP | `pip-icp-nat` |
| IP estática | `20.124.193.245` |
| Workspace | `dbx-icp-vnet` |
| Cluster | `cluster-icp` |
| Secret Scope | `intcomex` |

---

## 4. Prerrequisitos

- Acceso a Azure Portal y Azure Cloud Shell.
- Permisos para crear o actualizar recursos de red.
- Permisos para crear un workspace de Azure Databricks.
- Azure CLI autenticada en la suscripción correcta.
- Credenciales válidas de ICP.
- Repositorio Git para versionar el pipeline.

Seleccionar la suscripción correcta antes de ejecutar comandos:

```bash
az account set --subscription "36a971a6-a8e0-4ff8-9ff5-ef9daa8e0fdb"
az account show --query "{Name:name,Id:id}" -o table
```

---

## 5. Crear la red virtual

```bash
az network vnet create \
  --resource-group RG_TestFrancok \
  --name vnet-databricks-icp \
  --location eastus \
  --address-prefix 10.100.0.0/16
```

---

## 6. Crear las subredes

### 6.1 Subred host

```bash
az network vnet subnet create \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-host \
  --address-prefix 10.100.1.0/24
```

### 6.2 Subred container

```bash
az network vnet subnet create \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-container \
  --address-prefix 10.100.2.0/24
```

---

## 7. Delegar las subredes a Azure Databricks

```bash
az network vnet subnet update \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-host \
  --delegations Microsoft.Databricks/workspaces

az network vnet subnet update \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-container \
  --delegations Microsoft.Databricks/workspaces
```

---

## 8. Crear la IP pública estática

```bash
az network public-ip create \
  --resource-group RG_TestFrancok \
  --name pip-icp-nat \
  --location eastus \
  --sku Standard \
  --allocation-method Static
```

Consultar la IP asignada:

```bash
az network public-ip show \
  --resource-group RG_TestFrancok \
  --name pip-icp-nat \
  --query ipAddress \
  -o tsv
```

Resultado validado:

```text
20.124.193.245
```

---

## 9. Crear el NAT Gateway

```bash
az network nat gateway create \
  --resource-group RG_TestFrancok \
  --name nat-icp \
  --location eastus \
  --public-ip-addresses pip-icp-nat \
  --sku Standard
```

Obtener el identificador del NAT Gateway:

```bash
NAT_ID=$(az network nat gateway show \
  --resource-group RG_TestFrancok \
  --name nat-icp \
  --query id \
  -o tsv)
```

---

## 10. Asociar el NAT Gateway a ambas subredes

```bash
az network vnet subnet update \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-host \
  --nat-gateway "$NAT_ID"

az network vnet subnet update \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-container \
  --nat-gateway "$NAT_ID"
```

> **Importante:** después de crear el workspace, verificar nuevamente la asociación. Durante el despliegue, Azure Databricks puede actualizar las subredes. En esta implementación se requirió reasociar el NAT Gateway después de crear el workspace.

Verificar las asociaciones:

```bash
az network nat gateway show \
  --resource-group RG_TestFrancok \
  --name nat-icp \
  --query "subnets[].id" \
  -o table
```

El resultado debe incluir:

```text
snet-host
snet-container
```

---

## 11. Network Security Group

Inicialmente se creó un NSG manual:

```bash
az network nsg create \
  --resource-group RG_TestFrancok \
  --name nsg-databricks-icp \
  --location eastus
```

Sin embargo, durante la creación del workspace Azure Databricks generó y asoció su propio NSG administrado, con un nombre similar a:

```text
databricksnsgnj4b7zz3al6um
```

Por tanto, se debe validar el NSG que realmente está asociado a las subredes:

```bash
az network vnet subnet show \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-host \
  --query "networkSecurityGroup.id" \
  -o tsv

az network vnet subnet show \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-container \
  --query "networkSecurityGroup.id" \
  -o tsv
```

Listar las reglas salientes del NSG activo:

```bash
az network nsg rule list \
  --resource-group RG_TestFrancok \
  --nsg-name databricksnsgnj4b7zz3al6um \
  --query "[?direction=='Outbound'].{Name:name,Priority:priority,Destination:destinationAddressPrefix,Access:access}" \
  -o table
```

El NSG administrado por Databricks debe contener las reglas requeridas para comunicación con el plano de control, SQL, Storage y Event Hub. Evitar duplicar reglas con prioridades ya utilizadas.

---

## 12. Crear el workspace con VNet Injection

Crear el workspace desde Azure Portal con estos valores:

### Configuración básica

```text
Subscription: Patrocinio de Microsoft Azure 10K
Resource group: RG_TestFrancok
Workspace name: dbx-icp-vnet
Region: East US
Pricing tier: Premium
Workspace type: Hybrid
```

### Networking

```text
Secure Cluster Connectivity / No Public IP: Enabled
Deploy in your own Virtual Network: Yes
Virtual Network: vnet-databricks-icp
Public subnet: snet-host
Public subnet CIDR: 10.100.1.0/24
Private subnet: snet-container
Private subnet CIDR: 10.100.2.0/24
Deploy with NAT Gateway: No
```

Se seleccionó `No` en el NAT integrado porque ya existía `nat-icp` asociado a las subredes.

---

## 13. Validación posterior al despliegue

Ejecutar después de crear el workspace:

```bash
az network vnet subnet show \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-host \
  --query "{NAT:natGateway.id,NSG:networkSecurityGroup.id,Delegation:delegations[0].serviceName,DefaultOutbound:defaultOutboundAccess,RouteTable:routeTable.id}" \
  -o json

az network vnet subnet show \
  --resource-group RG_TestFrancok \
  --vnet-name vnet-databricks-icp \
  --name snet-container \
  --query "{NAT:natGateway.id,NSG:networkSecurityGroup.id,Delegation:delegations[0].serviceName,DefaultOutbound:defaultOutboundAccess,RouteTable:routeTable.id}" \
  -o json
```

Validaciones esperadas:

- `NAT` apunta a `nat-icp`.
- `NSG` apunta al NSG administrado por Databricks.
- `Delegation` es `Microsoft.Databricks/workspaces`.
- `DefaultOutbound` es `false` o `null`, según la respuesta de Azure CLI.
- `RouteTable` es `null` si no se configuró una UDR.

Si el NAT no aparece asociado, repetir el paso 10.

---

## 14. Crear el cluster

En el workspace `dbx-icp-vnet`:

```text
Compute type: All-purpose compute
Compute name: cluster-icp
Cluster mode: Single Node
Photon acceleration: Disabled
Data access mode: Unity Catalog compatible
Auto termination: según política corporativa
```

Para este pipeline de consumo REST, Single Node es suficiente durante desarrollo, porque la carga principal es I/O de red y no procesamiento distribuido intensivo.

> No usar Serverless para este caso si se requiere que el tráfico salga por la VNet y el NAT configurados.

---

## 15. Validar el cluster

En el Event log se debe observar:

```text
RUNNING: Compute is running
DRIVER_HEALTHY: Driver is healthy
```

Errores encontrados durante la implementación:

### `ADD_NODES_FAILED`

El cluster intentaba agregar un worker. Se corrigió configurándolo explícitamente como Single Node.

### `X_NHC_CONTROL_PLANE_UNREACHABLE`

El cluster no podía alcanzar el plano de control por HTTPS. Se revisaron:

- Asociación del NAT Gateway.
- NSG realmente asociado por Databricks.
- Reglas salientes del NSG.
- Ausencia de route table conflictiva.

La causa principal detectada fue que el NAT Gateway no aparecía asociado a las subredes después del despliegue. Se reasoció a `snet-host` y `snet-container`.

---

## 16. Validar la IP de salida desde Databricks

Ejecutar en un notebook unido a `cluster-icp`:

```python
import requests

ip_egress = requests.get(
    "https://api.ipify.org",
    timeout=30
).text

print(f"IP de egress: {ip_egress}")

assert ip_egress == "20.124.193.245", (
    f"IP inesperada: {ip_egress}"
)
```

Resultado validado:

```text
20.124.193.245
```

---

## 17. Probar conectividad con ICP

```python
import requests

response = requests.get(
    "https://marketplacexpe.intcomexcloud.com",
    timeout=30
)

print("HTTP status:", response.status_code)
```

Durante la validación se recibió HTTP `200`, lo que confirmó conectividad de red hacia ICP.

---

## 18. Crear el Secret Scope de ICP

Crear el scope y los secretos desde un notebook:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.secrets.create_scope(scope="intcomex")
    print("Scope intcomex creado")
except Exception as exc:
    print(f"El scope podría existir: {exc}")

w.secrets.put_secret(
    scope="intcomex",
    key="username",
    string_value="USUARIO_ICP"
)

w.secrets.put_secret(
    scope="intcomex",
    key="password",
    string_value="PASSWORD_ICP"
)

print("Secrets cargados")
```

> No escribir credenciales reales en Git, notebooks versionados, README, logs o capturas.

Validar la existencia de las claves:

```python
display(dbutils.secrets.list("intcomex"))
```

Resultado esperado:

```text
username
password
```

Validar sin revelar valores:

```python
username = dbutils.secrets.get("intcomex", "username")
password = dbutils.secrets.get("intcomex", "password")

print("Username length:", len(username))
print("Password length:", len(password))
```

---

## 19. Obtener SessionToken desde ICP

```python
import requests

BASE_URL = (
    "https://marketplacexpe.intcomexcloud.com/"
    "SimpleAPI/SimpleAPIService.svc/rest"
)

response = requests.post(
    f"{BASE_URL}/GetSessionToken",
    headers={
        "Content-Type": "application/json;charset=UTF8",
        "Accept": "application/json"
    },
    json={
        "username": dbutils.secrets.get("intcomex", "username"),
        "password": dbutils.secrets.get("intcomex", "password")
    },
    timeout=60
)

print("HTTP status:", response.status_code)
print(response.text[:500])
```

Si ICP responde con un error genérico de autenticación, validar:

1. Que las credenciales coincidan exactamente con Postman.
2. Que la IP `20.124.193.245` haya sido registrada en el allowlist de ICP.
3. Que la cuenta de integración esté habilitada.
4. Que el formato del request coincida con el request funcional de Postman.

---

## 20. Registro de IP en Intcomex

Solicitar a Intcomex que registre:

```text
20.124.193.245
```

Ejemplo de solicitud:

```text
Asunto: Registro de IP estática para MarketplaceSimpleAPI - DailyTech

Estimado equipo de Intcomex:

Solicitamos registrar en el allowlist de nuestra cuenta de integración la
siguiente IP pública estática, utilizada por Azure Databricks mediante VNet
Injection y NAT Gateway:

20.124.193.245

Agradeceremos confirmar cuando la IP se encuentre habilitada.
```

---

## 21. Versionamiento con Git folder

El código del pipeline debe almacenarse en un Git folder de Databricks.

Estructura sugerida:

```text
Intcomex/
├── README.md
├── .gitignore
├── notebooks/
│   ├── 00_setup_unity_catalog.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_licensing.py
│   ├── 03_gold_reports.py
│   └── 99_diagnostics.py
├── src/
│   └── icp_client.py
└── docs/
    └── runbook_network.md
```

Ramas sugeridas:

```text
main
└── develop
    └── feature/icp-pipeline
```

Nunca almacenar credenciales o SessionTokens en Git.

---

## 22. Script de diagnóstico consolidado

```python
import requests

EXPECTED_IP = "20.124.193.245"
ICP_HOST = "https://marketplacexpe.intcomexcloud.com"

print("=== Diagnóstico ICP ===")

actual_ip = requests.get(
    "https://api.ipify.org",
    timeout=30
).text

print("IP esperada:", EXPECTED_IP)
print("IP real:", actual_ip)
print("IP correcta:", actual_ip == EXPECTED_IP)

response = requests.get(ICP_HOST, timeout=30)
print("ICP HTTP status:", response.status_code)

scope_names = [scope.name for scope in dbutils.secrets.listScopes()]
print("Scope intcomex existe:", "intcomex" in scope_names)

secret_keys = [item.key for item in dbutils.secrets.list("intcomex")]
print("Keys disponibles:", secret_keys)
```

---

## 23. Checklist operativo

### Infraestructura

- [x] VNet creada.
- [x] Dos subredes creadas.
- [x] Subredes delegadas a Azure Databricks.
- [x] Public IP Standard y estática creada.
- [x] NAT Gateway creado.
- [x] NAT asociado a ambas subredes.
- [x] Workspace creado con VNet Injection.
- [x] NSG administrado por Databricks validado.

### Databricks

- [x] Cluster All-purpose creado.
- [x] Cluster configurado como Single Node.
- [x] Cluster en estado Running.
- [x] Driver en estado Healthy.
- [x] IP de egress validada.
- [x] Git folder configurado.
- [x] Secret Scope `intcomex` creado.
- [x] Keys `username` y `password` agregadas.

### ICP

- [x] Endpoint público accesible desde Databricks.
- [ ] IP `20.124.193.245` registrada por Intcomex.
- [ ] `GetSessionToken` validado con HTTP 200.
- [ ] `ExecuteReport` validado.
- [ ] Pipeline Bronze ejecutado.

---

## 24. Buenas prácticas

1. Mantener la infraestructura de red fuera del managed resource group de Databricks.
2. Asociar el NAT a ambas subredes.
3. Volver a validar la asociación del NAT después de crear el workspace.
4. No duplicar reglas con prioridades utilizadas por el NSG administrado por Databricks.
5. Usar Git para todo el código y documentación.
6. Guardar credenciales únicamente en Secret Scopes o un almacén de secretos corporativo.
7. Usar Service Principals para procesos automáticos en producción.
8. Usar Job Compute para ejecuciones programadas y All-purpose Compute solo para desarrollo.
9. Configurar auto-termination para controlar costos.
10. Verificar periódicamente que la IP real coincida con la IP registrada en ICP.
11. Documentar cualquier reasignación de NAT, NSG o subred.
12. No registrar tokens de sesión en logs.

---

## 25. Documentación de referencia

- [Guía de Azure Well-Architected para Azure Databricks](https://learn.microsoft.com/es-mx/azure/well-architected/service-guides/azure-databricks)
- [Azure Databricks en Microsoft Learn](https://learn.microsoft.com/azure/databricks/)
- [Portal de Intcomex Cloud Platform](https://marketplacexpe.intcomexcloud.com/)
- [Swagger de MarketplaceSimpleAPI](https://app.swaggerhub.com/apis/MarketplaceSimpleAPI/MarketplaceSimpleAPI/1.0.0#/Subscription/getsubscriptions)

---

## 26. Resultado final

La implementación permitió que el cluster `cluster-icp`, dentro del workspace `dbx-icp-vnet`, utilizara la VNet `vnet-databricks-icp` y el NAT Gateway `nat-icp` para salir a Internet mediante la dirección IP pública estática:

```text
20.124.193.245
```

La conectividad hacia el portal de ICP fue validada con HTTP `200`. El siguiente hito es registrar la IP en el allowlist de Intcomex y validar `GetSessionToken` y `ExecuteReport`.
