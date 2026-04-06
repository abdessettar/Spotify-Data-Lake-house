# Infrastructure Documentation

For the infrastructure, we use Terraform to deploy on Microsoft Azure. All resources live under the `infra/` directory, organized into a root module and four child modules.

```
infra/
├── main.tf / providers.tf / variables.tf / outputs.tf
├── terraform.tfvars.example
└── modules/
    ├── storage/      # Azure Data Lake Storage Gen2
    ├── registry/     # Azure Container Registry
    ├── identity/     # Managed Identity & Key Vault
    └── compute/      # Container Apps Jobs
```

---

## Root Module

The root module (`infra/main.tf`) creates a single Resource Group (`rg-spotify-de`) and wires together the four child modules.

### Multi-Region Design

| Region | Resources |
|---|---|
| **West Europe** | Storage, Container Registry, Managed Identity, Key Vault |
| **North Europe** | Container Apps Environment & Jobs |

Compute runs in a separate region because of Container Apps capacity constraints in West Europe when setting up our infra. This is an intentional workaround, not an availability pattern.

---

## Modules

### 1. Storage (`modules/storage/`)

**Resources created:**
- **Azure Storage Account**: Standard LRS, StorageV2, with hierarchical namespace (HNS) enabled to act as a Data Lake Gen2 store.
- **Blob Container**: `spotify-data`, private access.

**Why this was chosen:**
- HNS-enabled Storage Gen2 provides a true data lake with directory level operations and fine-grained ACLs, while remaining compatible with standard Blob APIs.
- Standard LRS (locally redundant) keeps costs low for a non-mission-critical analytics workload.
- TLS 1.2 minimum and a 7days soft-delete retention policy are enabled for baseline security and accidental deletion protection.

---

### 2. Registry (`modules/registry/`)

**Resources created:**
- **Azure Container Registry (ACR):** Basic SKU, admin access disabled.

**Why:**
- ACR Basic is the cheapest tier and sufficient for a single image pipeline. It integrates natively with Container Apps via managed identity (AcrPull role), so no Docker Hub credentials or image pull secrets are needed.
- Admin access is disabled in favor of RBAC-based authentication, following Azure security best practices.

---

### 3. Identity (`modules/identity/`)

**Resources created:**
- **User-Assigned Managed Identity** — `id-spotify-de-prod`.
- **RBAC Role Assignments:**
  - *Storage Blob Data Contributor* scoped to the storage account, so the pipeline can read/write lakehouse data.
  - *AcrPull* scoped to the container registry, so Container Apps can pull images.
- **Azure Key Vault**: Standard SKU.
- **Key Vault Access Policies:**
  - The managed identity gets `Get` and `List` on secrets (read-only at runtime).
  - The Terraform operator gets `Get`, `List`, `Set`, `Delete`, `Purge` (full lifecycle management).

**Benefits:**
- A `User-Assigned Managed Identity` was chosen over a system-assigned one because it decouples identity lifecycle from any single compute resource. Both the ingest and transform jobs share the same identity.
- `Key Vault` stores Spotify API credentials (`SPOTIFY-CLIENT-ID`, `SPOTIFY-CLIENT-SECRET`, `SPOTIFY-REFRESH-TOKEN`). These are added manually after `terraform apply` via the Azure CLI, keeping secrets out of Terraform state entirely.
- RBAC scoping follows the principle of least privilege: the identity only has access to the specific storage account and registry it needs.

---

### 4. Compute (`modules/compute/`)

**Resources created:**
- **Log Analytics Workspace**: 30-day retention, PerGB2018 pricing.
- **Container Apps Environment:** Consumption only workload profile.
- **Two Container Apps Jobs:**

| Job | Schedule (UTC) | Timeout | Resources | Command |
|---|---|---|---|---|
| **Ingest** | `:05` every hour | 30 min | 1 CPU / 2 Gi | `python -m src.main ingest` |
| **Transform** | `:20` every hour | 90 min | 2 CPU / 4 Gi | `python -m src.main transform-gold` |

**Why this was chosen:**
- **Azure Container Apps Jobs** are purposedly built for scheduled and event-driven batch workloads. Compared to Azure Functions (language/runtime constraints) or AKS (operational overhead), Container Apps Jobs offer a Docker native, serverless, cron scheduled execution model with zero cluster management.
- The Consumption workload profile means there are no idle costs as containers only run (and bill) during their scheduled windows.
- `Ingest` runs at `:05` of every hour to fetch the latest Spotify listening data, then `Transform` runs at `:20` to process the Bronze layer into Silver and Gold, giving ingest enough lead time to complete.
- The transform job gets more CPU and memory (2 CPU & 4 Gi vs 1 CPU é 2 Gi) because it rebuilds the full Silver star schema and then derives Gold aggregations.
- **Log Analytics** provides centralized logging and is required by the Container Apps Environment. The 30 days retention window balances debuggability with cost.

Both jobs receive their configuration via environment variables (`AZURE_STORAGE_ACCOUNT`, `DATA_CONTAINER`, `KEY_VAULT_URI`, `AZURE_CLIENT_ID`, `ENVIRONMENT`) and authenticate to Azure services using the shared managed identity.

---

## Security Concerns

| Concern | Approach |
|---|---|
| Workload authentication | User-Assigned Managed Identity: no credentials in code or environment variables |
| Spotify API secrets | Azure Key Vault, manually provisioned, read-only access at runtime |
| Image pulls | RBAC (AcrPull role): no admin credentials or image-pull secrets |
| Data access | RBAC (Storage Blob Data Contributor) scoped to the single storage account |
| Network | Private blob containers, TLS 1.2 minimum on storage |

---

## Deployment Quick Reference

```bash
# 1. Initialize and apply infrastructure
cd infra
cp terraform.tfvars.example terraform.tfvars   # edit with real values
terraform init
terraform plan
terraform apply

# 2. Manually add Spotify secrets to Key Vault:
az keyvault secret set --vault-name kv-spotify-de-prod --name SPOTIFY-CLIENT-ID --value "..."
az keyvault secret set --vault-name kv-spotify-de-prod --name SPOTIFY-CLIENT-SECRET --value "..."
az keyvault secret set --vault-name kv-spotify-de-prod --name SPOTIFY-REFRESH-TOKEN --value "..."

# 3. Build and push the container image:
az acr login --name crspotifydeprod
docker build -t crspotifydeprod.azurecr.io/spotify-data:latest .
docker push crspotifydeprod.azurecr.io/spotify-data:latest
```
