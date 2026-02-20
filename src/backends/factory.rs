use super::{
    dblab::DBLabBackend, local::LocalBackend, neon::NeonBackend,
    postgres_template::PostgresTemplateBackend, xata::XataBackend, DatabaseBranchingBackend,
};
use crate::config::{Config, NamedBackendConfig};
use anyhow::{Context, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackendType {
    Local,
    PostgresTemplate,
    Neon,
    DBLab,
    Xata,
}

impl BackendType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "local" | "docker" => Ok(BackendType::Local),
            "postgres_template" | "postgres" | "postgresql" => Ok(BackendType::PostgresTemplate),
            "neon" => Ok(BackendType::Neon),
            "dblab" | "database_lab" => Ok(BackendType::DBLab),
            "xata" | "xata_lite" => Ok(BackendType::Xata),
            _ => anyhow::bail!("Unknown backend type: {}. Valid types: local, postgres_template, neon, dblab, xata", s),
        }
    }

    pub fn is_local(s: &str) -> bool {
        matches!(s.to_lowercase().as_str(), "local" | "docker")
    }
}

pub struct NamedBackend {
    pub name: String,
    pub backend: Box<dyn DatabaseBranchingBackend>,
}

/// Create a backend from a NamedBackendConfig.
pub async fn create_backend_from_named_config(
    config: &Config,
    named: &NamedBackendConfig,
) -> Result<Box<dyn DatabaseBranchingBackend>> {
    let backend_type = BackendType::from_str(&named.backend_type)?;

    match backend_type {
        BackendType::Local => {
            let local_config = named.local.as_ref();
            let backend = LocalBackend::new(&named.name, config, local_config)
                .await
                .context("Failed to create local backend")?;
            Ok(Box::new(backend))
        }
        BackendType::PostgresTemplate => {
            let backend = PostgresTemplateBackend::new(config)
                .await
                .context("Failed to create PostgreSQL template backend")?;
            Ok(Box::new(backend))
        }
        BackendType::Neon => {
            if let Some(ref neon_config) = named.neon {
                let backend = NeonBackend::new(
                    resolve_env_var(&neon_config.api_key)?,
                    resolve_env_var(&neon_config.project_id)?,
                    Some(neon_config.base_url.clone()),
                )?;
                Ok(Box::new(backend))
            } else {
                anyhow::bail!("Neon backend selected but no neon configuration provided");
            }
        }
        BackendType::DBLab => {
            if let Some(ref dblab_config) = named.dblab {
                let backend = DBLabBackend::new(
                    resolve_env_var(&dblab_config.api_url)?,
                    resolve_env_var(&dblab_config.auth_token)?,
                )?;
                Ok(Box::new(backend))
            } else {
                anyhow::bail!("DBLab backend selected but no dblab configuration provided");
            }
        }
        BackendType::Xata => {
            if let Some(ref xata_config) = named.xata {
                let backend = XataBackend::new(
                    resolve_env_var(&xata_config.api_key)?,
                    resolve_env_var(&xata_config.organization_id)?,
                    resolve_env_var(&xata_config.project_id)?,
                    Some(xata_config.base_url.clone()),
                )?;
                Ok(Box::new(backend))
            } else {
                anyhow::bail!("Xata backend selected but no xata configuration provided");
            }
        }
    }
}

/// Resolve a single backend by name (or the default).
pub async fn resolve_backend(config: &Config, backend_name: Option<&str>) -> Result<NamedBackend> {
    config.validate_backends()?;

    let backends = config.resolve_backends();

    // If backends list is populated, use it
    if !backends.is_empty() {
        let named = if let Some(name) = backend_name {
            backends
                .iter()
                .find(|b| b.name == name)
                .ok_or_else(|| anyhow::anyhow!("Backend '{}' not found in configuration", name))?
        } else {
            backends
                .iter()
                .find(|b| b.default)
                .or(backends.first())
                .ok_or_else(|| anyhow::anyhow!("No backends configured"))?
        };

        let backend = create_backend_from_named_config(config, named).await?;
        return Ok(NamedBackend {
            name: named.name.clone(),
            backend,
        });
    }

    // No backends or backend config — fall back to auto-detection
    if backend_name.is_some() {
        anyhow::bail!("--database specified but no backends configured");
    }

    let backend = create_backend_default(config).await?;
    Ok(NamedBackend {
        name: "default".to_string(),
        backend,
    })
}

/// Instantiate all configured backends.
pub async fn create_all_backends(config: &Config) -> Result<Vec<NamedBackend>> {
    config.validate_backends()?;

    let named_configs = config.resolve_backends();

    if named_configs.is_empty() {
        // Fall back to default auto-detection
        let backend = create_backend_default(config).await?;
        return Ok(vec![NamedBackend {
            name: "default".to_string(),
            backend,
        }]);
    }

    let mut result = Vec::with_capacity(named_configs.len());
    for named in &named_configs {
        let backend = create_backend_from_named_config(config, named).await?;
        result.push(NamedBackend {
            name: named.name.clone(),
            backend,
        });
    }

    Ok(result)
}

/// Auto-detect backend when no config section is present.
async fn create_backend_default(config: &Config) -> Result<Box<dyn DatabaseBranchingBackend>> {
    // Backward compatibility: if database config differs from defaults,
    // use postgres_template backend
    if config.database.host != "localhost"
        || config.database.port != 5432
        || config.database.template_database != "template0"
    {
        let backend = PostgresTemplateBackend::new(config)
            .await
            .context("Failed to create PostgreSQL template backend")?;
        return Ok(Box::new(backend));
    }

    // Default to local backend — derive name from cwd for backward compatibility
    let default_name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .unwrap_or_else(|| "default".to_string());
    let backend = LocalBackend::new(&default_name, config, None)
        .await
        .context("Failed to create local backend")?;
    Ok(Box::new(backend))
}

fn resolve_env_var(value: &str) -> Result<String> {
    if value.starts_with("${") && value.ends_with('}') {
        let env_var = &value[2..value.len() - 1];
        std::env::var(env_var)
            .with_context(|| format!("Environment variable {} not found", env_var))
    } else {
        Ok(value.to_string())
    }
}
