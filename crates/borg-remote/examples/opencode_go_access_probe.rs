//! Explicit live Go catalog/readiness probe; no model requests or credential writes.
use anyhow::{Context, Result, ensure};
use borg_remote::{
    CodingProvider, probe_provider_admission_capabilities, probe_provider_capabilities,
};

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        for (label, capabilities) in [
            ("admission", probe_provider_admission_capabilities().await),
            ("detailed", probe_provider_capabilities().await),
        ] {
            let go = capabilities
                .iter()
                .find(|c| c.provider == CodingProvider::OpenCode)
                .context("OpenCode capability missing")?;
            ensure!(
                go.installed && go.authenticated && go.can_spawn,
                "Go {label} not ready"
            );
            ensure!(
                go.version.is_none(),
                "Go {label} depended on an external runtime"
            );
            ensure!(
                serde_json::to_value(go)?["billing"] == "subscription",
                "Go billing changed"
            );
            println!("PASS: native Go {label}, subscription billing");
        }
        let models = borg_provider::runtime::refresh_opencode_go_model_catalog().await?;
        ensure!(!models.is_empty(), "empty Go catalog");
        for model in &models {
            ensure!(
                model.id.starts_with("opencode-go/"),
                "foreign catalog route"
            );
            println!("{}", model.id);
        }
        println!("PASS: native Go catalog, {} models", models.len());
        Ok(())
    })
    .await
    .context("Go access probe timed out")?
}
