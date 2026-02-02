use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Config {
	pub(crate) server_config: ServerConfig,
	pub(crate) jwt_auth_config: Option<JwtAuthConfig>,
	pub(crate) postgresql_config: Option<PostgreSQLConfig>,
	pub(crate) sentry_config: Option<SentryConfig>,
	pub(crate) datadog_config: Option<DatadogConfig>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct SentryConfig {
	pub(crate) dsn: Option<String>, // Optional in TOML, can be overridden by env var `SENTRY_DSN`
	pub(crate) environment: Option<String>, // e.g., "production", "staging", "development"
	pub(crate) sample_rate: Option<f32>, // Value between 0.0 and 1.0, defaults to 1.0
}

impl SentryConfig {
	pub(crate) fn get_dsn(&self) -> Option<String> {
		std::env::var("SENTRY_DSN").ok().or_else(|| self.dsn.clone())
	}

	pub(crate) fn get_environment(&self) -> Option<String> {
		std::env::var("SENTRY_ENVIRONMENT").ok().or_else(|| self.environment.clone())
	}

	pub(crate) fn get_sample_rate(&self) -> f32 {
		std::env::var("SENTRY_SAMPLE_RATE")
			.ok()
			.and_then(|s| s.parse().ok())
			.or(self.sample_rate)
			.unwrap_or(1.0)
	}
}

/// Configuration for Datadog APM tracing.
#[derive(Deserialize, Clone)]
pub(crate) struct DatadogConfig {
	/// Whether Datadog tracing is enabled. Defaults to true if config section is present.
	/// Can be overridden by env var `DD_TRACE_ENABLED`
	pub(crate) enabled: Option<bool>,
	/// The service name for traces. Defaults to "vss-server".
	/// Can be overridden by env var `DD_SERVICE`
	pub(crate) service: Option<String>,
	/// The environment name (e.g., "production", "staging", "development").
	/// Can be overridden by env var `DD_ENV`
	pub(crate) env: Option<String>,
	/// The version of the service.
	/// Can be overridden by env var `DD_VERSION`
	pub(crate) version: Option<String>,
	/// The Datadog Agent host. Defaults to "localhost".
	/// Can be overridden by env var `DD_AGENT_HOST`
	pub(crate) agent_host: Option<String>,
	/// The Datadog Agent trace port. Defaults to 8126.
	/// Can be overridden by env var `DD_TRACE_AGENT_PORT`
	pub(crate) agent_port: Option<u16>,
}

impl DatadogConfig {
	pub(crate) fn is_enabled(&self) -> bool {
		std::env::var("DD_TRACE_ENABLED")
			.ok()
			.and_then(|s| s.parse().ok())
			.or(self.enabled)
			.unwrap_or(true)
	}

	pub(crate) fn get_service(&self) -> String {
		std::env::var("DD_SERVICE")
			.ok()
			.or_else(|| self.service.clone())
			.unwrap_or_else(|| "vss-server".to_string())
	}

	pub(crate) fn get_env(&self) -> Option<String> {
		std::env::var("DD_ENV").ok().or_else(|| self.env.clone())
	}

	pub(crate) fn get_version(&self) -> Option<String> {
		std::env::var("DD_VERSION").ok().or_else(|| self.version.clone())
	}

	pub(crate) fn get_agent_host(&self) -> String {
		std::env::var("DD_AGENT_HOST")
			.ok()
			.or_else(|| self.agent_host.clone())
			.unwrap_or_else(|| "localhost".to_string())
	}

	pub(crate) fn get_agent_port(&self) -> u16 {
		std::env::var("DD_TRACE_AGENT_PORT")
			.ok()
			.and_then(|s| s.parse().ok())
			.or(self.agent_port)
			.unwrap_or(8126)
	}
}

impl Default for DatadogConfig {
	fn default() -> Self {
		Self {
			enabled: Some(true),
			service: Some("vss-server".to_string()),
			env: None,
			version: None,
			agent_host: Some("localhost".to_string()),
			agent_port: Some(8126),
		}
	}
}

#[derive(Deserialize)]
pub(crate) struct ServerConfig {
	pub(crate) host: String,
	pub(crate) port: u16,
}

#[derive(Deserialize)]
pub(crate) struct JwtAuthConfig {
	pub(crate) rsa_pem: String,
}

#[derive(Deserialize)]
pub(crate) struct PostgreSQLConfig {
	pub(crate) username: Option<String>, // Optional in TOML, can be overridden by env
	pub(crate) password: Option<String>, // Optional in TOML, can be overridden by env
	pub(crate) host: String,
	pub(crate) port: u16,
	pub(crate) database: String,
	pub(crate) tls: Option<TlsConfig>,
}

#[derive(Deserialize)]
pub(crate) struct TlsConfig {
	pub(crate) ca_file: Option<String>,
}

impl PostgreSQLConfig {
	pub(crate) fn to_postgresql_endpoint(&self) -> String {
		let username_env = std::env::var("VSS_POSTGRESQL_USERNAME");
		let username = username_env.as_ref()
			.ok()
			.or_else(|| self.username.as_ref())
			.expect("PostgreSQL database username must be provided in config or env var VSS_POSTGRESQL_USERNAME must be set.");
		let password_env = std::env::var("VSS_POSTGRESQL_PASSWORD");
		let password = password_env.as_ref()
			.ok()
			.or_else(|| self.password.as_ref())
			.expect("PostgreSQL database password must be provided in config or env var VSS_POSTGRESQL_PASSWORD must be set.");

		format!("postgresql://{}:{}@{}:{}", username, password, self.host, self.port)
	}
}

pub(crate) fn load_config(config_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
	let config_str = std::fs::read_to_string(config_path)?;
	let config: Config = toml::from_str(&config_str)?;
	Ok(config)
}
