//! Hosts VSS http-server implementation.
//!
//! VSS is an open-source project designed to offer a server-side cloud storage solution specifically
//! tailored for noncustodial Lightning supporting mobile wallets. Its primary objective is to
//! simplify the development process for Lightning wallets by providing a secure means to store
//! and manage the essential state required for Lightning Network (LN) operations.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]
#![deny(missing_docs)]

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal::unix::SignalKind;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;

use log::{error, info, warn};

use api::auth::Authorizer;
#[cfg(noop_authorizer)]
use api::auth::NoopAuthorizer;
use api::kv_store::KvStore;
#[cfg(feature = "jwt")]
use auth_impls::jwt::JWTAuthorizer;
#[cfg(feature = "sigs")]
use auth_impls::signature::SignatureValidatingAuthorizer;
use impls::postgres_store::{PostgresPlaintextBackend, PostgresTlsBackend};
use util::logger::ServerLogger;
use vss_service::VssService;

mod util;
mod vss_service;

fn main() {
	let args: Vec<String> = std::env::args().collect();
	if args.len() != 2 {
		eprintln!("Usage: {} <config-file-path>", args[0]);
		std::process::exit(1);
	}

	let config = match util::config::load_config(&args[1]) {
		Ok(cfg) => cfg,
		Err(e) => {
			eprintln!("Failed to load configuration: {}", e);
			std::process::exit(1);
		},
	};

	// Initialize Sentry before the tokio runtime to ensure proper Hub inheritance
	// for spawned threads. The guard must be kept alive for the duration of the program.
	let _sentry_guard = initialize_sentry(&config.sentry_config);

	let Config {
		server_config: ServerConfig { host, port },
		jwt_auth_config,
		postgresql_config,
		..
	} = config;

	let addr: SocketAddr = match format!("{}:{}", host, port).parse() {
		Ok(addr) => addr,
		Err(e) => {
			eprintln!("Failed to initialize logger: {e}");
			std::process::exit(-1);
		},
	};

	let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
		Ok(runtime) => Arc::new(runtime),
		Err(e) => {
			error!("Failed to setup tokio runtime: {}", e);
			std::process::exit(-1);
		},
	};

	runtime.block_on(async {
		// Register SIGHUP handler for log rotation
		let mut sighup_stream = match tokio::signal::unix::signal(SignalKind::hangup()) {
			Ok(stream) => stream,
			Err(e) => {
				error!("Failed to register SIGHUP handler: {e}");
				std::process::exit(-1);
			}
		};

		let mut sigterm_stream = match tokio::signal::unix::signal(SignalKind::terminate()) {
			Ok(stream) => stream,
			Err(e) => {
				error!("Failed to register for SIGTERM stream: {}", e);
				std::process::exit(-1);
			},
		};

		let mut authorizer: Option<Arc<dyn Authorizer>> = None;
		#[cfg(feature = "jwt")]
		{
			if let Some(rsa_pem) = config.rsa_pem {
				authorizer = match JWTAuthorizer::new(&rsa_pem).await {
					Ok(auth) => {
						info!("Configured JWT authorizer with RSA public key");
						Some(Arc::new(auth))
					},
					Err(e) => {
						error!("Failed to configure JWT authorizer: {}", e);
						std::process::exit(-1);
					},
				};
			}
		}
		#[cfg(feature = "sigs")]
		{
			if authorizer.is_none() {
				info!("Configured signature-validating authorizer");
				authorizer = Some(Arc::new(SignatureValidatingAuthorizer));
			}
		}

		#[cfg(noop_authorizer)]
		let authorizer = if let Some(auth) = authorizer {
			auth
		} else {
			warn!("No authentication method configured, all storage with the same store id will be commingled.");
			Arc::new(NoopAuthorizer {})
		};

		#[cfg(not(noop_authorizer))]
		let authorizer = authorizer.unwrap_or_else(||  {
			error!("No authentication method configured, please configure either `JWTAuthorizer` or `SignatureValidatingAuthorizer`");
			std::process::exit(-1);
		});

		let store: Arc<dyn KvStore> = if let Some(crt_pem) = config.tls_config {
			let postgres_tls_backend = PostgresTlsBackend::new(
				&config.postgresql_prefix,
				&config.default_db,
				&config.vss_db,
				crt_pem.as_deref(),
			)
			.await
			.unwrap_or_else(|e| {
				error!("Failed to start postgres TLS backend: {}", e);
				std::process::exit(-1);
			});
			info!(
				"Connected to PostgreSQL TLS backend with DSN: {}/{}",
				config.postgresql_prefix, config.vss_db
			);
			Arc::new(postgres_tls_backend)
		} else {
			let postgres_plaintext_backend = PostgresPlaintextBackend::new(
				&config.postgresql_prefix,
				&config.default_db,
				&config.vss_db,
			)
			.await
			.unwrap_or_else(|e| {
				error!("Failed to start postgres plaintext backend: {}", e);
				std::process::exit(-1);
			});
			info!(
				"Connected to PostgreSQL plaintext backend with DSN: {}/{}",
				config.postgresql_prefix, config.vss_db
			);
			Arc::new(postgres_plaintext_backend)
		};

		let rest_svc_listener = TcpListener::bind(&config.bind_address).await.unwrap_or_else(|e| {
			error!("Failed to bind listening port: {}", e);
			std::process::exit(-1);
		});
		info!("Listening for incoming connections on {}{}", config.bind_address, crate::vss_service::BASE_PATH_PREFIX);

		loop {
			tokio::select! {
				res = rest_svc_listener.accept() => {
					match res {
						Ok((stream, _)) => {
							let io_stream = TokioIo::new(stream);
							let vss_service = VssService::new(Arc::clone(&store), Arc::clone(&authorizer));
							runtime.spawn(async move {
								if let Err(err) = http1::Builder::new().serve_connection(io_stream, vss_service).await {
									warn!("Failed to serve connection: {}", err);
								}
							});
						},
						Err(e) => warn!("Failed to accept connection: {}", e),
					}
				}
				_ = tokio::signal::ctrl_c() => {
					info!("Received CTRL-C, shutting down..");
					break;
				}
				_ = sighup_stream.recv() => {
					if let Err(e) = logger.reopen() {
						error!("Failed to reopen log file on SIGHUP: {e}");
					}
				}
				_ = sigterm_stream.recv() => {
					info!("Received SIGTERM, shutting down..");
					break;
				}
			}
		}
	});
}

/// Initializes Sentry error tracking if configured.
///
/// Sentry must be initialized before the tokio runtime starts to ensure proper
/// Hub inheritance for spawned threads. Returns a guard that must be kept alive
/// for the duration of the program to ensure events are flushed on shutdown.
fn initialize_sentry(
	sentry_config: &Option<util::config::SentryConfig>,
) -> Option<sentry::ClientInitGuard> {
	let config = match sentry_config {
		Some(cfg) => cfg,
		None => return None,
	};

	let dsn = match config.get_dsn() {
		Some(dsn) if !dsn.is_empty() => dsn,
		_ => return None,
	};

	let environment = config.get_environment();
	let sample_rate = config.get_sample_rate();

	let guard = sentry::init((
		dsn,
		sentry::ClientOptions {
			release: sentry::release_name!(),
			environment: environment.map(std::borrow::Cow::Owned),
			sample_rate,
			..Default::default()
		},
	));

	if guard.is_enabled() {
		println!(
			"Sentry initialized (environment: {}, sample_rate: {})",
			config.get_environment().unwrap_or_else(|| "default".to_string()),
			sample_rate
		);
	}

	Some(guard)
}
