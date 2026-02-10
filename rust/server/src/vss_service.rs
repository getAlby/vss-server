use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::Service;
use hyper::{Request, Response, StatusCode};
use std::collections::HashMap;

use prost::Message;
use tracing::{instrument, Instrument, Span};

use api::auth::Authorizer;
use api::error::VssError;
use api::kv_store::KvStore;
use api::types::{
	DeleteObjectRequest, DeleteObjectResponse, ErrorCode, ErrorResponse, GetObjectRequest,
	GetObjectResponse, ListKeyVersionsRequest, ListKeyVersionsResponse, PutObjectRequest,
	PutObjectResponse,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
pub struct VssService {
	store: Arc<dyn KvStore>,
	authorizer: Arc<dyn Authorizer>,
}

impl VssService {
	pub(crate) fn new(store: Arc<dyn KvStore>, authorizer: Arc<dyn Authorizer>) -> Self {
		Self { store, authorizer }
	}
}

const BASE_PATH_PREFIX: &str = "/vss";

impl Service<Request<Incoming>> for VssService {
	type Response = Response<Full<Bytes>>;
	type Error = hyper::Error;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

	fn call(&self, req: Request<Incoming>) -> Self::Future {
		let store = Arc::clone(&self.store);
		let authorizer = Arc::clone(&self.authorizer);
		let path = req.uri().path().to_owned();
		let method = req.method().to_string();

		let prefix_stripped_path = path.strip_prefix(BASE_PATH_PREFIX).unwrap_or_default().to_owned();

		// Create a root span for the HTTP request
		let span = tracing::info_span!(
			"http.request",
			http.method = %method,
			http.url = %path,
			http.route = %prefix_stripped_path,
			span.type = "web",
			resource.name = %format!("{} {}", method, prefix_stripped_path),
		);

		// Use .instrument(span) instead of span.enter() for async code.
		// span.enter() is not safe across .await points as the future may
		// resume on a different thread, causing span lifecycle issues.
		Box::pin(
			async move {
				match prefix_stripped_path.as_str() {
					"/getObject" => {
						handle_request(store, authorizer, req, "getObject", handle_get_object_request)
							.await
					},
					"/putObjects" => {
						handle_request(store, authorizer, req, "putObjects", handle_put_object_request)
							.await
					},
					"/deleteObject" => {
						handle_request(
							store,
							authorizer,
							req,
							"deleteObject",
							handle_delete_object_request,
						)
						.await
					},
					"/listKeyVersions" => {
						handle_request(
							store,
							authorizer,
							req,
							"listKeyVersions",
							handle_list_object_request,
						)
						.await
					},
					"/testSentry" => {
						// Test endpoint to verify Sentry integration
						handle_test_sentry_request().await
					},
					_ => {
						sentry::capture_message(
							&format!("Invalid request path: {}", prefix_stripped_path),
							sentry::Level::Warning,
						);
						tracing::warn!(http.status_code = 400, "Invalid request path: {}", prefix_stripped_path);
						let error_msg = "Invalid request path.".as_bytes();
						Ok(Response::builder()
							.status(StatusCode::BAD_REQUEST)
							.body(Full::new(Bytes::from(error_msg)))
							.unwrap())
					},
				}
			}
			.instrument(span),
		)
	}
}

#[instrument(
	name = "vss.get_object",
	skip(store, user_token, request),
	fields(
		store_id = %request.store_id,
		key = %request.key,
		span.type = "vss"
	)
)]
async fn handle_get_object_request(
	store: Arc<dyn KvStore>, user_token: String, request: GetObjectRequest,
) -> Result<GetObjectResponse, VssError> {
	store.get(user_token, request).await
}

#[instrument(
	name = "vss.put_objects",
	skip(store, user_token, request),
	fields(
		store_id = %request.store_id,
		transaction_items_count = %request.transaction_items.len(),
		delete_items_count = %request.delete_items.len(),
		span.type = "vss"
	)
)]
async fn handle_put_object_request(
	store: Arc<dyn KvStore>, user_token: String, request: PutObjectRequest,
) -> Result<PutObjectResponse, VssError> {
	store.put(user_token, request).await
}

#[instrument(
	name = "vss.delete_object",
	skip(store, user_token, request),
	fields(
		store_id = %request.store_id,
		span.type = "vss"
	)
)]
async fn handle_delete_object_request(
	store: Arc<dyn KvStore>, user_token: String, request: DeleteObjectRequest,
) -> Result<DeleteObjectResponse, VssError> {
	store.delete(user_token, request).await
}

#[instrument(
	name = "vss.list_key_versions",
	skip(store, user_token, request),
	fields(
		store_id = %request.store_id,
		key_prefix = ?request.key_prefix,
		span.type = "vss"
	)
)]
async fn handle_list_object_request(
	store: Arc<dyn KvStore>, user_token: String, request: ListKeyVersionsRequest,
) -> Result<ListKeyVersionsResponse, VssError> {
	store.list_key_versions(user_token, request).await
}

/// Test endpoint to verify Sentry integration is working.
/// Sends a test error event to Sentry and returns a confirmation message.
async fn handle_test_sentry_request(
) -> Result<<VssService as Service<Request<Incoming>>>::Response, hyper::Error> {
	// Create a test error and capture it
	let test_error =
		std::io::Error::new(std::io::ErrorKind::Other, "Test error from /vss/testSentry endpoint");
	sentry::capture_error(&test_error);

	// Also send a test message
	sentry::capture_message("Test message from /vss/testSentry endpoint", sentry::Level::Warning);

	let response_body = b"Sentry test events sent. Check your Sentry dashboard.";
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Full::new(Bytes::from(response_body.to_vec())))
		.unwrap())
}
async fn handle_request<
	T: Message + Default,
	R: Message,
	F: FnOnce(Arc<dyn KvStore>, String, T) -> Fut + Send + 'static,
	Fut: Future<Output = Result<R, VssError>> + Send,
>(
	store: Arc<dyn KvStore>, authorizer: Arc<dyn Authorizer>, request: Request<Incoming>,
	operation_name: &str, handler: F,
) -> Result<<VssService as Service<Request<Incoming>>>::Response, hyper::Error> {
	let (parts, body) = request.into_parts();
	let headers_map = parts
		.headers
		.iter()
		.map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or_default().to_string()))
		.collect::<HashMap<String, String>>();

	// Create a span for authentication and use .instrument() for async-safety
	let auth_span = tracing::info_span!("auth.verify", span.type = "auth");
	let user_token = match authorizer
		.verify(&headers_map)
		.instrument(auth_span)
		.await
	{
		Ok(auth_response) => {
			tracing::info!("Authentication successful");
			auth_response.user_token
		},
		Err(e) => {
			sentry::capture_message(
				&format!("Authentication failure: {}", e),
				sentry::Level::Warning,
			);
			tracing::warn!(error = %e, "Authentication failure");
			return Ok(build_error_response(e));
		},
	};

	// TODO: we should bound the amount of data we read to avoid allocating too much memory.
	let bytes = body.collect().await?.to_bytes();

	// Record request body size
	Span::current().record("http.request.body.size", bytes.len());

	match T::decode(bytes) {
		Ok(request) => match handler(store.clone(), user_token, request).await {
			Ok(response) => {
				let response_bytes = response.encode_to_vec();
				Span::current().record("http.response.body.size", response_bytes.len());
				Span::current().record("http.status_code", 200);
				tracing::info!(
					http.status_code = 200,
					operation = operation_name,
					"Request completed successfully"
				);
				Ok(Response::builder()
					.body(Full::new(Bytes::from(response_bytes)))
					// unwrap safety: body only errors when previous chained calls failed.
					.unwrap())
			},
			Err(e) => {
				let status_code = get_error_status_code(&e);
				Span::current().record("http.status_code", status_code);
				Span::current().record("error", true);

				match &e {
					VssError::InternalServerError(msg) => {
						sentry::capture_message(
							&format!("Internal server error: {}", msg),
							sentry::Level::Error,
						);
						tracing::error!(error = %e, http.status_code = status_code, "Internal server error");
					},
					VssError::NoSuchKeyError(_) => {
						// NoSuchKeyError is a normal case when a key doesn't exist (404).
						// Don't send these to Sentry as they're expected errors.
						tracing::info!(error = %e, http.status_code = status_code, "Key not found");
					},
					_ => {
						sentry::capture_message(
							&format!("Request error: {}", e),
							sentry::Level::Warning,
						);
						tracing::warn!(error = %e, http.status_code = status_code, "Request error");
					},
				}
				Ok(build_error_response(e))
			},
		},
		Err(e) => {
			sentry::capture_message(
				&format!("Error parsing protobuf request: {}", e),
				sentry::Level::Warning,
			);
			Span::current().record("http.status_code", 400);
			Span::current().record("error", true);
			tracing::warn!(error = %e, http.status_code = 400, "Error parsing protobuf request");
			Ok(Response::builder()
				.status(StatusCode::BAD_REQUEST)
				.body(Full::new(Bytes::from(b"Error parsing request".to_vec())))
				// unwrap safety: body only errors when previous chained calls failed.
				.unwrap())
		},
	}
}

/// Returns the HTTP status code for a given VssError
fn get_error_status_code(e: &VssError) -> u16 {
	match e {
		VssError::NoSuchKeyError(_) => 404,
		VssError::ConflictError(_) => 409,
		VssError::InvalidRequestError(_) => 400,
		VssError::AuthError(_) => 401,
		VssError::InternalServerError(_) => 500,
	}
}

fn build_error_response(e: VssError) -> Response<Full<Bytes>> {
	let (status_code, error_response) = match e {
		VssError::NoSuchKeyError(msg) => {
			let status = StatusCode::NOT_FOUND;
			let error = ErrorResponse {
				error_code: ErrorCode::NoSuchKeyException.into(),
				message: msg.to_string(),
			};
			(status, error)
		},
		VssError::ConflictError(msg) => {
			let status = StatusCode::CONFLICT;
			let error = ErrorResponse {
				error_code: ErrorCode::ConflictException.into(),
				message: msg.to_string(),
			};
			(status, error)
		},
		VssError::InvalidRequestError(msg) => {
			let status = StatusCode::BAD_REQUEST;
			let error = ErrorResponse {
				error_code: ErrorCode::InvalidRequestException.into(),
				message: msg.to_string(),
			};
			(status, error)
		},
		VssError::AuthError(msg) => {
			let status = StatusCode::UNAUTHORIZED;
			let error = ErrorResponse {
				error_code: ErrorCode::AuthException.into(),
				message: msg.to_string(),
			};
			(status, error)
		},
		VssError::InternalServerError(_) => {
			let status = StatusCode::INTERNAL_SERVER_ERROR;
			let error = ErrorResponse {
				error_code: ErrorCode::InternalServerException.into(),
				message: "Unknown Server Error occurred.".to_string(),
			};
			(status, error)
		},
	};
	Response::builder()
		.status(status_code)
		.body(Full::new(Bytes::from(error_response.encode_to_vec())))
		// unwrap safety: body only errors when previous chained calls failed.
		.unwrap()
}
