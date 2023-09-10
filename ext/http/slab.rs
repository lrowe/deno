// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.
use crate::request_properties::HttpConnectionProperties;
use crate::response_body::CompletionHandle;
use crate::response_body::ResponseBytes;
use deno_core::error::AnyError;
use deno_core::OpState;
use deno_core::ResourceId;
use http::request::Parts;
use http::HeaderMap;
use hyper1::body::Incoming;
use hyper1::upgrade::OnUpgrade;
use tokio::sync::oneshot;

use once_cell::sync::Lazy;
use send_wrapper::SendWrapper;
use sharded_slab::Config;
use sharded_slab::OwnedEntry;
use sharded_slab::Slab;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

pub type Request = hyper1::Request<Incoming>;
pub type Response = hyper1::Response<ResponseBytes>;
pub type SlabId = usize;

enum RequestBodyState {
  Incoming(Incoming),
  Resource(HttpRequestBodyAutocloser),
}

impl From<Incoming> for RequestBodyState {
  fn from(value: Incoming) -> Self {
    RequestBodyState::Incoming(value)
  }
}

/// Ensures that the request body closes itself when no longer needed.
pub struct HttpRequestBodyAutocloser(Option<oneshot::Sender<()>>);

impl HttpRequestBodyAutocloser {
  pub fn new(res: ResourceId, op_state: Rc<RefCell<OpState>>) -> Self {
    let (send, recv) = oneshot::channel::<()>();
    // spawn requires send future but it stays on same thread.
    // XXX could we use https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html?
    let wrapped_op_state = SendWrapper::new(op_state);
    tokio::spawn(async move {
      _ = recv.await;
      _ = wrapped_op_state.borrow_mut().resource_table.close(res);
    });
    Self(Some(send))
  }
}

impl Drop for HttpRequestBodyAutocloser {
  fn drop(&mut self) {
    if let Some(sender) = self.0.take() {
      _ = sender.send(());
    }
  }
}

pub struct HttpSlabRecord {
  request_info: HttpConnectionProperties,
  request_parts: Parts,
  request_body: Option<RequestBodyState>,
  // The response may get taken before we tear this down
  response: Option<Response>,
  promise: CompletionHandle,
  trailers: Arc<RwLock<Option<HeaderMap>>>,
  been_dropped: bool,
  #[cfg(feature = "__zombie_http_tracking")]
  alive: bool,
}

struct CustomConfig;
impl Config for CustomConfig {
  const INITIAL_PAGE_SIZE: usize = 1024;
}

static SLAB: Lazy<Arc<Slab<RwLock<HttpSlabRecord>, CustomConfig>>> =
  Lazy::new(|| {
    let slab = Arc::new(Slab::new_with_config::<CustomConfig>());
    let index = slab.insert(
      // SAFETY:
      // This value is never used an simply a placeholder to reserve index 0.
      unsafe {
        std::mem::transmute(
          [0_u8; std::mem::size_of::<RwLock<HttpSlabRecord>>()],
        )
      },
    );
    assert_eq!(index, Some(0));
    slab
  });

macro_rules! http_trace {
  ($index:expr, $args:tt) => {
    #[cfg(feature = "__http_tracing")]
    {
      println!("HTTP id={} total=?: {}", $index, format!($args),);
    }
  };
}

/// Hold a lock on the slab table and a reference to one entry in the table.
pub struct SlabEntry(OwnedEntry<RwLock<HttpSlabRecord>, CustomConfig>);

pub fn slab_get(index: SlabId) -> SlabEntry {
  assert_ne!(index, 0);
  http_trace!(index, "slab_get");
  let Some(entry) = SLAB.clone().get_owned(index) else {
    panic!(
      "HTTP state error: Attempted to access invalid request {}",
      index
    )
  };
  #[cfg(feature = "__zombie_http_tracking")]
  {
    assert!(
      entry.read().unwrap().alive,
      "HTTP state error: Entry is not alive"
    );
  }
  SlabEntry(entry)
}

#[allow(clippy::let_and_return)]
fn slab_insert_raw(
  request_parts: Parts,
  request_body: Option<Incoming>,
  request_info: HttpConnectionProperties,
) -> SlabId {
  let index = {
    let body = ResponseBytes::default();
    let trailers = body.trailers();
    let request_body = request_body.map(|r| r.into());
    SLAB
      .clone()
      .insert(RwLock::new(HttpSlabRecord {
        request_info,
        request_parts,
        request_body,
        response: Some(Response::new(body)),
        trailers,
        been_dropped: false,
        promise: CompletionHandle::default(),
        #[cfg(feature = "__zombie_http_tracking")]
        alive: true,
      }))
      .unwrap()
  };
  assert_ne!(index, 0);
  http_trace!(index, "slab_insert");
  index
}

pub fn slab_insert(
  request: Request,
  request_info: HttpConnectionProperties,
) -> SlabId {
  let (request_parts, request_body) = request.into_parts();
  slab_insert_raw(request_parts, Some(request_body), request_info)
}

pub fn slab_drop(index: SlabId) {
  assert_ne!(index, 0);
  http_trace!(index, "slab_drop");
  let SlabEntry(entry) = slab_get(index);
  let mut record = entry.write().unwrap();
  assert!(
    !record.been_dropped,
    "HTTP state error: Entry has already been dropped"
  );
  record.been_dropped = true;
  if record.promise.is_completed() {
    drop(record);
    slab_expunge(index);
  }
}

fn slab_expunge(index: SlabId) {
  assert_ne!(index, 0);
  #[cfg(__zombie_http_tracking)]
  {
    let entry = SLAB.get(index).unwrap();
    let mut value = entry.get_mut().unwrap();
    value.alive = false;
  }
  #[cfg(not(__zombie_http_tracking))]
  {
    SLAB.remove(index);
  }
  http_trace!(index, "slab_expunge");
}

impl SlabEntry {
  /// Perform the Hyper upgrade on this entry.
  pub fn upgrade(&mut self) -> Result<OnUpgrade, AnyError> {
    // Manually perform the upgrade. We're peeking into hyper's underlying machinery here a bit
    let mut entry = self.0.write().unwrap();
    entry
      .request_parts
      .extensions
      .remove::<OnUpgrade>()
      .ok_or_else(|| AnyError::msg("upgrade unavailable"))
  }

  /// Take the Hyper body from this entry.
  pub fn take_body(&mut self) -> Option<Incoming> {
    let mut entry = self.0.write().unwrap();
    let body_holder = &mut entry.request_body;
    let body = body_holder.take();
    match body {
      Some(RequestBodyState::Incoming(body)) => Some(body),
      x => {
        *body_holder = x;
        None
      }
    }
  }

  pub fn take_resource(&mut self) -> Option<HttpRequestBodyAutocloser> {
    let mut entry = self.0.write().unwrap();
    let body_holder = &mut entry.request_body;
    let body = body_holder.take();
    match body {
      Some(RequestBodyState::Resource(res)) => Some(res),
      x => {
        *body_holder = x;
        None
      }
    }
  }

  /// Replace the request body with a resource ID and the OpState we'll need to shut it down.
  /// We cannot keep just the resource itself, as JS code might be reading from the resource ID
  /// to generate the response data (requiring us to keep it in the resource table).
  pub fn put_resource(&mut self, res: HttpRequestBodyAutocloser) {
    let mut entry = self.0.write().unwrap();
    entry.request_body = Some(RequestBodyState::Resource(res));
  }

  /// Complete this entry, potentially expunging it if it is fully complete (ie: dropped as well).
  pub fn complete(self) {
    let entry = self.0.read().unwrap();
    assert!(
      !entry.promise.is_completed(),
      "HTTP state error: Entry has already been completed"
    );
    http_trace!(self.0.key(), "SlabEntry::complete");
    entry.promise.complete(true);
    // If we're all done, we need to drop ourself to release the lock before we expunge this record
    if entry.been_dropped {
      let index = self.0.key();
      drop(entry);
      slab_expunge(index);
    }
  }

  /// Has the future for this entry been dropped? ie, has the underlying TCP connection
  /// been closed?
  pub fn cancelled(&self) -> bool {
    let entry = self.0.read().unwrap();
    entry.been_dropped
  }

  /// Get a mutable reference to the response.
  pub fn with_response<F>(&mut self, func: F)
  where
    F: FnOnce(&mut Response),
  {
    let mut entry = self.0.write().unwrap();
    func(entry.response.as_mut().unwrap());
  }

  /// Get a mutable reference to the trailers.
  pub fn with_trailers<F>(&mut self, func: F)
  where
    F: FnOnce(&RwLock<Option<HeaderMap>>),
  {
    let entry = self.0.write().unwrap();
    func(&entry.trailers);
  }

  /// Take the response.
  pub fn take_response(&mut self) -> Response {
    let mut entry = self.0.write().unwrap();
    entry.response.take().unwrap()
  }

  /// Get a reference to the connection properties.
  pub fn with_request_info<F, T>(&self, func: F) -> T
  where
    F: FnOnce(&HttpConnectionProperties) -> T,
  {
    let entry = self.0.read().unwrap();
    func(&entry.request_info)
  }

  /// Get a reference to the request parts.
  pub fn with_request_parts<F, T>(&self, func: F) -> T
  where
    F: FnOnce(&Parts) -> T,
  {
    let entry = self.0.read().unwrap();
    func(&entry.request_parts)
  }

  /// Get a reference to the completion handle.
  pub fn promise(&self) -> CompletionHandle {
    let entry = self.0.read().unwrap();
    entry.promise.clone()
  }

  /// Get a reference to the response body completion handle.
  pub fn body_promise(&self) -> CompletionHandle {
    let entry = self.0.read().unwrap();
    entry.response.as_ref().unwrap().body().completion_handle()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use deno_net::raw::NetworkStreamType;
  use http::Request;

  #[test]
  fn test_slab() {
    let req = Request::builder().body(()).unwrap();
    let (parts, _) = req.into_parts();
    let id = slab_insert_raw(
      parts,
      None,
      HttpConnectionProperties {
        peer_address: "".into(),
        peer_port: None,
        scheme: "",
        fallback_host: "".into(),
        local_port: None,
        stream_type: NetworkStreamType::Tcp,
      },
    );
    let entry = slab_get(id);
    entry.complete();
    slab_drop(id);
  }
}
