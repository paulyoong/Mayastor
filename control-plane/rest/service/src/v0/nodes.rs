use super::*;
use crate::authentication::authenticate;
use actix_web::{Error, FromRequest};
use futures::future::Ready;
use paperclip::actix::Apiv2Security;
use serde::Deserialize;

pub(super) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_nodes).service(get_node);
}

#[derive(Apiv2Security, Deserialize)]
#[openapi(
    apiKey,
    alias = "JWT",
    in = "header",
    name = "Authorization",
    description = "Use format 'Bearer TOKEN'"
)]
struct AccessToken;

impl FromRequest for AccessToken {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;
    type Config = ();

    fn from_request(
        req: &HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        futures::future::ready(authenticate(req).map(|_| Self {}))
    }
}

#[get("/v0", "/nodes", tags(Nodes))]
async fn get_nodes(
    _token: AccessToken,
) -> Result<web::Json<Vec<Node>>, RestError> {
    RestRespond::result(MessageBus::get_nodes().await)
}
#[get("/v0", "/nodes/{id}", tags(Nodes))]
async fn get_node(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<web::Json<Node>, RestError> {
    RestRespond::result(MessageBus::get_node(&node_id).await)
}
