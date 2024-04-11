use tonic::transport::server::Router;
use warehouse_events::warehouse_events_server::{WarehouseEvents, WarehouseEventsServer};
use crate::events::warehouse_events::{Empty, WarehouseEvent};


pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
}



pub struct WarehouseEventsService {}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        println!("Got event: {}", request.into_inner().event);
        Ok(tonic::Response::new(Empty {}))
    }
}

pub fn new_grpc_server() -> Router {
    // I need to expose these services as grpc-web (using tonic_web I think)

    // let server = tonic_web::enable(GreeterServer::new(GrpcServiceImpl::default()));

    let server = tonic::transport::Server::builder()
        .add_service(WarehouseEventsServer::new(WarehouseEventsService{}));

    server
}