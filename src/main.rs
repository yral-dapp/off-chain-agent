use std::env;
use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::{response::Html, routing::get, Router};
use http::header::CONTENT_TYPE;
use tonic::metadata::MetadataValue;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceExt;

use crate::auth::{check_auth_grpc, AuthBearer};
use crate::canister::canisters_list_handler;
use crate::canister::snapshot::backup_job_handler;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};

mod auth;
pub mod canister;
mod consts;
mod events;

#[tokio::main]
async fn main() {
    // let off_chain_agent_grpc_auth_token = r#"ZDe-z,;K1|O!Yiv23tHgJufL|V.KP]LYj.G90Y+3C]Y*WRc8+XS]uRP+Bv0iw{0TkQB"esc,#Li0#/W4JFL)KMS@:fL\t:)'3w[?.GPO8lFpBA4l0e(H?R]ah9YD,0'+EPz{PB/Qluk5V7!h*|h|GZi,k24C"SL=yJ[L#c,TUI#|ha?5qHgQuJYD:h!)Jglpl@ul8[.tbbR.{29(]I17DgM)d.ME);C6FRf}lTk@-MB@I3YWjpQwW3E|Tk,7y2pC4JjT8]7XUV:Pdd)Y3,lfwU]b9U+}U=}7.{/O/-kdF;FcWUf.-([Ode)LLJ+,PekfVdkHs[!.R5}w]5evIi?QL{at27duOOKHKHwt\S#JhlSKrAWovWj?43yHf\r[V5#:,SzE8[l6'3uq9H@QwQb}RTbhB#3B/y7x5(b/OKd-+L@(V*u'sSW(zG52{RzyU-@qZ79(g3Fpd5WM*tmQ,,p{p6gHMktS?;5vwjOEmgpQvC#lD'HHt"=doE@S?iu.E-IZ"#.to_string();

    // let token: MetadataValue<_> = format!("Bearer {}", off_chain_agent_grpc_auth_token)
    //     .parse()
    //     .unwrap();

    // println!("token: {:?}", token);

    // build our application with a route
    let http = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        .map_err(axum::BoxError::from)
        .boxed_clone();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let grpc = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WarehouseEventsServer::with_interceptor(
            WarehouseEventsService {},
            check_auth_grpc,
        )))
        .add_service(reflection_service)
        .into_service()
        .map_response(|r| r.map(axum::body::boxed))
        .boxed_clone();

    let http_grpc = Steer::new(
        vec![http, grpc],
        |req: &http::Request<hyper::Body>, _svcs: &[_]| {
            if req.headers().get(CONTENT_TYPE).map(|v| v.as_bytes()) != Some(b"application/grpc") {
                0
            } else {
                1
            }
        },
    );

    // run it
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 50051));

    println!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(Shared::new(http_grpc))
        .await
        .unwrap();
}

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}
