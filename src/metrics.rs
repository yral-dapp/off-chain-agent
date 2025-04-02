use yral_metrics::{
    metric_sender::{mock::MaybeMockLocalMetricEventTx, vectordb::VectorDbMetricTx, LocalMetricTx},
    metrics::EventSource,
};

pub type CfMetricTx = LocalMetricTx<MaybeMockLocalMetricEventTx<VectorDbMetricTx>>;

pub fn init_metrics() -> CfMetricTx {
    let ev_tx = MaybeMockLocalMetricEventTx::Real(VectorDbMetricTx::default());
    LocalMetricTx::new(EventSource::Yral, ev_tx)
}
