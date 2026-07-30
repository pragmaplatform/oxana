use crate::{QueueConfig, RuntimeBuilder, worker_registry::WorkerConfig};

pub struct ComponentRegistry<DT>
where
    DT: Clone + Send + Sync + 'static,
{
    /// `module_path!()`
    pub module_path: &'static str,
    /// `stringify!(MyStruct)`
    pub type_name: &'static str,
    pub definition: fn() -> ComponentDefinition<DT>,
}

pub enum ComponentDefinition<DT>
where
    DT: Clone + Send + Sync + 'static,
{
    Queue(QueueConfig),
    Worker(WorkerConfig<DT>),
    WorkerRegistration(fn(RuntimeBuilder<DT>) -> RuntimeBuilder<DT>),
}

pub trait RegisterComponents {
    type Context: Clone + Send + Sync + 'static;

    fn register_components(runtime: RuntimeBuilder<Self::Context>)
    -> RuntimeBuilder<Self::Context>;
}

/// Macro to create a component registry
pub use inventory::collect as create_component_registry;

/// Macro to register a Queue or Worker
pub use inventory::submit as register_component;

/// Helper type to iterate components
pub use inventory::iter as iterate_components;

impl<DT> ComponentRegistry<DT>
where
    DT: Clone + Send + Sync + 'static,
{
    pub fn register_components(
        mut runtime: RuntimeBuilder<DT>,
        items: impl Iterator<Item = &'static Self>,
    ) -> RuntimeBuilder<DT> {
        for component in items {
            tracing::info!(
                "Registering {}::{}",
                component.module_path,
                component.type_name
            );
            match (component.definition)() {
                ComponentDefinition::Queue(q) => runtime = runtime.queue_with(q),
                ComponentDefinition::Worker(w) => runtime = runtime.worker_with(w),
                ComponentDefinition::WorkerRegistration(register) => runtime = register(runtime),
            }
        }
        runtime
    }
}
