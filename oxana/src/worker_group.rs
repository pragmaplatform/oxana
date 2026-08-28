use std::any::TypeId;

/// Marker trait for a worker group.
///
/// Prefer deriving this trait with `#[derive(oxana::WorkerGroup)]`.
pub trait WorkerGroup: 'static {}

/// A worker group or tuple of worker groups accepted by runtime filters.
pub trait WorkerGroups {
    #[doc(hidden)]
    fn group_ids(self) -> Vec<TypeId>;
}

impl<G> WorkerGroups for G
where
    G: WorkerGroup,
{
    fn group_ids(self) -> Vec<TypeId> {
        vec![TypeId::of::<G>()]
    }
}

macro_rules! impl_worker_groups_tuple {
    ($($group:ident),+) => {
        impl<$($group),+> WorkerGroups for ($($group,)+)
        where
            $($group: WorkerGroup),+
        {
            fn group_ids(self) -> Vec<TypeId> {
                vec![$(TypeId::of::<$group>()),+]
            }
        }
    };
}

impl_worker_groups_tuple!(A);
impl_worker_groups_tuple!(A, B);
impl_worker_groups_tuple!(A, B, C);
impl_worker_groups_tuple!(A, B, C, D);
impl_worker_groups_tuple!(A, B, C, D, E);
impl_worker_groups_tuple!(A, B, C, D, E, F);
impl_worker_groups_tuple!(A, B, C, D, E, F, G);
impl_worker_groups_tuple!(A, B, C, D, E, F, G, H);
impl_worker_groups_tuple!(A, B, C, D, E, F, G, H, I);
impl_worker_groups_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_worker_groups_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_worker_groups_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);

#[doc(hidden)]
pub fn worker_group_id<G: WorkerGroup>() -> TypeId {
    TypeId::of::<G>()
}
