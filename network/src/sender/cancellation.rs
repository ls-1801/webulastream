use crate::sender::network_service::Result;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

pub(crate) trait Cancellable {
    fn cancellation_token(&self) -> &CancellationToken;
}

pub(crate) fn cancel_all<IdentifierType, CancellableType>(
    map: HashMap<IdentifierType, CancellableType>,
) -> Result<()>
where
    CancellableType: Cancellable,
{
    map.into_iter()
        .for_each(|(_, value)| value.cancellation_token().cancel());
    Ok(())
}
