use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(id: String) -> Self {
                Self(id)
            }
        }

        impl From<&str> for $name {
            fn from(id: &str) -> Self {
                Self(id.to_owned())
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

string_id!(
    /// Id of a [`crate::proto::Unit`], as assigned by the engine.
    UnitId
);
string_id!(
    /// Id of a [`crate::proto::Group`], as assigned by the engine.
    GroupId
);

/// Id of an in-flight command, assigned by [`crate::Dispatcher`] when the
/// command is sent. Engines echo it back through [`crate::Dispatcher::complete`]
/// to acknowledge the command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandId(pub(crate) u64);

impl fmt::Display for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<u64> for CommandId {
    /// Reconstructs a [`CommandId`] engines received from [`crate::CommandSink::dispatch`]
    /// (e.g. round-tripped through an engine-side callback) so it can be
    /// passed back to [`crate::Dispatcher::complete`].
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<CommandId> for u64 {
    fn from(id: CommandId) -> Self {
        id.0
    }
}

/// Monotonic allocator for [`CommandId`]s, owned by [`crate::Dispatcher`].
#[derive(Debug, Default)]
pub(crate) struct CommandIdAllocator(AtomicU64);

impl CommandIdAllocator {
    pub(crate) fn next(&self) -> CommandId {
        CommandId(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_id_round_trips_through_u64() {
        let allocator = CommandIdAllocator::default();
        let id = allocator.next();
        let raw: u64 = id.into();
        assert_eq!(CommandId::from(raw), id);
    }

    #[test]
    fn allocator_hands_out_distinct_ids() {
        let allocator = CommandIdAllocator::default();
        assert_ne!(allocator.next(), allocator.next());
    }
}
