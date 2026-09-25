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

/// Monotonic allocator for [`CommandId`]s, owned by [`crate::Dispatcher`].
#[derive(Debug, Default)]
pub(crate) struct CommandIdAllocator(AtomicU64);

impl CommandIdAllocator {
    pub(crate) fn next(&self) -> CommandId {
        CommandId(self.0.fetch_add(1, Ordering::Relaxed))
    }
}
