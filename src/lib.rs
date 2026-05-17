pub mod generator;
pub mod matchmaking;
pub mod orchestration;
pub mod validator;

pub mod prelude {
    pub use super::generator::prelude::*;
}

pub mod locations {
    pub const MATCHMAKER_INGRESS_SOCKET_PATH: &str = "/tmp/rtmm.in.sock";
    pub const MATCHMAKER_EGRESS_SOCKET_PATH: &str = "/tmp/rtmm.out.sock";
}
