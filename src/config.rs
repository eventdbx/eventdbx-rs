use std::time::Duration;

/// Runtime configuration for an [`EventDbxClient`](crate::EventDbxClient).
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub token: String,
    pub tenant_id: String,
    pub protocol_version: u16,
    pub connect_timeout: Duration,
    pub request_timeout: Option<Duration>,
}

impl ClientConfig {
    /// Creates a configuration with sensible defaults (`port = 6363`, `tenant = "default"`).
    pub fn new(host: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 6363,
            token: token.into(),
            tenant_id: "default".into(),
            protocol_version: 1,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Some(Duration::from_secs(10)),
        }
    }

    /// Sets the wire protocol version sent during the hello handshake.
    pub fn with_protocol_version(mut self, version: u16) -> Self {
        self.protocol_version = version;
        self
    }

    /// Overrides how long the client waits for the TCP connection to open.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Overrides the per-request timeout. Use `None` to disable it.
    pub fn with_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Overrides the target TCP port. Defaults to 6363.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Overrides the tenant identifier sent during the handshake.
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = tenant_id.into();
        self
    }

    /// Returns `host:port`, handy when logging target endpoints.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
