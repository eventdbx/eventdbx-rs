use eventdbx_client::ClientConfig;

#[test]
fn client_config_defaults() {
    let config = ClientConfig::new("example.com", "token-123");

    assert_eq!(config.host, "example.com");
    assert_eq!(config.port, 6363, "default port should be 6363");
    assert_eq!(config.tenant_id, "default");
    assert_eq!(config.address(), "example.com:6363");
}

#[test]
fn client_config_overrides() {
    let config = ClientConfig::new("example.com", "token-123")
        .with_tenant("acme")
        .with_port(7000)
        .with_protocol_version(2)
        .with_request_timeout(None);

    assert_eq!(config.tenant_id, "acme");
    assert_eq!(config.port, 7000);
    assert_eq!(config.protocol_version, 2);
    assert!(config.request_timeout.is_none());
}
