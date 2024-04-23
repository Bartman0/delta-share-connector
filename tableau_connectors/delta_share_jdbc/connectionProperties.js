(function propertiesbuilder(attr) {
    const DELTA_SHARE_CONNECTOR_VERSION = '1.0';

    // These are the DBClientConfig properties
    var props = {};
    props["server"] = "localhost"
    props["user"] = "n/a";
    props["password"] = "n/a";
    props['custom_user_agent'] = 'delta_share/' + DELTA_SHARE_CONNECTOR_VERSION + '('
        + connectionHelper.GetProductName()
        + '/' + connectionHelper.GetProductVersion()
        + ' ' + connectionHelper.GetPlatform() + ')';
    return props;
})
