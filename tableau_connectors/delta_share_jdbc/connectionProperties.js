(function propertiesbuilder(attr) {
    const DELTA_LAKE_CONNECTOR_VERSION = '1.0';

    // These are the DBClientConfig properties
    var props = {};
    props['custom_user_agent'] = 'tableau/' + DELTA_LAKE_CONNECTOR_VERSION + '('
        + connectionHelper.GetProductName()
        + '/' + connectionHelper.GetProductVersion()
        + ' ' + connectionHelper.GetPlatform() + ')';
    return props;
})
