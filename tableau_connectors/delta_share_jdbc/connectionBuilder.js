(function dsbuilder(attr) {
    var connectionString = ":memory:";

    let url = "jdbc:duckdb:";  // `jdbc:duckdb:` or `jdbc:duckdb::memory:` are equivalent
    url +=  connectionString;

    return [url];
})
