public String executeSparkSQLToJson(String sqlQuery) throws Exception {
    HttpPost sessionPost = livySparkDatabaseConfig.getConnection();
    sessionPost.setEntity(new StringEntity("{ \"kind\": \"pyspark\" }"));

    CloseableHttpClient client = HttpClients.createDefault();
    HttpResponse sessionResponse = client.execute(sessionPost);
    String sessionJson = EntityUtils.toString(sessionResponse.getEntity());
    int sessionId = new JSONObject(sessionJson).getInt("id");

    String statementUrl = livySparkDatabaseConfig.getUrl() + ":" +
                          livySparkDatabaseConfig.getPort() +
                          "/sessions/" + sessionId + "/statements";

    HttpPost statementPost = new HttpPost(statementUrl);
    statementPost.setHeader("Content-Type", "application/json");

    String payload = "{ \"code\": \"spark.sql(\\\"" + sqlQuery + "\\\").toJSON().collect()\" }";
    statementPost.setEntity(new StringEntity(payload));

    HttpResponse statementResponse = client.execute(statementPost);
    String statementJson = EntityUtils.toString(statementResponse.getEntity());
    int statementId = new JSONObject(statementJson).getInt("id");

    // Poll for result
    String resultUrl = statementUrl + "/" + statementId;
    String rawJson = "";
    boolean isReady = false;

    while (!isReady) {
        Thread.sleep(2000);
        HttpResponse getResp = client.execute(new HttpGet(resultUrl));
        String responseBody = EntityUtils.toString(getResp.getEntity());

        JSONObject resultObj = new JSONObject(responseBody);
        if ("available".equals(resultObj.getString("state"))) {
            rawJson = resultObj.getJSONObject("output")
                               .getJSONObject("data")
                               .getString("text/plain");
            isReady = true;
        }
    }

    // Clean raw JSON string
    return rawJson.replace("\\\"", "\"").replace("\"[", "[").replace("]\"", "]");
}
