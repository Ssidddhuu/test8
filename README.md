public String executeSparkSQLShow(String sqlQuery) throws Exception {
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

    String payload = "{ \"code\": \"spark.sql(\\\"" + sqlQuery + "\\\").show()\" }";
    statementPost.setEntity(new StringEntity(payload));

    HttpResponse statementResponse = client.execute(statementPost);
    String statementJson = EntityUtils.toString(statementResponse.getEntity());
    int statementId = new JSONObject(statementJson).getInt("id");

    // Poll for plain text result
    String resultUrl = statementUrl + "/" + statementId;
    String output = "";
    boolean isDone = false;

    while (!isDone) {
        Thread.sleep(2000);
        HttpResponse getResp = client.execute(new HttpGet(resultUrl));
        String resultJson = EntityUtils.toString(getResp.getEntity());

        JSONObject resultObj = new JSONObject(resultJson);
        if ("available".equals(resultObj.getString("state"))) {
            output = resultObj.getJSONObject("output")
                              .getJSONObject("data")
                              .getString("text/plain");
            isDone = true;
        }
    }

    return output;
}
