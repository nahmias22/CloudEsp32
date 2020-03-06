package com.inm429.cloud.app;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

//import java.util.ArrayList;
//import java.util.Arrays;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

@SuppressWarnings("deprecation")
public class getTable {

  public static Bigquery createAuthorizedClient() throws IOException {
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    String jsonPath = "C:\\cloudcomputing-city2020-70d6c1d85950.json";
	GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(jsonPath));

    
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(BigqueryScopes.all());
    }

    return new Bigquery.Builder(transport, jsonFactory, credential)
        .setApplicationName("Bigquery Samples")
        .build();
  }

  private static List<TableRow> executeQuery(String querySql, Bigquery bigquery, String projectId)
      throws IOException {
    QueryResponse query =
        bigquery.jobs().query(projectId, new QueryRequest().setQuery(querySql)).execute();

    // Execute it
    GetQueryResultsResponse queryResult =
        bigquery
            .jobs()
            .getQueryResults(
                query.getJobReference().getProjectId(), query.getJobReference().getJobId())
            .execute();

    return queryResult.getRows();
  }

  private static void printResults(List<TableRow> rows) {
    System.out.print("\nQuery Results:\n------------\n");
    for (TableRow row : rows) {
      for (TableCell field : row.getF()) {
        System.out.printf("%-50s", field.getV());
      }
      System.out.println();
    }
  }

  public static void main(String[] args) throws IOException {
    Scanner sc;
    if (args.length == 0) {
      sc = new Scanner(System.in);
    } else {
      sc = new Scanner(args[0]);
      sc.close();
    }
    String projectId="cloudcomputing-city2020";

    Bigquery bigquery = createAuthorizedClient();

    List<TableRow> rows =
        executeQuery(
            "SELECT * "
                + "FROM [espData.live] ORDER BY timestamp\n",
            bigquery,
            projectId);

    printResults(rows);
//    Object[] array = rows.toArray();
//    System.out.print(array);
   // int l = length(rows(0));
  }
}

