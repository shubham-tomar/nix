package com.kafka.flink.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class Utils {

    public static void ListNessieBranches() {
        try {
            String nessieUrl = "http://localhost:8080/branches"; // Nessie REST API URL
            URL url = new URL(nessieUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                System.out.println("Failed to get branches from Nessie. HTTP error code: " + conn.getResponseCode());
                return;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder response = new StringBuilder();
            String output;
            while ((output = br.readLine()) != null) {
                response.append(output);
            }
            conn.disconnect();

            // Parse JSON response
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(response.toString());
            
            // Check if we have a successful response
            if (rootNode.has("status") && rootNode.get("status").asInt() == 200) {
                // Get the branches array from the data object
                JsonNode branchesNode = rootNode.path("data").path("branches");
                
                System.out.println("✅ Available Nessie Branches:");
                for (JsonNode branch : branchesNode) {
                    String name = branch.path("name").asText();
                    String type = branch.path("type").asText();
                    String hash = branch.path("hash").asText();
                    System.out.println("- " + name + " (" + type + ") [" + hash.substring(0, 8) + "...]");
                }
            } else {
                System.out.println("❌ Failed to retrieve branches. Response: " + rootNode.toString());
            }

        } catch (Exception e) {
            System.out.println("❌ Error fetching Nessie branches: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void CreateBranch(String sourceRef, String branchName) {
        try {
            String nessieUrl = "http://localhost:8080/branches"; // Nessie REST API URL
            URL url = new URL(nessieUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            
            // Create JSON request body
            String jsonInputString = String.format(
                "{\"branch_name\": \"%s\", \"source_ref\": \"%s\"}", 
                branchName,
                sourceRef
            );
            
            // Write the request body
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            
            int responseCode = conn.getResponseCode();
            if (responseCode != 201 && responseCode != 200) {
                System.out.println("❌ Failed to create branch in Nessie. HTTP error code: " + responseCode);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String output;
                    while ((output = br.readLine()) != null) {
                        errorResponse.append(output);
                    }
                    System.out.println("Error response: " + errorResponse);
                }
                return;
            }

            // Read the response
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder response = new StringBuilder();
            String output;
            while ((output = br.readLine()) != null) {
                response.append(output);
            }
            conn.disconnect();

            // Parse JSON response
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(response.toString());

            // Check if we have a successful response (201 for created)
            if (rootNode.has("status") && (rootNode.get("status").asInt() == 201 || rootNode.get("status").asInt() == 200)) {
                // Get the hash from the response data
                String hash = rootNode.path("data").path("hash").asText();
                System.out.println("✅ Created branch in Nessie: " + branchName + " with hash: " + hash.substring(0, 8) + "...");
            } else {
                System.out.println("❌ Failed to create branch. Response: " + rootNode.toString());
            }

        } catch (Exception e) {
            System.out.println("❌ Error creating Nessie branch: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void DeleteBranch(String branchName) {
        try {
            String nessieUrl = "http://localhost:8080/branches/" + branchName; // Nessie REST API URL
            URL url = new URL(nessieUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                System.out.println("❌ Failed to delete branch in Nessie. HTTP error code: " + conn.getResponseCode());
                return;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder response = new StringBuilder();
            String output;
            while ((output = br.readLine()) != null) {
                response.append(output);
            }
            conn.disconnect();

            // Parse JSON response
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(response.toString());

            // Check if we have a successful response (200 for deleted)
            if (rootNode.has("status") && rootNode.get("status").asInt() == 200) {
                System.out.println("✅ Deleted branch in Nessie: " + branchName);
            } else {
                System.out.println("❌ Failed to delete branch. Response: " + rootNode.toString());
            }

        } catch (Exception e) {
            System.out.println("❌ Error deleting Nessie branch: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void MergeBranches(String sourceRef, String targetRef) {
        try {
            String nessieUrl = "http://localhost:8080/branches/merge"; // Nessie REST API URL for merging
            URL url = new URL(nessieUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            
            // Create JSON request body
            String jsonInputString = String.format(
                "{\"source_branch\": \"%s\", \"target_branch\": \"%s\"}", 
                sourceRef,
                targetRef
            );
            
            // Write the request body
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            // Get the response
            int responseCode = conn.getResponseCode();
            
            if (responseCode == 200) {
                BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
                StringBuilder response = new StringBuilder();
                String output;
                while ((output = br.readLine()) != null) {
                    response.append(output);
                }
                conn.disconnect();

                // Parse JSON response
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(response.toString());
                
                // Extract merge hash from response
                if (rootNode.has("data") && rootNode.get("data").has("merge_hash")) {
                    String mergeHash = rootNode.get("data").get("merge_hash").asText();
                    System.out.println("✅ Successfully merged branch " + sourceRef + " into " + targetRef);
                    System.out.println("📝 Merge hash: " + mergeHash);
                } else {
                    System.out.println("✅ Branch merge completed but couldn't extract merge hash details");
                }
            } else {
                // Handle error responses
                BufferedReader br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
                StringBuilder response = new StringBuilder();
                String output;
                while ((output = br.readLine()) != null) {
                    response.append(output);
                }
                conn.disconnect();
                
                System.out.println("❌ Failed to merge branches in Nessie. HTTP error code: " + responseCode);
                System.out.println("❌ Error response: " + response.toString());
            }

        } catch (Exception e) {
            System.out.println("❌ Error merging Nessie branches: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
