package com.esri.rtsink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author david
 */
public class GetIp implements HttpHandler {

    @Override
    public void handle(HttpExchange he) throws IOException {
        String response = "";

        JSONObject obj = new JSONObject();
        JSONArray array = new JSONArray();
        
        try {
            Process p = Runtime.getRuntime().exec("dig @master.mesos rtsink.marathon.mesos +short");

            String s = null;
            String se = null;
            
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            while ((s = stdInput.readLine()) != null) {
                String ips[] = s.split("\n");
                for (String ip: ips) {
                    array.put(ip);                                        
                }
                
            }

            while ((se = stdError.readLine()) != null) {
                throw new Exception(se);
            }

            // Add additional code for health check
            obj.put("ip", array);
            response = obj.toString();
        } catch (Exception e) {
            response = "\"error\":\"" + e.getMessage() + "\"";
            e.printStackTrace();
        }

        he.sendResponseHeaders(200, response.length());
        OutputStream os = he.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

}
