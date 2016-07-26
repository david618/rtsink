
package com.esri.rtsink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import org.json.JSONObject;

/**
 *
 * @author david
 */
public class RootHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange he) throws IOException {
        String response = "";
        
        JSONObject obj = new JSONObject();
        try {
            // Add additional code for health check
            obj.put("healthy", true);                        
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
