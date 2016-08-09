/*

*/
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

    static long cnt = 0;
    static double rate = 0;
    static long tm = System.currentTimeMillis();    
    
    public static void reset() {
        cnt = 0;
        rate = 0.0;
        tm = System.currentTimeMillis();
    }    
    
    public static void setCnt(long cnt) {
        RootHandler.cnt = cnt;
    }

    public static void setRate(double rate) {
        RootHandler.rate = rate;
    }

    public static void setTm(long tm) {
        RootHandler.tm = tm;
    }    
    
    @Override
    public void handle(HttpExchange he) throws IOException {
        String response = "";
        
        JSONObject obj = new JSONObject();
        try {
            
            String uriPath = he.getRequestURI().toString();
            
            if (uriPath.equalsIgnoreCase("/count") || uriPath.equalsIgnoreCase("/count/")) {
                // Return count
                obj.put("tm", tm);
                // Add additional code for health check
                obj.put("count", cnt);    
                obj.put("rate", rate);                
            } else if (uriPath.equalsIgnoreCase("/reset") || uriPath.equalsIgnoreCase("/reset/")) {
                // Reset counts
                reset();
                obj.put("done", true);     
            } else if (uriPath.equalsIgnoreCase("/")) {
                // 
                // Add additional code for health check
                obj.put("healthy", true);                        
            } else {
                obj.put("error","Unsupported URI");
            }
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
