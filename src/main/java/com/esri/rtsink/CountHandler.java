
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
public class CountHandler implements HttpHandler {

    static long cnt = 0;
    static double rate = 0;
    static long tm = System.currentTimeMillis();

    public static void setCnt(long cnt) {
        CountHandler.cnt = cnt;
    }

    public static void setRate(double rate) {
        CountHandler.rate = rate;
    }

    public static void setTm(long tm) {
        CountHandler.tm = tm;
    }

    
    @Override
    public void handle(HttpExchange he) throws IOException {
        String response = "";
        
        JSONObject obj = new JSONObject();
        try {
            obj.put("tm", tm);
            // Add additional code for health check
            obj.put("count", cnt);    
            obj.put("rate", rate);
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
