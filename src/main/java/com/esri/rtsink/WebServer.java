/*
Web Server allows for Marathon health check and also retrieval of counts.
 */
package com.esri.rtsink;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;

/**
 *
 * @author david
 */
public class WebServer {

    private final int port;
    HttpContext cntContext;
    
    RootHandler rootHandler;
    
    public void addCnt(long cnt) {
        rootHandler.addCnt(cnt);
    }
    
    public void addRate(double rate) {
        rootHandler.addRate(rate);
    }

    public void addLatency(double latency) {
        rootHandler.addLatency(latency);
    }

    public void setTm(long tm) {
        rootHandler.setTm(tm);
    }
    
    public WebServer(int port) {

        this.port = port;

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new RootHandler());
            server.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
