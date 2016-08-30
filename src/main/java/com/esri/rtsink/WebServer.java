/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
    
    public void setCnt(long cnt) {
        rootHandler.addCnt(cnt);
    }
    
    public void setRate(double rate) {
        rootHandler.addRate(rate);
    }    
    
    public void setTm(long tm) {
        rootHandler.setTm(tm);
    }
    
    public WebServer(int port) {

        this.port = port;

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new RootHandler());
//            cntHandler = new CountHandler();
//            server.createContext("/count", cntHandler);
//            server.createContext("/ip", new GetIp());
//            server.createContext("/reset", new ResetHandler());
            server.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
