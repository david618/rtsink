/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.esri.rtsink;

/**
 *
 * @author davi5017
 */
public interface Filter {
    public boolean filter(String line);  // Return true if the critera matches (e.g. fieldFoo="bar")  
}
