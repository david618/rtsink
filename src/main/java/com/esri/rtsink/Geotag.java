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
public interface Geotag extends Transform {
    public String transform(String line, boolean filter);
}
