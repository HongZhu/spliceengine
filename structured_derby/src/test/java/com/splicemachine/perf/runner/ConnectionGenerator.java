package com.splicemachine.perf.runner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public class ConnectionGenerator {
    private final String serverName;

    public ConnectionGenerator(String serverName) {
        this.serverName = serverName;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:derby://"+serverName+"/wombat;create=true");
    }
}
