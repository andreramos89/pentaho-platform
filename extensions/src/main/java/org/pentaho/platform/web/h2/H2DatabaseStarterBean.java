/*!
 *
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 *
 * Copyright (c) 2002-2018 Hitachi Vantara. All rights reserved.
 *
 */

package org.pentaho.platform.web.h2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.pentaho.platform.web.h2.messages.Messages;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;


public class H2DatabaseStarterBean {

  private static final Log logger = LogFactory.getLog( H2DatabaseStarterBean.class );

  private Server h2Server;

  private int port = 9001; // Default Port
  private int failoverPort = port;

  private Map<String, String> databases = new LinkedHashMap<String, String>();

  private boolean allowFailoverToDefaultPort;

  protected boolean checkPort() {
    if ( ( port < 0 ) || ( port > 65535 ) ) {
      if ( allowFailoverToDefaultPort ) {
        logger.error( Messages.getErrorString( "H2DatabaseStarterBean.ERROR_0004_INVALID_PORT", "" + failoverPort ) );
        port = failoverPort;
      } else {
        return logFailure( "H2DatabaseStarterBean.ERROR_0004_INVALID_PORT", "" + failoverPort );
      }
    }

    try {
      ServerSocket sock = new ServerSocket( port );
      sock.close();
    } catch ( IOException ex1 ) {
      if ( port == failoverPort ) {
        return logFailure( "H2DatabaseStarterBean.ERROR_0006_DEFAULT_PORT_IN_USE" );
      } else {
        if ( allowFailoverToDefaultPort ) {
          logger.error( Messages.getErrorString(
              "H2DatabaseStarterBean.ERROR_0005_SPECIFIED_PORT_IN_USE", Integer.toString( port ), "" + failoverPort ) );
          port = failoverPort;
          try {
            ServerSocket sock = new ServerSocket( port );
            sock.close();
          } catch ( IOException ex2 ) {
            return logFailure( "H2DatabaseStarterBean.ERROR_0006_DEFAULT_PORT_IN_USE" );
          }
        } else {
          return logFailure(
              "H2DatabaseStarterBean.ERROR_0008_SPECIFIED_PORT_IN_USE_NO_FAILOVER", Integer.toString( port ) );
        }
      }
    }
    return true;
  }

  protected Properties getServerProperties(  ) {
    Properties props = new Properties();
    //


    return props;

  }

  protected Server getNewH2Server() throws SQLException {
    return Server.createTcpServer("-tcpPort", String.valueOf( port ), "-tcpAllowOthers");
  }

  protected Server getNewH2Server(int port) throws SQLException {
    this.port=port;
    return Server.createTcpServer("-tcpPort", String.valueOf( port ), "-tcpAllowOthers");
  }

  public boolean start() {

    try {
      HashMap<String, String> databaseList = getDatabaseMap();

      h2Server = getNewH2Server(port).start();

      //start databases

      startDatabases(databaseList);

    }catch ( SQLException e ){
      if(e.getSQLState() != null && e.getSQLState().equals( "90061" )){
        port+=1; //fix this
        start();
      }
      return false;
    }
    logger.info( "Started Server " + h2Server.getService() + " - current status: " + h2Server.getStatus() );

    return h2Server.isRunning( false );
  }

  private void startDatabases(HashMap<String,String> databases) {

    for(HashMap.Entry entry : databases.entrySet()){
      String databaseName = (String) entry.getKey();
      String startUpScript = (String) entry.getValue();

      File file = new File( startUpScript );
      if ( !file.exists() ){
        continue;
      }

      try {

        Properties props = new Properties();
        props.put( "user", "SA" );
        props.put( "password", "" );
        Class.forName("org.h2.Driver");
        Connection currentConn = DriverManager.getConnection( "jdbc:h2:../../data/h2/"+databaseName, props );

        boolean tablesExist = tablesAlreadyCreated(currentConn, databaseName);

        if(!tablesExist) {
          RunScript.execute( currentConn, new FileReader( startUpScript ) );
        }
        logger.info( "Starting up database: " + databaseName + " with script: " + startUpScript );

      }catch ( SQLException e ){
          logFailure( "H2DatabaseStarterBean.ERROR_0009_PROBLEM_CREATING_DATABASE" , databaseName);
      } catch ( FileNotFoundException e ) {
          logFailure( "H2DatabaseStarterBean.ERROR_0010_STARTUP_SCRIPT_NOT_FOUND", startUpScript );
      } catch ( ClassNotFoundException e ) {
          logFailure( "H2DatabaseStarterBean.ERROR_0011_DRIVER_NOT_FOUND", "org.h2.Driver" );
      }

    }
  }

  private boolean tablesAlreadyCreated( Connection currentConn, String databaseName ) throws SQLException {

    ResultSet rset = currentConn.getMetaData().getTables(null, null, "*", null);

    return rset.next();
  }

  private HashMap<String,String> getDatabaseMap() {

    HashMap<String, String> databaseMap = new HashMap<>();
    int idx = 0;
    for ( Map.Entry<String, String> entry : databases.entrySet() ) {
      databaseMap.put( entry.getKey(), entry.getValue() );

      logger.debug( MessageFormat.format(
        "H2 database {0} configured to start with name {1}", entry.getValue(), entry.getKey() ) ); 
      idx++;
    }
    return databaseMap;
      
  }

  public boolean start(Server server) throws SQLException {


    if(server == null){
      h2Server = getNewH2Server().start();
    }

    logger.info( "Started Server " + h2Server.getService() + " - current status: " + h2Server.getStatus() );

    return h2Server.isRunning( false );
  }

  public boolean stop( Server server) {
    h2Server = server;
    return stop();
  }

  public boolean stop( ) {

    if(h2Server== null || !h2Server.isRunning( false )){
      return true;
    }
    logger.info( "Stopping Server " + h2Server.getService() + " - current status: " + h2Server.getStatus() );

    h2Server.stop();

    return true;
  }

  private boolean logFailure( final String errorId ) {
    logger.error( Messages.getErrorString( errorId ) );
    logger.warn( Messages.getString( "H2DatabaseStarterBean.WARN_NO_DATABASES" ) );
    return false;
  }

  private boolean logFailure( final String errorId, String param ) {
    logger.error( Messages.getErrorString( errorId, param ) );
    logger.warn( Messages.getString( "H2DatabaseStarterBean.WARN_NO_DATABASES" ) );
    return false;
  }

  /*
   * Getters and Setters
   */

  public void setPort( int value ) {
    port = value;
  }

  public int getPort() {
    return port;
  }

  public void setFailoverPort( int value ) {
    failoverPort = value;
  }

  public int getFailoverPort() {
    return failoverPort;
  }

  public Map<String, String> getDatabases() {
    return databases;
  }

  public void setDatabases( Map<String, String> databases ) {
    this.databases = databases;
  }

  public void setAllowPortFailover( boolean value ) {
    allowFailoverToDefaultPort = value;
  }

  public boolean getAllowPortFailover() {
    return allowFailoverToDefaultPort;
  }

}
