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
 * Copyright (c) 2023 Hitachi Vantara. All rights reserved.
 *
 */

package org.pentaho.platform.web.http.context;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.web.h2.H2DatabaseStarterBean;
import org.pentaho.platform.web.h2.messages.Messages;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class H2StartupListener implements ServletContextListener {

  private static final Log logger = LogFactory.getLog( H2StartupListener.class );

  public void contextDestroyed( ServletContextEvent sce ) {
    ServletContext ctx = sce.getServletContext();
    Object obj = ctx.getAttribute( "h2db-starter-bean" ); 
    if ( obj != null ) {
      logger.debug( "Context listener stopping Embedded H2" ); 
      H2DatabaseStarterBean starterBean = (H2DatabaseStarterBean) obj;
      starterBean.stop();
    }
  }

  private Map<String, String> getDatabases( ServletContext ctx ) {
    HashMap<String, String> map = new LinkedHashMap<String, String>();
    String dbs = ctx.getInitParameter( "h2-databases" );
    if ( dbs != null ) {
      String[] dbEntries = dbs.split( "," ); 
      for ( int i = 0; i < dbEntries.length; i++ ) {
        String[] entry = dbEntries[ i ].split( "@" ); 
        if ( ( entry.length != 2 ) || ( StringUtils.isEmpty( entry[ 0 ] ) ) || ( StringUtils.isEmpty( entry[ 1 ] ) ) ) {
          logger.error(
            Messages.getErrorString( "H2DatabaseStartupListener.ERROR_0001_H2_ENTRY_MALFORMED" ) );
          continue;
        }
        map.put( entry[ 0 ], entry[ 1 ] );
      }
    }
    return map;
  }

  public void contextInitialized( ServletContextEvent sce ) {
    ServletContext ctx = sce.getServletContext();

    logger.debug( "Starting H2DB Embedded Listener" ); 
    H2DatabaseStarterBean starterBean = new H2DatabaseStarterBean();
    String port = ctx.getInitParameter( "h2db-port" ); 
    int portNum = -1;
    if ( port != null ) {
      logger.debug( String.format( "Port override specified: %s", port ) ); 
      try {
        portNum = Integer.parseInt( port );
        starterBean.setPort( portNum );
      } catch ( NumberFormatException ex ) {
        logger.error( Messages.getErrorString( "H2StartupListener.ERROR_0004_INVALID_PORT", "9001" ) );
        port = null; // force check default port
      }
    }

    starterBean.setDatabases( getDatabases( ctx ) );

    String sampleDataAllowPortFailover = ctx.getInitParameter( "h2-allow-port-failover" );
    if ( ( sampleDataAllowPortFailover != null ) && ( sampleDataAllowPortFailover.equalsIgnoreCase( "true" ) ) ) { 
      logger.debug( String.format( "Allow Port Failover specified" ) ); 
      starterBean.setAllowPortFailover( true );
    }

    if ( starterBean.start() ) {
      ctx.setAttribute( "h2db-starter-bean", starterBean ); 
    }

  }

}
