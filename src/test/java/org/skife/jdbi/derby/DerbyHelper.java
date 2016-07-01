/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.jdbi.derby;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.skife.jdbi.HandyMapThing;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import javax.sql.DataSource;

public class DerbyHelper
{
    public static final String DERBY_SYSTEM_HOME = "target";

    private Driver driver;
    private boolean running = false;
    private EmbeddedDataSource dataSource;

    private final String dbName;

    public DerbyHelper()
    {
        this.dbName = "memory:testing-" + UUID.randomUUID().toString();
    }

    public void start() throws SQLException, IOException
    {
        if (!running)
        {
            // Make derby.log end up in the 'target'-directory.
            // Could also suppress the log completely.
            System.setProperty("derby.system.home", DERBY_SYSTEM_HOME);

            EmbeddedDataSource newDataSource = new EmbeddedDataSource();
            newDataSource.setCreateDatabase("create");
            newDataSource.setDatabaseName(dbName);

            dataSource = newDataSource;

            final Connection conn = dataSource.getConnection();
            conn.close();

            running = true;
        }
    }

    public void stop() throws SQLException
    {

        // Drop the in-memory database.
        dataSource.setCreateDatabase(null);
        dataSource.setConnectionAttributes("drop=true");
        try {
            dataSource.getConnection();
            if (true) throw new IllegalStateException("failed to drop Derby database");
        } catch (SQLException sqle) {
            if (!"08006".equals(sqle.getSQLState())) {
                throw sqle;
            }
        }
    }

    public Connection getConnection() throws SQLException
    {
        return dataSource.getConnection();
    }

    public String getDbName()
    {
        return dbName;
    }

    public String getJdbcConnectionString()
    {
        return "jdbc:derby:" + getDbName();
    }

    public void dropAndCreateSomething() throws SQLException
    {
        final Connection conn = getConnection();

        final Statement create = conn.createStatement();
        try
        {
            create.execute("create table something ( id integer, name varchar(50), integerValue integer, intValue integer )");
        }
        catch (Exception e)
        {
            // probably still exists because of previous failed test, just delete then
            create.execute("delete from something");
        }
        create.close();
        conn.close();
    }

    public DataSource getDataSource()
    {
        return dataSource;
    }

    public static String doIt()
    {
        return "it";
    }

    public static <K> HandyMapThing<K> map(K k, Object v)
    {
        HandyMapThing<K>s =  new HandyMapThing<K>();
        return s.add(k, v);
    }
}
