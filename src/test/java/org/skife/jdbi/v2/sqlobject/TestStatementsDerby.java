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
package org.skife.jdbi.v2.sqlobject;

import java.util.List;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.mixins.CloseMe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.skife.jdbi.v2.DBITestCase;

public class TestStatementsDerby
        extends DBITestCase
{
    private Handle handle;

    @Override
    protected void doSetUp() throws Exception
    {
        handle = openHandle();
    }

    @Override
    protected void doTearDown() throws Exception
    {
        handle.close();
    }

    @Test
    public void testInsert() throws Exception
    {
        Inserter i = SqlObjectBuilder.attach(handle, Inserter.class);

        // this is what is under test here
        int rows_affected = i.insert(2, "Diego");

        String name = handle.createQuery("select name from something where id = 2").mapTo(String.class).first();

        assertEquals(1, rows_affected);
        assertEquals("Diego", name);

        i.close();
    }

    @Test
    public void testInsertWithNull() throws Exception
    {
        Inserter i = SqlObjectBuilder.attach(handle, Inserter.class);

        // this is what is under test here
        int rows_affected = i.insert(2, null);

        String name = handle.createQuery("select name from something where id = 2").mapTo(String.class).first();

        assertEquals(1, rows_affected);
        assertEquals(null, name);

        i.close();
    }

    @Test
    public void testValuesWithNull() throws Exception
    {
        Inserter i = SqlObjectBuilder.attach(handle, Inserter.class);
        i.insert(1, "test1");
        i.insert(2, "test2");

        // this is what is under test here
        List<Integer> values1 = i.values(null);
        assertTrue(values1.isEmpty());

        List<Integer> values2 = i.values(2);
        assertEquals(2, values2.size());

        List<Integer> values3 = i.values(1);
        assertTrue(values3.isEmpty());

        i.close();
    }

    @Test
    public void testInsertWithVoidReturn() throws Exception
    {
        Inserter i = SqlObjectBuilder.attach(handle, Inserter.class);

        // this is what is under test here
        i.insertWithVoidReturn(2, "Diego");

        String name = handle.createQuery("select name from something where id = 2").mapTo(String.class).first();

        assertEquals("Diego", name);

        i.close();
    }

    public static interface Inserter extends CloseMe
    {
        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        public int insert(@Bind("id") long id, @Bind("name") String name);

        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        public void insertWithVoidReturn(@Bind("id") long id, @Bind("name") String name);

        @SqlQuery("select id from something where :it <> 1")
        public List<Integer> values(@Bind Object it);
    }
}
