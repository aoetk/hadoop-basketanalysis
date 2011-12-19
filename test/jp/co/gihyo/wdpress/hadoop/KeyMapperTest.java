/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 * @author aoetakashi
 */
public class KeyMapperTest {

    private static final Text TEST_LINE = new Text("54E8C79987B6F2F3	970916215423	pregnant");

    public KeyMapperTest() {
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        KeyMapper mapper = new KeyMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        mapper.map(null, TEST_LINE, context);
        verify(context).write(new Text("54E8C79987B6F2F3#970916215423"), new Text("970916215423#pregnant"));
    }

}
