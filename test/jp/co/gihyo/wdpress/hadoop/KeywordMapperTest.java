/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 * @author aoetakashi
 */
public class KeywordMapperTest {

    private static final Text TEST_LINE = new Text("54E8C79987B6F2F3	970916215423	pregnant");

    public KeywordMapperTest() {
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        KeywordMapper mapper = new KeywordMapper();
        Context context = mock(Context.class);
        mapper.map(null, TEST_LINE, context);
        verify(context).write(new Text("pregnant"), new IntWritable(1));
    }
}
