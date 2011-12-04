/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author aoetakashi
 */
public class KeywordPairReducerTest {

    private static final Text[] INPUTS = {
        new Text("970916001949	yahoo chat"),
        new Text("970916001954	yahoo chat"),
        new Text("970916003523	yahoo chat"),
        new Text("970916011322	yahoo search"),
        new Text("970916011404	yahoo chat")
    };

    public KeywordPairReducerTest() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void testReduce() throws IOException {
        List<Text> inputList = Arrays.asList(INPUTS);
        KeywordPairReducer reducer = new KeywordPairReducer();
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
        reducer.reduce(new Text("BED75271605EBD0C	970916001949"), inputList.iterator(), output, null);
        verify(output, times(1)).collect(new Text("yahoo search	yahoo chat"), new IntWritable(1));
    }

}
