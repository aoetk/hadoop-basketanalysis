/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
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
    public void testMap() throws IOException {
        KeyMapper mapper = new KeyMapper();
        OutputCollector<Text, Text> output = mock(OutputCollector.class);
        mapper.map(null, TEST_LINE, output, null);
        verify(output).collect(new Text("54E8C79987B6F2F3#970916215423"), new Text("970916215423#pregnant"));
    }

}
