package mm.functions;
import java.io.*;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;
public class WordCountFunction implements Function<String, String> {
   // This function is invoked every time a message is published to the input topic
    @Override
    public String process(String input, Context context) throws Exception {
       String[] words = input.split("\\s+");
       return Integer.toString(words.length);
   }
 }
