package cscie55.hw8.problem1;

import java.io.IOException;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GsonTweetEater {



    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private IntWritable word = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // TODO your code here
            String line = value.toString();

            Gson gson = new Gson();

            // convert to generic JsonElement
            JsonElement je = gson.fromJson(line, JsonElement.class);
            // test to see what kind of object the Element is
            if(je.isJsonObject()){
                JsonObject jo = (JsonObject) je;
                System.out.println(jo.get("text"));
                JsonElement user = jo.get("user");
                // examine nested retrieved object, again to determine type
                if(user.isJsonArray()) {
                    JsonArray arr = user.getAsJsonArray();
                    JsonElement name = arr.get(0);
                    System.out.println(name.toString());
                }
                else if (user.isJsonObject()){
                    JsonObject name = user.getAsJsonObject();
                    System.out.println(name.get("name"));
                }
            }



        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // these vars are tmp for dev purposes
        // comment these 3 lines before putting into a jar for running in AWS
        String FILENAME = args[0];
        String input = GsonTweetEater.class.getClassLoader().getResource(FILENAME).getFile();//("").getPath().replaceFirst("^/(.:/)", "$1");
        String output = new String("target/")+args[1]; // replace real cmd line args w/ hard-coded

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(GsonTweetEater.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // these lines are for dev purposes
        // comment these 2 lines before putting into a for running in AWS
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        // UNCOMMENT these 2 lines before putting into a jar for running in AWS

      //   FileInputFormat.addInputPath(job, new Path(args[0]));
      //   FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


