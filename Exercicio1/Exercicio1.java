package advanced.Recuperacao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio1 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio1");

        // Setting Job classes
        j.setJarByClass(Exercicio1.class);
        j.setMapperClass(MapForExercicio1.class);
        j.setReducerClass(ReduceForExercicio1.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForExercicio1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

                String[] columns = value.toString().split(";");
                    Text city = new Text(columns[6]);
                    Text curitiba = new Text("Curitiba");

                    Text firstline = new Text(columns[0]);
                    Text FirstHeader = new Text("NU_ANO_CENSO");

                    if (city.equals(curitiba) && !(firstline.equals(FirstHeader)))
                        con.write(curitiba, new IntWritable(1));
        }

    }

    public static class ReduceForExercicio1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable value : values) {
                totalCount += value.get();
            }
            con.write(key, new IntWritable(totalCount));
        }

    }

}
