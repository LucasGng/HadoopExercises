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

public class Exercicio2 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file
        Path finalOutputFile = new Path(files[2]);
        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio2");
        Job j2 = Job.getInstance(c, "Exercicio2-final");
        // Setting Job classes
        j.setJarByClass(Exercicio2.class);
        j.setMapperClass(MapForExercicio2.class);
        j.setReducerClass(ReduceForExercicio2.class);

        j2.setJarByClass(Exercicio2.class);
        j2.setMapperClass(LastMap.class);
        j2.setReducerClass(LastReduce.class);
        // Setting Job output classes
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        j2.setMapOutputKeyClass(IntWritable.class);
        j2.setMapOutputValueClass(Text.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(Text.class);
        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);
        FileInputFormat.addInputPath(j2, outputFile);
        FileOutputFormat.setOutputPath(j2, finalOutputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) && j2.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForExercicio2 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String[] columns = value.toString().split(";");
            Text region = new Text(columns[1]);

            Text firstline = new Text(columns[0]);
            Text FirstHeader = new Text("NU_ANO_CENSO");

            if (!(firstline.equals(FirstHeader)))
                // Saindo do Map com a Regiao e a contagem de escola (registro)
                con.write(region, new IntWritable(1));
        }

    }

    public static class ReduceForExercicio2 extends Reducer<Text, IntWritable,IntWritable,Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable value : values) {
                totalCount += value.get();
            }

            con.write(new IntWritable(totalCount), key);
        }

    }

    public static class LastMap extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            int valores = Integer.parseInt(columns[0]);
            String regiao = columns[1];
            con.write(new IntWritable(valores), new Text(regiao));
        }

    }
    public static class LastReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            Text out = new Text();
            for (Text value : values) {
                out = value;
            }
            con.write(key, out);
        }
    }
}
