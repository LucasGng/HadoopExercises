package advanced.Recuperacao;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
import java.util.Objects;

public class Exercicio6 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file
        Path finalOutputFile = new Path(files[2]);
        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio6");
        Job j2 = Job.getInstance(c, "Exercicio6-1");
        // Setting Job classes
        j.setJarByClass(Exercicio6.class);
        j.setMapperClass(MapForExercicio6.class);
        j.setReducerClass(ReduceForExercicio6.class);

        j2.setJarByClass(Exercicio6.class);
        j2.setMapperClass(MapForExercicio6Higher.class);
        j2.setReducerClass(ReduceForExercicio6Higher.class);
        // Setting Job output classes
        j.setMapOutputKeyClass(SiglaDependencia.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(SiglaDependencia.class);
        j.setOutputValueClass(IntWritable.class);

        j2.setMapOutputKeyClass(IntReverse.class);
        j2.setMapOutputValueClass(SiglaDependencia.class);
        j2.setOutputKeyClass(IntReverse.class);
        j2.setOutputValueClass(SiglaDependencia.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);
        FileInputFormat.addInputPath(j2, outputFile);
        FileOutputFormat.setOutputPath(j2, finalOutputFile);
        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) && j2.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForExercicio6 extends Mapper<LongWritable, Text, SiglaDependencia, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String[] columns = value.toString().split(";", -1);
            String sigla = columns[4];
            String dependencia = columns[15];
            Text firstline = new Text(columns[0]);
            Text FirstHeader = new Text("NU_ANO_CENSO");


            if (!(firstline.equals(FirstHeader))) {
                    // Saindo do Map com a Regiao e a contagem de escola (registro)
                    con.write(new SiglaDependencia(sigla, dependencia), new IntWritable(1));
            }
        }

    }

    public static class ReduceForExercicio6 extends Reducer<SiglaDependencia, IntWritable, SiglaDependencia, IntWritable> {

        public void reduce(SiglaDependencia key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int totalschools = 0;

            for(IntWritable value: values){
                totalschools += value.get();
            }
            con.write(key, new IntWritable(totalschools));
        }

    }

    public static class MapForExercicio6Higher extends Mapper<LongWritable, Text, IntReverse, SiglaDependencia>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            String sigla = columns[0];
            String dependencia = columns[1];
            int count = Integer.parseInt(columns[2]);
            con.write( new IntReverse(count), new SiglaDependencia(sigla, dependencia));
        }
    }

    public static class ReduceForExercicio6Higher extends Reducer<IntReverse, SiglaDependencia, IntReverse, SiglaDependencia>{
        public void reduce(IntReverse key, Iterable<SiglaDependencia> values, Context con) throws IOException, InterruptedException {
            SiglaDependencia out = new SiglaDependencia();
            for (SiglaDependencia value : values) {
                out = value;
            }
            con.write(key, out);
        }
    }
}
