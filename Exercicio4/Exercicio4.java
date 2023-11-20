package advanced.Recuperacao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.util.Objects;

public class Exercicio4 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio4");

        // Setting Job classes
        j.setJarByClass(Exercicio1.class);
        j.setMapperClass(MapForExercicio4.class);
        j.setReducerClass(ReduceForExercicio4.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ContagemMatriculas.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForExercicio4 extends Mapper<LongWritable, Text, Text, ContagemMatriculas> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String[] columns = value.toString().split(";", -1);
            Text firstline = new Text(columns[0]);
            Text FirstHeader = new Text("NU_ANO_CENSO");

            if (!(firstline.equals(FirstHeader)) && !Objects.equals(columns[305], "")) {
                Text regiao = new Text(columns[1]);
                int matriculas = Integer.parseInt(columns[305]);
                con.write(regiao, new ContagemMatriculas(matriculas, 1));
            }
        }

    }

    public static class ReduceForExercicio4 extends Reducer<Text, ContagemMatriculas, Text, FloatWritable> {

        public void reduce(Text key, Iterable<ContagemMatriculas> values, Context con) throws IOException, InterruptedException {
            int sumcount = 0;
            int summat = 0;
            for (ContagemMatriculas value : values) {
                summat += value.getMatriculas();
                sumcount += value.getContagem();
            }
            float media = (float) summat / sumcount;
            con.write(key, new FloatWritable(media));
        }

    }

}
