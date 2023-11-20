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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
import java.util.Objects;

public class Exercicio5 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio3");
        // Setting Job classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(MapForExercicio3.class);
        j.setReducerClass(ReduceForExercicio3.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(LocalizacaoDependencia.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(LocalizacaoDependencia.class);
        j.setOutputValueClass(IntWritable.class);


        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);
        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForExercicio3 extends Mapper<LongWritable, Text, LocalizacaoDependencia, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String[] columns = value.toString().split(";", -1);
            String localizacao = columns[17];
            String dependencia = columns[15];
            Text firstline = new Text(columns[0]);
            Text FirstHeader = new Text("NU_ANO_CENSO");


            if (!(firstline.equals(FirstHeader))) {
                // Saindo do Map com a Regiao e a contagem de escola (registro)
                con.write(new LocalizacaoDependencia(localizacao, dependencia), new IntWritable(1));
            }
        }

    }

    public static class ReduceForExercicio3 extends Reducer<LocalizacaoDependencia, IntWritable, LocalizacaoDependencia, IntWritable> {

        public void reduce(LocalizacaoDependencia key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int totalschools = 0;
            for(IntWritable value: values){
                totalschools += value.get();
            }
            con.write(key, new IntWritable(totalschools));
        }

    }
}
