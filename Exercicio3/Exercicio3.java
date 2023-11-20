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

public class Exercicio3 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file
        Path finalOutputFile = new Path(files[3]);
        // Job creation (with its name)
        Job j = Job.getInstance(c, "Exercicio3");
        Job j2 = Job.getInstance(c, "Exercicio3-1");
        // Setting Job classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(MapForExercicio3.class);
        j.setReducerClass(ReduceForExercicio3.class);

        j2.setJarByClass(Exercicio3.class);
        j2.setMapperClass(MapForExercicio3Higher.class);
        j2.setReducerClass(ReduceForExercicio3Higher.class);
        // Setting Job output classes
        j.setMapOutputKeyClass(NomeMunicipio.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(NomeMunicipio.class);
        j.setOutputValueClass(IntWritable.class);

        j2.setMapOutputKeyClass(IntWritable.class);
        j2.setMapOutputValueClass(NomeMunicipio.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(NomeMunicipio.class);
        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);
        FileInputFormat.addInputPath(j2, outputFile);
        FileOutputFormat.setOutputPath(j2, finalOutputFile);
        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) && j2.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForExercicio3 extends Mapper<LongWritable, Text, NomeMunicipio, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String[] columns = value.toString().split(";", -1);
            String nome = columns[14];
            String municipio = columns[6];
            Text firstline = new Text(columns[0]);
            Text FirstHeader = new Text("NU_ANO_CENSO");


            if (!(firstline.equals(FirstHeader)) && !Objects.equals(columns[338], "") && !Objects.equals(columns[342], "") && !Objects.equals(columns[345], "")) {
                try {
                    // Saindo do Map com a Regiao e a contagem de escola (registro)
                    int basica = Integer.parseInt(columns[338]);
                    int fundamental = Integer.parseInt(columns[342]);
                    int medio = Integer.parseInt(columns[345]);
                    con.write(new NomeMunicipio(nome, municipio), new IntWritable(basica + fundamental + medio));
                }
                catch (NumberFormatException e){
                    System.out.println("Non-numeric amount found.");
                }
            }
        }

    }

    public static class ReduceForExercicio3 extends Reducer<NomeMunicipio, IntWritable, NomeMunicipio, IntWritable> {

        public void reduce(NomeMunicipio key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int totalworkers = 0;

            for(IntWritable value: values){
                totalworkers += value.get();
            }
            con.write(key, new IntWritable(totalworkers));
        }

    }

    public static class MapForExercicio3Higher extends Mapper<LongWritable, Text, IntWritable, NomeMunicipio>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            String nome = columns[0];
            String municipio = columns[1];
            int numberofworkers = Integer.parseInt(columns[2]);
            con.write( new IntWritable(numberofworkers), new NomeMunicipio(nome, municipio));
        }


    }

    public static class ReduceForExercicio3Higher extends Reducer<IntWritable, NomeMunicipio, IntWritable, NomeMunicipio>{
        private IntWritable lastKey = new IntWritable();
        private NomeMunicipio lastValue = new NomeMunicipio();
        public void reduce(IntWritable key, Iterable<NomeMunicipio> values, Context con) throws IOException, InterruptedException{
            NomeMunicipio out = new NomeMunicipio();
            for(NomeMunicipio value: values){
              lastKey = key;
              lastValue = value;
            }
        }
        @Override
        protected void cleanup(Context con) throws IOException, InterruptedException {
            con.write(lastKey, lastValue);
        }
    }
}
