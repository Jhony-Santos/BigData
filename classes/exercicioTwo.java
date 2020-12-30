package TDE2;


import java.io.IOException;

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



public class exercicioTwo {
    public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job two = new Job(c, "exercicioTwo");

        // Registro das classes
        two.setJarByClass(exercicioTwo.class);
        two.setMapperClass(MapForTwo.class);
        two.setReducerClass(ReduceForTwo.class);



        two.setMapOutputKeyClass(Text.class);//Mapper
        two.setOutputValueClass(IntWritable.class);
        two.setOutputKeyClass(Text.class);// Reducer
        two.setOutputValueClass(IntWritable.class);


        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(two,new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(two, new Path("output/resolutionTwo.txt"));



        // lanca o job e aguarda sua execucao
        System.exit(two.waitForCompletion(true) ? 0 : 1);


    }

    public static class MapForTwo extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {

            String line=value.toString();
            String column[]=line.split(";");

            if(column[0].equals("country_or_area")|| column[2].equals("TOTAL")) {
                return;
            }

            else{
                enviando.write(new Text(column[1]), new IntWritable(1));

            }

        }
    }

    public static class ReduceForTwo extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text year, Iterable<IntWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            int transactionYear=0;

            for(IntWritable ano:values){
                transactionYear+=ano.get();

            }

            recebendo.write(new Text(year), new IntWritable(transactionYear));




        }
    }

}





















