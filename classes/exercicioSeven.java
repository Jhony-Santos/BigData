package TDE2;


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

public class exercicioSeven {

    public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job seven = new Job(c, "exercicioSeven");

        // Registro das classes
        seven.setJarByClass(exercicioSeven.class);
        seven.setMapperClass(exercicioSeven.MapForSeven.class);
        seven.setReducerClass(exercicioSeven.ReduceForSeven.class);



        seven.setMapOutputKeyClass(exercicioSevenWritable.class); // Tipo de chave da funcao Map
        seven.setMapOutputValueClass(IntWritable.class); // Tipo de valor da funcao Map
        seven.setOutputKeyClass(exercicioSevenWritable.class); // Tipo de chave da funcao Reducer
        seven.setOutputValueClass(IntWritable.class);// Tipo de valor da funcao Reducer



        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(seven,new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(seven,new Path("output/resolutionSeven.txt"));



        // lanca o job e aguarda sua execucao
        seven.waitForCompletion(true);


    }
    public static class MapForSeven extends Mapper<LongWritable, Text,exercicioSevenWritable, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {

            String linha=value.toString();
            String[] colunas=linha.split(";");

            if(colunas[2].equals("TOTAL") ||colunas[0].equals("country_or_area")){
                return;
            }else{
                //String year=String.valueOf(colunas[1]);
                //String flowType=String.valueOf(colunas[4]);


                exercicioSevenWritable chave= new exercicioSevenWritable(colunas[1],colunas[4]);
                //chave.setValores(flowType,year);

                enviando.write(chave,new IntWritable(1));

            }


        }
    }

    public static class ReduceForSeven extends Reducer<exercicioSevenWritable, IntWritable, exercicioSevenWritable, IntWritable> {

        // Funcao de reduce
        public void reduce(exercicioSevenWritable chave, Iterable<IntWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            int numberTransaction=0;

            for(IntWritable transaction:values){
                numberTransaction+=transaction.get();

            }
            recebendo.write(chave, new IntWritable(numberTransaction));




        }
    }





}
