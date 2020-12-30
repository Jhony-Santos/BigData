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


public class exercicioOne {
    public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job objeto = new Job(c, "exercicioOne");

        // Registro das classes
        objeto.setJarByClass(exercicioOne.class);
        objeto.setMapperClass(MapForOne.class);
        objeto.setReducerClass(ReduceOne.class);



        objeto.setMapOutputKeyClass(Text.class); // Tipo de chave da funcao Map
        objeto.setMapOutputValueClass(IntWritable.class); // Tipo de valor da funcao Map
        objeto.setOutputKeyClass(Text.class); // Tipo de chave da funcao Reducer
        objeto.setOutputValueClass(IntWritable.class);// Tipo de valor da funcao Reducer


        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(objeto,new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(objeto,new Path("output/resolutionOne.txt"));



        // lanca o job e aguarda sua execucao
        objeto.waitForCompletion(true);


    }
    public static class MapForOne extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {

            String linha=value.toString();
            String[] colunas=linha.split(";");


            if(colunas[2].equals("TOTAL") ||colunas[0].equals("country_or_area")){
                return;
            }


            if(colunas[0].equals("Brazil")){

                enviando.write(new Text(colunas[0]), new IntWritable(1));

              }


            }
        }

    public static class ReduceOne extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text country, Iterable<IntWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            int transactionsBrazil=0;

            for(IntWritable brazil:values){
                    transactionsBrazil+=brazil.get();
            }
            recebendo.write(country, new IntWritable(transactionsBrazil));



        }
    }

}







