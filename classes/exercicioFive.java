package TDE2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class exercicioFive {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job five = new Job(c, "exercicioFive");

        // Registro das classes
        five.setJarByClass(exercicioFive.class);
        five.setMapperClass(MapForFive.class);
        five.setReducerClass(ReduceForFive.class);

        five.setOutputKeyClass(exercicioFiveWritable.class);
        five.setOutputValueClass(DoubleWritable.class);
        five.setOutputKeyClass(exercicioFiveWritable.class);
        five.setOutputValueClass(DoubleWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(five, new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(five,new Path("output/resolutionFive.txt"));

        // lanca o job e aguarda sua execucao
        five.waitForCompletion(true);


    }

    public static class MapForFive extends Mapper<LongWritable, Text, exercicioFiveWritable, DoubleWritable> {

        // A FUNCAO MAP É UMA ESPÉCIE DE FILTRO A PARTIR NO QUAL DEFINIMOS O(s) OBJETO(s) DE ESTUDO
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {
            //nesse caso preciso passar duas chaves (chave composta):

                // country as Brazil (chave) [0]
                // export flow as category(chave)[4]
                // unit type [7]
                // price (valor) [5] valor



            String [] colunas=value.toString().split(";");



            if( colunas[0].equals("country_or_area") || colunas[2].equals("TOTAL")){
                return;

            }else if(colunas[0].equals("Brazil") && colunas[4].equals("Export")){

                String year=colunas[1];
                String unitType=colunas[7];
                String category=colunas[9];

                DoubleWritable price= new DoubleWritable(Double.parseDouble(colunas[5]));// valor
                exercicioFiveWritable chave=new exercicioFiveWritable(); //instancio
                chave.setTodosValores(year,unitType, category);

                enviando.write(chave, price);
            }


        }

    }

    //SHORT AND SHUFFLE AGRUPAM OS VALORES PELAS CHAVES COMUNS



     // FUNCAO RESPONSAVEL POR DEVOLVER OS VALORES DESEJADOS
    public static class ReduceForFive extends Reducer<exercicioFiveWritable, DoubleWritable, exercicioFiveWritable, DoubleWritable> {

        // Funcao de reduce
        public void reduce(exercicioFiveWritable chave, Iterable<DoubleWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            int contador = 0;
            double somaPrices = 0.0f;


            for (DoubleWritable obj : values) {

                somaPrices += obj.get();
                contador+=1;

            }
            double media = somaPrices / contador;

            recebendo.write(chave, new DoubleWritable(media));











        }
    }


}












