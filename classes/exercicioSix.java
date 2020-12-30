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

public class exercicioSix {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job six = new Job(c, "exercicioFive");

        // Registro das classes
        six.setJarByClass(exercicioSix.class);
        six.setMapperClass(exercicioSix.MapForSix.class);
        six.setReducerClass(exercicioSix.ReduceForSix.class);


        six.setMapOutputKeyClass(exercicioSixWritable.class);
        six.setMapOutputValueClass(exercicioSixValoresWritable.class);
        six.setOutputKeyClass(exercicioSixWritable.class);
        six.setOutputValueClass(Text.class);






        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(six, new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(six, new Path("output/resolutionSix.txt"));

        // lanca o job e aguarda sua execucao
        six.waitForCompletion(true);


    }
    public static class MapForSix extends Mapper<LongWritable, Text, exercicioSixWritable, exercicioSixValoresWritable> {

        // A FUNCAO MAP É UMA ESPÉCIE DE FILTRO A PARTIR NO QUAL DEFINIMOS O(s) OBJETO(s) DE ESTUDO
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {

            String [] colunas=value.toString().split(";");



            if( colunas[0].equals("country_or_area") || colunas[2].equals("TOTAL")){
                return;

            }else {

                String year=colunas[1]; //chave
                String unitType=colunas[7];//chave
                //String commodity=colunas[3];//valor
                // int price=colunas[4];


                 Double price=Double.parseDouble(colunas[5]) ;
                 String commodity=String.valueOf(colunas[3]);

                exercicioSixWritable chave=new exercicioSixWritable(); //instancio
                chave.setTodosValores(year,unitType);

                enviando.write(chave, new exercicioSixValoresWritable(price,commodity));
            }


        }

    }
    public static class ReduceForSix extends Reducer<exercicioSixWritable, exercicioSixValoresWritable, exercicioSixWritable, Text> {

        // Funcao de reduce
        public void reduce(exercicioSixWritable chave, Iterable<exercicioSixValoresWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            double highestPrice=0;
            String nameCommidity="";

            for(exercicioSixValoresWritable obj:values){
                if(obj.getPrice()>highestPrice){
                    highestPrice=obj.getPrice();
                    nameCommidity= obj.getCommodity();

                }

            }

            recebendo.write(chave,new Text("Name of commidity"+nameCommidity+"maior preço"+highestPrice));






        }
    }




}
