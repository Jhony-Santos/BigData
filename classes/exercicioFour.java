package TDE2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;



public class exercicioFour {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job four = new Job(c, "exercicioFour");

        // Registro das classes
        four.setJarByClass(exercicioFour.class);
        four.setMapperClass(MapForFour.class);
        four.setReducerClass(ReduceForFour.class);

        four.setMapOutputKeyClass(Text.class);
        four.setMapOutputValueClass(exercicioFourWritable.class);
        four.setOutputKeyClass(Text.class);
        four.setOutputValueClass(FloatWritable.class);


        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(four, new Path("in/transactions.csv"));
        FileOutputFormat.setOutputPath(four,new Path("output/resolutionFour.txt"));

        // lanca o job e aguarda sua execucao
        four.waitForCompletion(true) ;


    }

    public static class MapForFour extends Mapper<LongWritable, Text, Text, exercicioFourWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context enviando)
                throws IOException, InterruptedException {


            // preciso obter o ano e o valor da commidity;


            String linha = value.toString();
            String column[] = linha.split(";");


            if (column[0].equals("country_or_area") || column[2].equals("TOTAL")) {
                return;
            } else {
                try{
                    float valueCommodity = Float.parseFloat(column[5]);
                    enviando.write(new Text(column[1]), new exercicioFourWritable(1, valueCommodity));

                }catch(Exception e){
                    System.out.println("Não conseguimos computar os dados");


                }



            }

        }
    }

    public static class ReduceForFour extends Reducer<Text, exercicioFourWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text year, Iterable<exercicioFourWritable> values, Context recebendo)
                throws IOException, InterruptedException {

            int somaN = 0;
            float somaSomas = 0.0f;


            for (exercicioFourWritable obj : values) {

                somaN += obj.getN();
                somaSomas += obj.getSoma();

            }
            float media = somaSomas / somaN;

            recebendo.write(year, new FloatWritable(media));


        }
    }


}
