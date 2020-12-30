package TDE2;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


// necessário dois contrutores, sendo um deles vazio, e metodos getter and setter;
// metodo writableComparable gera: compareTo, write and readFields;

public class exercicioFiveWritable implements WritableComparable {
    private String unitType;
    private String year;
    private String category;



    public exercicioFiveWritable(String year, String category,String unitType){
        this.year=year;
        this.category=category;
        this.unitType=unitType;
    }
    public exercicioFiveWritable(){} // construtor vazio

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    @Override
    public String toString() { // responsavel pelo print no .txt
        return "exercicioFiveWritable{"
                 +
                ", unitType='" + unitType + '\'' +
                ", year='" + year + '\'' +
                ", category='" + category + '\'' +
                '}';
    }

    public void setTodosValores(String year, String category, String unitType){
        this.year=year;
        this.category=category;
        this.unitType=unitType;


    }

    @Override // metodo gerado automaticamente,independente
    public boolean equals(Object obj) {
      if(this==obj){
          return true;
      }else if(obj==null || getClass()!=obj.getClass()){
          return false;
      }else{
          exercicioFiveWritable five=(exercicioFiveWritable) obj;
          return year.equals(five.year) && category.equals(five.unitType)
                 && unitType.equals(five.unitType);

      }
    }

    @Override
    public int hashCode() {
        return Objects.hash(year,category ,unitType);
    }

    @Override // gerado automaticamente ao criarmos o writableComparable
    public int compareTo(Object obj) {
        return this.toString().compareTo(obj.toString());
    }


    @Override // gerado automaticamente ao criarmos o writableComparable
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        category = in.readUTF();
        unitType=in.readUTF();

    }

    @Override // gerado automaticamente ao criarmos o writableComparable
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(year));
        out.writeUTF(String.valueOf(category));
        out.writeUTF(String.valueOf(unitType));

    }




}
