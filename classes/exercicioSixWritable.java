package TDE2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class exercicioSixWritable implements WritableComparable {
    private String year;
    private String unitType;

    public  exercicioSixWritable(){}

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public void setTodosValores(String year, String unitType){
        this.year=year;
        this.unitType=unitType;

    }


    public exercicioSixWritable(String year, String unitType){
        this.year=year;
        this.unitType=unitType;
    }

    @Override
    public String toString() {
        return "exercicioSixWritable{" +
                "year='" + year + '\'' +
                ", unitType='" + unitType + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(year,unitType);
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }else if(obj==null || getClass()!=obj.getClass()){
            return false;
        }else{
            exercicioSixWritable six=(exercicioSixWritable) obj;
            return year.equals(six.year) && unitType.equals(six.unitType);
        }
    }

    @Override
    public int compareTo(Object o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        year = input.readUTF();
        unitType = input.readUTF();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(year));
        out.writeUTF(String.valueOf(unitType));

    }




}
