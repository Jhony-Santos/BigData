package TDE2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class exercicioSevenWritable implements WritableComparable {
    private String flowType;
    private String year;

public exercicioSevenWritable(){}

public exercicioSevenWritable(String exportFlow, String year){
    this.flowType=exportFlow;
    this.year=year;
}

    public String getExportFlow() {
        return flowType;
    }

    public void setExportFlow(String flowType) {
        this.flowType = flowType;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }
    public void setValores( String flowType,String year){
        this.flowType=flowType;
        this.year=year;

    }

    @Override
    public String toString() {
        return "exercicioSevenWritable{" +
                "exportFlow='" + flowType + '\'' +
                ", year='" + year + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowType,year);
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }else if(obj==null || getClass()!=obj.getClass()){
            return false;
        }else{
            exercicioSevenWritable seven=(exercicioSevenWritable) obj;
            return flowType.equals(seven.flowType) && year.equals(seven.year);

        }

    }

    @Override
    public int compareTo(Object o) {
        return this.toString().compareTo(o.toString());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        flowType= in.readUTF();
        year = in.readUTF();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(flowType));
        out.writeUTF(String.valueOf(year));

    }


}
