package TDE2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class exercicioSixValoresWritable implements Writable {
    private Double price;
    private String commodity;


    public exercicioSixValoresWritable(){}//construtor vazio

    public exercicioSixValoresWritable(double price, String commodity){
        this.price=price;
        this.commodity=commodity;
    }


    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        price=Double.parseDouble(in.readUTF());
        commodity=String.valueOf(in.readUTF());

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(price));
        out.writeUTF(String.valueOf(commodity));



    }




}
