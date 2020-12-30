package TDE2;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class exercicioFourWritable implements Writable {
    private int n;
    private float soma;

    public exercicioFourWritable(){}

    public exercicioFourWritable(int n, float soma){
        this.n=n;
        this.soma=soma;

    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getSoma() {
        return soma;
    }

    public void setSoma(float soma) {
        this.soma = soma;
    }




    @Override
    public void readFields(DataInput in) throws IOException {
        n=Integer.parseInt(in.readUTF());
        soma=Float.parseFloat(in.readUTF());

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(soma));
    }





}
