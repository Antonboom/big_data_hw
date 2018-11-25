package task_1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {

    SumCountWritable() {
        this.sum = 0d;
        this.count = 0;
    }

    SumCountWritable(double sum, int count){
        this.sum = sum;
        this.count = count;
    }

    private double sum;
    private int count;

    public void set(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return this.sum;
    }

    public int getCount() {
        return this.count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.sum);
        out.writeInt(this.count);
    }

    public void readFields(DataInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readInt();
    }

    @Override
    public String toString() {
        return this.sum + ":" + this.count;
    }
}
