package advanced.Recuperacao;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IntReverse implements WritableComparable<advanced.Recuperacao.IntReverse> {

    private int data;

    public IntReverse() {
        this.data = 0;
    }

    public IntReverse(int data) {
        this.data = data;
    }

    public int get() {
        return data;
    }

    public void set(int data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        advanced.Recuperacao.IntReverse that = (advanced.Recuperacao.IntReverse) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return data + "\t";
    }

    @Override
    public int compareTo(advanced.Recuperacao.IntReverse o) {
        if (hashCode() > o.hashCode()) {
            return -1;
        } else if (hashCode() < o.hashCode()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        data = dataInput.readInt();
    }
}
